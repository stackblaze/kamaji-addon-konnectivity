// Package controller implements the management-cluster reconciler that watches
// Kamaji TenantControlPlane resources and keeps each tenant's konnectivity
// tunnel healthy.
//
// Konnectivity is the apiserver→in-cluster egress tunnel: the hosted apiserver
// runs in the management cluster and has no route into the tenant's pod/service
// network, so admission webhooks (CNPG/Postgres, MariaDB) and apiserver→kubelet
// (logs/exec/metrics) all ride a reverse tunnel opened by in-tenant agents.
// Kamaji enables konnectivity but neither guarantees nor checks that the tunnel
// actually SERVES dials — and a known failure mode leaves it up-but-wedged: the
// agent registers on the konnectivity-server yet every DIAL_REQ times out
// (~30s), typically when an agent attaches to the terminating apiserver pod
// during a control-plane roll.
//
// This controller closes that gap: for every konnectivity-enabled TCP it
// periodically verifies the tunnel serves dials (probing each node's kubelet
// through the apiserver proxy) and, when wedged, self-heals by bouncing the
// tenant's konnectivity-agent DaemonSet. Opt out per tenant with the annotation
// konnectivity.stackblaze.com/verify=false.
package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// AnnotationVerify, set to "false" on a TenantControlPlane, opts that tenant
	// out of tunnel verification + self-heal. Any other value (or absent) keeps
	// the default-on behaviour for konnectivity-enabled tenants.
	AnnotationVerify = "konnectivity.stackblaze.com/verify"

	// restartedAtAnnotation is bumped on the agent DaemonSet pod template to
	// trigger a rolling restart (the same mechanism as `kubectl rollout restart`).
	restartedAtAnnotation = "konnectivity.stackblaze.com/restartedAt"

	agentNamespace = "kube-system"
	agentDaemonSet = "konnectivity-agent"
)

// tcpGVK is the Kamaji TenantControlPlane GroupVersionKind we watch.
var tcpGVK = schema.GroupVersionKind{
	Group:   "kamaji.clastix.io",
	Version: "v1alpha1",
	Kind:    "TenantControlPlane",
}

// clientBuilder turns a kubeconfig into a typed clientset. Overridable in tests.
var clientBuilder = func(kubeconfig []byte) (kubernetes.Interface, error) {
	cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

// TenantControlPlaneReconciler verifies + self-heals the konnectivity tunnel of
// every konnectivity-enabled TenantControlPlane.
type TenantControlPlaneReconciler struct {
	client.Client
	// APIReader is an uncached reader used to GET admin-kubeconfig Secrets
	// without caching every Secret in the management cluster.
	APIReader client.Reader
	Scheme    *runtime.Scheme
	Recorder  record.EventRecorder

	SteadyInterval  time.Duration
	RecheckInterval time.Duration
	BounceCooldown  time.Duration
	ProbeTimeout    time.Duration
	ProbeRounds     int

	mu         sync.Mutex
	lastBounce map[string]time.Time
}

// probeResult distinguishes "tunnel served dials" from "could not reach the
// apiserver at all" (CP not ready / kubeconfig missing) — only the former, when
// it fails, warrants bouncing the agents.
type probeResult struct {
	healthy   bool // tunnel served dials on every node
	reachable bool // we reached the tenant apiserver (listed nodes) at all
}

func (r *TenantControlPlaneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("tcp", req.NamespacedName)

	tcp := &unstructured.Unstructured{}
	tcp.SetGroupVersionKind(tcpGVK)
	if err := r.Get(ctx, req.NamespacedName, tcp); err != nil {
		if apierrors.IsNotFound(err) {
			r.forget(req.NamespacedName.String())
			tunnelHealthy.DeleteLabelValues(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if tcp.GetDeletionTimestamp() != nil {
		r.forget(req.NamespacedName.String())
		return ctrl.Result{}, nil
	}

	// Only act on konnectivity-enabled tenants that haven't opted out.
	if !konnectivityEnabled(tcp) {
		tunnelHealthy.DeleteLabelValues(req.Namespace, req.Name)
		return ctrl.Result{}, nil
	}
	if tcp.GetAnnotations()[AnnotationVerify] == "false" {
		return ctrl.Result{}, nil
	}

	tenant, err := r.tenantClient(ctx, req.Namespace, req.Name)
	if err != nil {
		// CP not far enough along to have an admin kubeconfig yet — wait, don't bounce.
		logger.V(1).Info("tenant client not ready yet", "reason", err.Error())
		return ctrl.Result{RequeueAfter: r.RecheckInterval}, nil
	}

	res := r.probe(ctx, tenant)
	switch {
	case res.healthy:
		tunnelHealthy.WithLabelValues(req.Namespace, req.Name).Set(1)
		logger.V(1).Info("konnectivity tunnel healthy")
		return ctrl.Result{RequeueAfter: r.SteadyInterval}, nil

	case !res.reachable:
		// Couldn't reach the apiserver at all (still provisioning, or transient).
		// Not a tunnel wedge — never bounce on this; just re-check soon.
		tunnelHealthy.WithLabelValues(req.Namespace, req.Name).Set(0)
		probeErrorsTotal.WithLabelValues(req.Namespace, req.Name).Inc()
		logger.V(1).Info("tenant apiserver unreachable; will re-check")
		return ctrl.Result{RequeueAfter: r.RecheckInterval}, nil

	default:
		// Reachable but wedged: agents are connected yet dials don't complete.
		tunnelHealthy.WithLabelValues(req.Namespace, req.Name).Set(0)
		return r.remediate(ctx, tcp, tenant, req.NamespacedName.String())
	}
}

// remediate bounces the tenant's konnectivity-agent DaemonSet, rate-limited per
// tenant so a genuinely-broken cluster doesn't get restarted in a tight loop.
func (r *TenantControlPlaneReconciler) remediate(ctx context.Context, tcp *unstructured.Unstructured, tenant kubernetes.Interface, key string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if r.recentlyBounced(key) {
		// Gave it a bounce recently and it's still wedged — let that restart
		// settle before trying again instead of hammering.
		r.event(tcp, corev1.EventTypeWarning, "TunnelWedged",
			"konnectivity tunnel still not serving dials after a recent agent restart; waiting before retrying")
		logger.Info("tunnel still wedged within cooldown; backing off")
		return ctrl.Result{RequeueAfter: r.RecheckInterval}, nil
	}

	if err := r.bounceAgents(ctx, tenant); err != nil {
		if apierrors.IsNotFound(err) {
			// No agent DaemonSet — konnectivity isn't actually deployed in the
			// tenant yet. Re-check; don't treat as fatal.
			logger.V(1).Info("konnectivity-agent DaemonSet not found; will re-check")
			return ctrl.Result{RequeueAfter: r.RecheckInterval}, nil
		}
		return ctrl.Result{}, fmt.Errorf("bounce konnectivity-agent: %w", err)
	}

	r.markBounced(key)
	remediationsTotal.WithLabelValues(tcp.GetNamespace(), tcp.GetName()).Inc()
	r.event(tcp, corev1.EventTypeWarning, "BouncedAgents",
		"konnectivity tunnel was up-but-wedged (apiserver→in-cluster dials timed out); restarted the konnectivity-agent DaemonSet to recover it")
	logger.Info("bounced konnectivity-agent DaemonSet to recover wedged tunnel")
	return ctrl.Result{RequeueAfter: r.RecheckInterval}, nil
}

// probe verifies the tunnel serves apiserver→in-cluster dials by hitting every
// node's kubelet through the apiserver proxy, several rounds (so requests land
// on both round-robined apiserver replicas), with a short per-call timeout so a
// wedged dial fails fast instead of hanging ~30s.
func (r *TenantControlPlaneReconciler) probe(ctx context.Context, tenant kubernetes.Interface) probeResult {
	nodes, err := tenant.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil || len(nodes.Items) == 0 {
		return probeResult{healthy: false, reachable: false}
	}
	rounds := r.ProbeRounds
	if rounds < 1 {
		rounds = 1
	}
	for round := 0; round < rounds; round++ {
		for i := range nodes.Items {
			node := nodes.Items[i].Name
			cctx, cancel := context.WithTimeout(ctx, r.ProbeTimeout)
			err := tenant.CoreV1().RESTClient().Get().
				AbsPath(fmt.Sprintf("/api/v1/nodes/%s/proxy/healthz", node)).
				Do(cctx).Error()
			cancel()
			if err != nil {
				return probeResult{healthy: false, reachable: true}
			}
		}
	}
	return probeResult{healthy: true, reachable: true}
}

// bounceAgents triggers a rolling restart of the tenant's konnectivity-agent
// DaemonSet by stamping its pod template (same as `kubectl rollout restart`).
func (r *TenantControlPlaneReconciler) bounceAgents(ctx context.Context, tenant kubernetes.Interface) error {
	patch := []byte(fmt.Sprintf(
		`{"spec":{"template":{"metadata":{"annotations":{%q:%q}}}}}`,
		restartedAtAnnotation, metav1.Now().UTC().Format(time.RFC3339),
	))
	_, err := tenant.AppsV1().DaemonSets(agentNamespace).Patch(
		ctx, agentDaemonSet, types.StrategicMergePatchType, patch, metav1.PatchOptions{},
	)
	return err
}

// tenantClient builds a typed client for the tenant from its admin kubeconfig
// secret. Prefers the in-cluster Service endpoint (admin.svc) so the controller
// reaches the tenant apiserver directly from the management cluster without a
// public NAT hairpin; falls back to admin.conf.
func (r *TenantControlPlaneReconciler) tenantClient(ctx context.Context, ns, name string) (kubernetes.Interface, error) {
	secret := &corev1.Secret{}
	key := types.NamespacedName{Namespace: ns, Name: name + "-admin-kubeconfig"}
	if err := r.APIReader.Get(ctx, key, secret); err != nil {
		return nil, fmt.Errorf("get admin kubeconfig secret: %w", err)
	}
	for _, k := range []string{"admin.svc", "admin.conf", "super-admin.svc", "super-admin.conf"} {
		if kc, ok := secret.Data[k]; ok && len(kc) > 0 {
			return clientBuilder(kc)
		}
	}
	return nil, fmt.Errorf("admin kubeconfig secret %s has no usable kubeconfig key", key)
}

func (r *TenantControlPlaneReconciler) event(obj *unstructured.Unstructured, etype, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(obj, etype, reason, msg)
	}
}

func (r *TenantControlPlaneReconciler) recentlyBounced(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	last, ok := r.lastBounce[key]
	return ok && time.Since(last) < r.BounceCooldown
}

func (r *TenantControlPlaneReconciler) markBounced(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastBounce == nil {
		r.lastBounce = map[string]time.Time{}
	}
	r.lastBounce[key] = time.Now()
}

func (r *TenantControlPlaneReconciler) forget(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.lastBounce, key)
}

// konnectivityEnabled reports whether spec.addons.konnectivity is set on the TCP.
// A present sub-object (even `konnectivity: {}`) means Kamaji enables it.
func konnectivityEnabled(tcp *unstructured.Unstructured) bool {
	addons, found, err := unstructured.NestedMap(tcp.Object, "spec", "addons")
	if err != nil || !found {
		return false
	}
	konn, ok := addons["konnectivity"]
	return ok && konn != nil
}

// SetupWithManager wires the controller in: watch TenantControlPlanes
// (unstructured), reconcile on change + on the per-result requeue interval.
func (r *TenantControlPlaneReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.lastBounce == nil {
		r.lastBounce = map[string]time.Time{}
	}
	tcpObj := &unstructured.Unstructured{}
	tcpObj.SetGroupVersionKind(tcpGVK)
	return ctrl.NewControllerManagedBy(mgr).
		Named("tenantcontrolplane-konnectivity").
		For(tcpObj).
		Complete(r)
}
