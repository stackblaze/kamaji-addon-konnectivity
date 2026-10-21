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
	"net/http"
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
	"sigs.k8s.io/controller-runtime/pkg/builder"
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

	// UnhealthyThreshold is how many CONSECUTIVE unhealthy verdicts a tenant
	// must accumulate before we bounce its agents. The konnectivity-agent
	// connection set churns (agents re-dial through a single VIP fronting N
	// servers), so an isolated probe can catch a sub-second blip that clears on
	// its own by the next recheck. Bouncing on that single blip restarts the
	// whole DaemonSet at once — a real coverage gap far worse than the blip.
	// Requiring the unhealth to PERSIST across consecutive rechecks filters
	// transient churn while still healing a genuine wedge (which stays
	// unhealthy) within UnhealthyThreshold*RecheckInterval. <=1 keeps the old
	// bounce-on-first-failure behaviour.
	UnhealthyThreshold int

	// Per-server coverage check. ServerMetricsPort is the konnectivity-server
	// admin port that serves /metrics; CoverageTimeout bounds a single scrape.
	// HTTPClient is overridable in tests; nil uses http.DefaultClient.
	ServerMetricsPort int
	CoverageTimeout   time.Duration
	HTTPClient        *http.Client

	// Per-agent liveness (see agents.go). ManageAgents keeps a /readyz
	// readinessProbe on the agent DaemonSet and deletes agent pods that stay
	// NotReady (no konnectivity-server connection) for AgentRestartAfter.
	// AgentCheckInterval is how often a HEALTHY tenant's agents are looked at
	// between full tunnel probes (which still run every SteadyInterval).
	ManageAgents bool
	// AgentTenants, when non-empty, limits agent management to these
	// "namespace/name" TCP keys (or bare TCP names) — the canary switch for a
	// dev-zone-first rollout on a management cluster that also hosts prod
	// zones. Empty = every tenant.
	AgentTenants       map[string]bool
	AgentRestartAfter  time.Duration
	AgentCheckInterval time.Duration

	// Now is the clock; nil uses time.Now. Overridable in tests.
	Now func() time.Time

	mu         sync.Mutex
	lastBounce map[string]time.Time
	// lastProbe remembers when (and at which TCP generation) a tenant's
	// tunnel was last fully probed and found healthy, so the cheaper agent
	// check can run more often than the full probe.
	lastProbe map[string]probeMemo
	// unhealthyStreak counts consecutive unhealthy verdicts per tenant; reset
	// to 0 on any healthy verdict and after a bounce.
	unhealthyStreak map[string]int
}

type probeMemo struct {
	at         time.Time
	generation int64
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
			deleteTenantMetrics(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if tcp.GetDeletionTimestamp() != nil {
		r.forget(req.NamespacedName.String())
		return ctrl.Result{}, nil
	}

	// Only act on konnectivity-enabled tenants that haven't opted out. Drop
	// any tunnel state we published earlier so the alert path doesn't keep
	// reporting a verdict nobody maintains any more.
	if !konnectivityEnabled(tcp) || tcp.GetAnnotations()[AnnotationVerify] == "false" {
		if !konnectivityEnabled(tcp) {
			deleteTenantMetrics(req.Namespace, req.Name)
		}
		r.forget(req.NamespacedName.String())
		if err := r.applyTunnelAnnotations(ctx, tcp, clearTunnelStatePatch(tcp.GetAnnotations())); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	key := req.NamespacedName.String()
	now := r.now()

	tenant, err := r.tenantClient(ctx, req.Namespace, req.Name)
	if err != nil {
		// CP not far enough along to have an admin kubeconfig yet — wait, don't bounce.
		logger.V(1).Info("tenant client not ready yet", "reason", err.Error())
		return ctrl.Result{RequeueAfter: r.RecheckInterval}, nil
	}

	// Per-agent liveness: cheap (one DaemonSet GET + two LISTs), so it runs on
	// every pass, including the ones that skip the full tunnel probe.
	agents, restarted := r.maintainAgents(ctx, tcp, tenant, now)

	if !r.probeDue(key, tcp.GetGeneration(), now, agents, restarted) {
		return ctrl.Result{RequeueAfter: r.healthyRequeue()}, nil
	}

	res := r.probe(ctx, tenant)

	// Deterministic per-server coverage: scrape each konnectivity-server's
	// connected-agent count directly, so we catch the HA failure mode where the
	// dial probe passes (its requests happened to land on the covered apiserver
	// replica) while another replica's server has zero agents. Best-effort —
	// known=false falls back to the dial probe.
	cov := r.checkCoverage(ctx, req.Namespace, req.Name)
	if cov.known {
		serversTotal.WithLabelValues(req.Namespace, req.Name).Set(float64(cov.total))
		serversCovered.WithLabelValues(req.Namespace, req.Name).Set(float64(cov.covered))
	}

	if !res.reachable {
		// Couldn't reach the apiserver at all (still provisioning, or transient).
		// Not a tunnel wedge — never bounce on this; just re-check soon. The
		// published tunnel state is left as it was: an unreachable apiserver
		// is alerted on by the health bridge in its own right.
		r.dropProbeMemo(key)
		tunnelHealthy.WithLabelValues(req.Namespace, req.Name).Set(0)
		probeErrorsTotal.WithLabelValues(req.Namespace, req.Name).Inc()
		logger.V(1).Info("tenant apiserver unreachable; will re-check")
		return ctrl.Result{RequeueAfter: r.RecheckInterval}, nil
	}

	healthy, reason := tunnelVerdict(res, cov, agents)
	if err := r.publishTunnelState(ctx, tcp, healthy, reason, now); err != nil {
		// Alerting is best-effort relative to healing: log and carry on.
		logger.Error(err, "could not publish tunnel state on the TenantControlPlane")
	}

	if healthy {
		tunnelHealthy.WithLabelValues(req.Namespace, req.Name).Set(1)
		r.clearUnhealthy(key)
		r.rememberProbe(key, tcp.GetGeneration(), now)
		logger.V(1).Info("konnectivity tunnel healthy", "serversCovered", cov.covered, "serversTotal", cov.total,
			"agentsReady", agents.ready, "agentsDesired", agents.desired)
		return ctrl.Result{RequeueAfter: r.healthyRequeue()}, nil
	}

	// Reachable but not fully healthy: dials are wedged, a control-plane
	// replica's konnectivity-server has no agent coverage, or no agent is
	// connected at all.
	r.dropProbeMemo(key)
	tunnelHealthy.WithLabelValues(req.Namespace, req.Name).Set(0)

	if len(restarted) > 0 {
		// The per-agent restart just acted on this very failure; give the
		// replacement agents the bounce cooldown to connect before also
		// rolling the whole DaemonSet.
		r.markBounced(key)
		return ctrl.Result{RequeueAfter: r.RecheckInterval}, nil
	}

	// Debounce: the agent connection set churns (agents re-dial through a
	// single VIP fronting N servers), so an isolated probe can catch a
	// sub-second blip that clears by the next recheck. Bouncing on that lone
	// blip restarts the whole DaemonSet at once — a real, worse coverage
	// gap. Require the unhealth to PERSIST across consecutive rechecks; a
	// genuine wedge stays unhealthy and still heals within
	// UnhealthyThreshold*RecheckInterval.
	streak := r.recordUnhealthy(key)
	if threshold := r.unhealthyThreshold(); streak < threshold {
		logger.V(1).Info("tunnel unhealthy; awaiting confirmation before bouncing",
			"reason", reason, "streak", streak, "threshold", threshold)
		return ctrl.Result{RequeueAfter: r.RecheckInterval}, nil
	}
	return r.remediate(ctx, tcp, tenant, key, reason)
}

// tunnelVerdict folds the dial probe, per-server coverage and the agents'
// own connection state into one healthy/unhealthy verdict with a reason.
func tunnelVerdict(res probeResult, cov coverage, agents agentState) (bool, string) {
	switch {
	case agents.tunnelDown():
		return false, fmt.Sprintf("had no konnectivity-agent connected to any server (0/%d agents ready)", agents.desired)
	case !res.healthy:
		return false, "was up-but-wedged (apiserver→in-cluster dials timed out)"
	case cov.incomplete():
		return false, fmt.Sprintf("had a control-plane replica with no agent coverage (%d/%d konnectivity-servers covered)", cov.covered, cov.total)
	}
	return true, ""
}

// publishTunnelState records the verdict on the TCP (only on a transition)
// and in the unhealthy-duration gauge.
func (r *TenantControlPlaneReconciler) publishTunnelState(ctx context.Context, tcp *unstructured.Unstructured, healthy bool, reason string, now time.Time) error {
	ann := tcp.GetAnnotations()
	patch := tunnelStatePatch(ann, healthy, reason, now)
	err := r.applyTunnelAnnotations(ctx, tcp, patch)
	if err == nil && patch != nil {
		ann = tcp.GetAnnotations()
		if !healthy {
			r.event(tcp, corev1.EventTypeWarning, "TunnelUnhealthy", "konnectivity tunnel "+reason)
		} else {
			r.event(tcp, corev1.EventTypeNormal, "TunnelHealthy", "konnectivity tunnel healthy again")
		}
	}
	tunnelUnhealthySeconds.WithLabelValues(tcp.GetNamespace(), tcp.GetName()).Set(unhealthySeconds(ann, now))
	return err
}

// maintainAgents keeps the /readyz readinessProbe on the agent DaemonSet,
// exports the per-agent gauges, and restarts agents that stayed disconnected.
func (r *TenantControlPlaneReconciler) maintainAgents(ctx context.Context, tcp *unstructured.Unstructured, tenant kubernetes.Interface, now time.Time) (agentState, []string) {
	if !r.managesAgentsOf(tcp.GetNamespace(), tcp.GetName()) {
		return agentState{}, nil
	}
	logger := log.FromContext(ctx)
	ns, name := tcp.GetNamespace(), tcp.GetName()

	if patched, err := r.ensureAgentReadinessProbe(ctx, tenant); err != nil {
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "could not ensure the konnectivity-agent readinessProbe")
		}
	} else if patched {
		r.event(tcp, corev1.EventTypeNormal, "AgentReadinessProbe",
			"added a /readyz readinessProbe to the konnectivity-agent DaemonSet (agents roll once, one node at a time)")
		logger.Info("added /readyz readinessProbe to the konnectivity-agent DaemonSet")
	}

	st := r.inspectAgents(ctx, tenant, now)
	if !st.known {
		return st, nil
	}
	agentsDesired.WithLabelValues(ns, name).Set(float64(st.desired))
	agentsReady.WithLabelValues(ns, name).Set(float64(st.ready))
	nodesWithoutAgent.WithLabelValues(ns, name).Set(float64(len(st.nodesWithoutAgent)))
	agentsStuckTerminating.WithLabelValues(ns, name).Set(float64(len(st.stuckTerminating)))
	if len(st.stuckTerminating) > 0 {
		r.event(tcp, corev1.EventTypeWarning, "AgentStuckTerminating", fmt.Sprintf(
			"konnectivity-agent pod(s) %v are terminating far past their grace period — the node cannot kill them (read-only root filesystem / containerd wedged?); the node needs attention",
			st.stuckTerminating))
	}

	restarted, err := r.restartStuckAgents(ctx, tenant, st)
	if err != nil {
		logger.Error(err, "could not restart a disconnected konnectivity-agent")
	}
	if len(restarted) > 0 {
		agentRestartsTotal.WithLabelValues(ns, name).Add(float64(len(restarted)))
		r.event(tcp, corev1.EventTypeWarning, "RestartedAgent", fmt.Sprintf(
			"konnectivity-agent pod(s) %v had no konnectivity-server connection (NotReady on /readyz) for over %s; deleted so the DaemonSet replaces them",
			restarted, r.agentRestartAfter()))
		logger.Info("restarted disconnected konnectivity-agent pods", "pods", restarted)
	}
	return st, restarted
}

// managesAgentsOf reports whether per-agent liveness applies to this TCP.
func (r *TenantControlPlaneReconciler) managesAgentsOf(ns, name string) bool {
	if !r.ManageAgents {
		return false
	}
	if len(r.AgentTenants) == 0 {
		return true
	}
	return r.AgentTenants[ns+"/"+name] || r.AgentTenants[name]
}

// probeDue decides whether this pass runs the full tunnel probe. It does
// unless the tenant was found healthy less than SteadyInterval ago, at the
// same TCP generation, and its agents look fine right now.
func (r *TenantControlPlaneReconciler) probeDue(key string, generation int64, now time.Time, agents agentState, restarted []string) bool {
	if len(restarted) > 0 || len(agents.notReadyTooLong) > 0 || agents.tunnelDown() ||
		(agents.known && agents.ready < agents.desired) {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.lastProbe[key]
	if !ok || m.generation != generation {
		return true
	}
	return now.Sub(m.at) >= r.SteadyInterval
}

func (r *TenantControlPlaneReconciler) rememberProbe(key string, generation int64, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastProbe == nil {
		r.lastProbe = map[string]probeMemo{}
	}
	r.lastProbe[key] = probeMemo{at: now, generation: generation}
}

func (r *TenantControlPlaneReconciler) dropProbeMemo(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.lastProbe, key)
}

// healthyRequeue is the requeue after a healthy pass: the agent-check cadence
// when per-agent liveness is on (the full probe still only runs every
// SteadyInterval — see probeDue), else SteadyInterval.
func (r *TenantControlPlaneReconciler) healthyRequeue() time.Duration {
	if r.ManageAgents && r.AgentCheckInterval > 0 && r.AgentCheckInterval < r.SteadyInterval {
		return r.AgentCheckInterval
	}
	return r.SteadyInterval
}

func (r *TenantControlPlaneReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// remediate bounces the tenant's konnectivity-agent DaemonSet, rate-limited per
// tenant so a genuinely-broken cluster doesn't get restarted in a tight loop.
func (r *TenantControlPlaneReconciler) remediate(ctx context.Context, tcp *unstructured.Unstructured, tenant kubernetes.Interface, key, reason string) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if r.recentlyBounced(key) {
		// Gave it a bounce recently and it's still unhealthy — let that restart
		// settle before trying again instead of hammering.
		r.event(tcp, corev1.EventTypeWarning, "TunnelWedged",
			"konnectivity tunnel still unhealthy after a recent agent restart; waiting before retrying")
		logger.Info("tunnel still unhealthy within cooldown; backing off", "reason", reason)
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
		fmt.Sprintf("konnectivity tunnel %s; restarted the konnectivity-agent DaemonSet to recover it", reason))
	logger.Info("bounced konnectivity-agent DaemonSet to recover unhealthy tunnel", "reason", reason)
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
	// After a bounce, re-confirm sustained unhealth before the next one so a
	// single post-restart blip doesn't immediately re-bounce.
	delete(r.unhealthyStreak, key)
}

// unhealthyThreshold is the number of consecutive unhealthy verdicts required
// before bouncing. <=1 preserves the original bounce-on-first-failure behaviour.
func (r *TenantControlPlaneReconciler) unhealthyThreshold() int {
	if r.UnhealthyThreshold < 1 {
		return 1
	}
	return r.UnhealthyThreshold
}

// recordUnhealthy increments and returns the tenant's consecutive-unhealthy count.
func (r *TenantControlPlaneReconciler) recordUnhealthy(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.unhealthyStreak == nil {
		r.unhealthyStreak = map[string]int{}
	}
	r.unhealthyStreak[key]++
	return r.unhealthyStreak[key]
}

// clearUnhealthy resets the tenant's consecutive-unhealthy count (called on a
// healthy verdict).
func (r *TenantControlPlaneReconciler) clearUnhealthy(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.unhealthyStreak, key)
}

func (r *TenantControlPlaneReconciler) forget(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.lastBounce, key)
	delete(r.unhealthyStreak, key)
	delete(r.lastProbe, key)
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
		For(tcpObj, builder.WithPredicates(tcpEventFilter())).
		Complete(r)
}
