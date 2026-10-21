package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

// Per-agent liveness.
//
// Kamaji renders the tenant's konnectivity-agent DaemonSet and re-asserts, on
// every reconcile, a livenessProbe on GET /healthz — which in
// apiserver-network-proxy's agent (cmd/agent/app/server.go, v0.30–v0.35) is a
// handler that ALWAYS answers "ok" while the process runs. It says nothing
// about the tunnel. An agent that lost every server connection — or one that
// got SIGTERM, entered "draining" (pkg/agent/clientset.go: "Skipping
// connectOnce - agent is draining") and was never killed — stays Running and
// "live" forever while the apiserver answers "No agent available".
//
// The agent's GET /readyz is the endpoint that reflects the tunnel: it runs the
// server-connected check (pkg/agent/readiness.go), which fails with "no servers
// connected" unless at least one gRPC connection to a konnectivity-server is
// READY (ClientSet.HealthyClientsCount() > 0).
//
// We cannot point Kamaji's livenessProbe at /readyz: Kamaji overwrites it on
// every reconcile, so patching it would flip the pod template back and forth
// and roll the agents on every Kamaji resync. Kamaji does NOT manage the
// readinessProbe, so the controller:
//
//  1. ensures a readinessProbe on GET /readyz (one-time DaemonSet patch,
//     idempotent; also makes DaemonSet rollouts wait for each new agent to
//     actually connect before moving to the next node), and
//  2. acts as the liveness half: any agent pod that has been NotReady for
//     longer than AgentRestartAfter (and whose container has been up at least
//     that long) is deleted so the DaemonSet replaces it.

const (
	agentContainerName = "konnectivity-agent"
	agentPodLabelKey   = "k8s-app"
	agentPodLabelValue = "konnectivity-agent"

	// Kamaji passes --health-server-port=8134; this is the upstream default
	// used only when the flag is absent.
	upstreamAgentHealthPort = 8093

	agentReadyzPath = "/readyz"
)

// desiredAgentReadinessProbe is the readinessProbe the controller keeps on the
// agent container. Every field is spelled out (including the ones the apiserver
// would default) so a semantic comparison with the live object is stable and
// we never re-patch an already-correct DaemonSet.
//
// period 10s x failureThreshold 3 => an agent with no server connection is
// marked NotReady after ~30s. Readiness never restarts anything by itself; the
// restart is restartStuckAgents' job, after a much longer window.
func desiredAgentReadinessProbe(c *corev1.Container) *corev1.Probe {
	host, port := agentHealthEndpoint(c)
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   agentReadyzPath,
				Host:   host,
				Port:   intstr.FromInt32(int32(port)),
				Scheme: corev1.URISchemeHTTP,
			},
		},
		InitialDelaySeconds: 5,
		TimeoutSeconds:      5,
		PeriodSeconds:       10,
		SuccessThreshold:    1,
		FailureThreshold:    3,
	}
}

// agentHealthEndpoint derives where the kubelet must probe the agent's health
// server from the container args: --health-server-port (Kamaji: 8134), and
// --health-server-host when it binds a single address (newer Kamaji binds
// 127.0.0.1 for hostNetwork agents — then the kubelet, which runs in the host
// netns, must probe that host instead of the pod IP).
func agentHealthEndpoint(c *corev1.Container) (string, int) {
	args := map[string]string{}
	all := append(append([]string{}, c.Command...), c.Args...)
	for i := 0; i < len(all); i++ {
		a := all[i]
		if !strings.HasPrefix(a, "--") {
			continue
		}
		if k, v, ok := strings.Cut(a, "="); ok {
			args[k] = v
		} else if i+1 < len(all) && !strings.HasPrefix(all[i+1], "-") {
			args[a] = all[i+1]
		}
	}
	port := upstreamAgentHealthPort
	if p, err := strconv.Atoi(args["--health-server-port"]); err == nil && p > 0 {
		port = p
	}
	host := ""
	if h := args["--health-server-host"]; h != "" {
		if ip := net.ParseIP(h); ip == nil || !ip.IsUnspecified() {
			host = h
		}
	}
	return host, port
}

func agentContainerIndex(spec *corev1.PodSpec) int {
	for i := range spec.Containers {
		if spec.Containers[i].Name == agentContainerName {
			return i
		}
	}
	if len(spec.Containers) == 1 {
		return 0
	}
	return -1
}

// ensureAgentReadinessProbe adds/repairs the /readyz readinessProbe on the
// tenant's konnectivity-agent DaemonSet. Returns true when it patched (which
// rolls the agents once, one node at a time). NotFound is returned as-is so the
// caller can treat "no DaemonSet yet" as benign.
func (r *TenantControlPlaneReconciler) ensureAgentReadinessProbe(ctx context.Context, tenant kubernetes.Interface) (bool, error) {
	ds, err := tenant.AppsV1().DaemonSets(agentNamespace).Get(ctx, agentDaemonSet, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	idx := agentContainerIndex(&ds.Spec.Template.Spec)
	if idx < 0 {
		return false, fmt.Errorf("konnectivity-agent DaemonSet has no %q container", agentContainerName)
	}
	c := &ds.Spec.Template.Spec.Containers[idx]
	want := desiredAgentReadinessProbe(c)
	if equality.Semantic.DeepEqual(c.ReadinessProbe, want) {
		return false, nil
	}
	patch, err := json.Marshal(map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []map[string]any{{
						"name":           c.Name,
						"readinessProbe": want,
					}},
				},
			},
		},
	})
	if err != nil {
		return false, err
	}
	_, err = tenant.AppsV1().DaemonSets(agentNamespace).Patch(ctx, agentDaemonSet, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	return err == nil, err
}

// agentState summarises the tenant's agent pods for metrics, the tunnel
// verdict and per-agent remediation.
type agentState struct {
	known bool // false when the DaemonSet/pods couldn't be read

	desired int // DaemonSet desiredNumberScheduled
	ready   int // agent pods Running + Ready (connected to >=1 server, once our probe is live)

	nodesWithoutAgent []string // Ready, schedulable-or-not nodes with no live agent pod
	stuckTerminating  []string // agent pods terminating for longer than their grace period + slack
	notReadyTooLong   []*corev1.Pod

	probeLive bool // the DaemonSet carries our /readyz readinessProbe
}

// inspectAgents reads the agent DaemonSet, its pods and the tenant's nodes.
func (r *TenantControlPlaneReconciler) inspectAgents(ctx context.Context, tenant kubernetes.Interface, now time.Time) agentState {
	st := agentState{}
	ds, err := tenant.AppsV1().DaemonSets(agentNamespace).Get(ctx, agentDaemonSet, metav1.GetOptions{})
	if err != nil {
		return st
	}
	pods, err := tenant.CoreV1().Pods(agentNamespace).List(ctx, metav1.ListOptions{
		LabelSelector: agentPodLabelKey + "=" + agentPodLabelValue,
	})
	if err != nil {
		return st
	}
	nodes, err := tenant.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return st
	}
	st.known = true
	st.desired = int(ds.Status.DesiredNumberScheduled)
	st.probeLive = dsHasReadyzProbe(ds)

	restartAfter := r.agentRestartAfter()
	covered := map[string]bool{}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp != nil {
			grace := int64(30)
			if p.DeletionGracePeriodSeconds != nil {
				grace = *p.DeletionGracePeriodSeconds
			}
			if now.Sub(p.DeletionTimestamp.Time) > time.Duration(grace)*time.Second+2*time.Minute {
				// The kubelet could not kill it (the 10-21 Central incident:
				// read-only root filesystem). Nothing we can do from here —
				// surface it.
				st.stuckTerminating = append(st.stuckTerminating, p.Name)
			}
			continue
		}
		if p.Spec.NodeName != "" && p.Status.Phase == corev1.PodRunning {
			covered[p.Spec.NodeName] = true
		}
		if p.Status.Phase != corev1.PodRunning {
			continue
		}
		if podReady(p) {
			st.ready++
			continue
		}
		if st.probeLive && podNotReadyFor(p, now) >= restartAfter && containerUpFor(p, now) >= restartAfter {
			st.notReadyTooLong = append(st.notReadyTooLong, p)
		}
	}
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if !nodeReady(n) {
			continue // a NotReady node can't run an agent anyway; node health is alerted elsewhere
		}
		if !covered[n.Name] {
			st.nodesWithoutAgent = append(st.nodesWithoutAgent, n.Name)
		}
	}
	return st
}

// tunnelDown reports the "no agent connected at all" state, which is a dead
// tunnel regardless of what the dial probe saw.
func (s agentState) tunnelDown() bool {
	return s.known && s.probeLive && s.desired > 0 && s.ready == 0
}

func dsHasReadyzProbe(ds *appsv1.DaemonSet) bool {
	idx := agentContainerIndex(&ds.Spec.Template.Spec)
	if idx < 0 {
		return false
	}
	p := ds.Spec.Template.Spec.Containers[idx].ReadinessProbe
	return p != nil && p.HTTPGet != nil && p.HTTPGet.Path == agentReadyzPath
}

func podNotReadyFor(p *corev1.Pod, now time.Time) time.Duration {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status != corev1.ConditionTrue {
			if c.LastTransitionTime.IsZero() {
				return 0
			}
			return now.Sub(c.LastTransitionTime.Time)
		}
	}
	return 0
}

// containerUpFor is how long the agent container has been running in its
// current incarnation (0 when it isn't running) — a fresh agent gets the full
// restart window to connect before we judge it.
func containerUpFor(p *corev1.Pod, now time.Time) time.Duration {
	for _, cs := range p.Status.ContainerStatuses {
		if cs.Name != agentContainerName && len(p.Status.ContainerStatuses) > 1 {
			continue
		}
		if cs.State.Running == nil || cs.State.Running.StartedAt.IsZero() {
			return 0
		}
		return now.Sub(cs.State.Running.StartedAt.Time)
	}
	return 0
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func (r *TenantControlPlaneReconciler) agentRestartAfter() time.Duration {
	if r.AgentRestartAfter <= 0 {
		return 90 * time.Second
	}
	return r.AgentRestartAfter
}

// restartStuckAgents deletes agent pods that have been disconnected (NotReady
// on /readyz) for longer than AgentRestartAfter, so the DaemonSet replaces them
// — the liveness restart Kamaji's own /healthz probe can never trigger.
//
// While other agents are still connected it restarts at most ONE per call (one
// per recheck), so we never take healthy coverage down with it. When no agent
// is connected at all the tunnel is already dead and every stuck agent is
// restarted at once.
func (r *TenantControlPlaneReconciler) restartStuckAgents(ctx context.Context, tenant kubernetes.Interface, st agentState) ([]string, error) {
	if len(st.notReadyTooLong) == 0 {
		return nil, nil
	}
	victims := st.notReadyTooLong
	if st.ready > 0 {
		victims = victims[:1]
	}
	var restarted []string
	for _, p := range victims {
		err := tenant.CoreV1().Pods(p.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &p.UID},
		})
		if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return restarted, fmt.Errorf("delete stuck agent %s: %w", p.Name, err)
		}
		if err == nil {
			restarted = append(restarted, p.Name)
		}
	}
	return restarted, nil
}
