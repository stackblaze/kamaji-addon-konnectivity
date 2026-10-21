package controller

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// kamajiAgentArgs is what Kamaji 26.7.x renders for a hostNetwork agent.
var kamajiAgentArgs = []string{
	"--admin-server-port=8133",
	"--ca-cert=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
	"--health-server-port=8134",
	"--logtostderr=true",
	"--proxy-server-host=zone-konnectivity.example",
	"--proxy-server-port=443",
	"--sync-forever=true",
	"-v=8",
}

func agentDS(args []string, readiness *corev1.Probe, desired int32) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: agentDaemonSet, Namespace: agentNamespace},
		Spec: appsv1.DaemonSetSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					HostNetwork: true,
					Containers: []corev1.Container{{
						Name:           agentContainerName,
						Command:        []string{"/proxy-agent"},
						Args:           args,
						ReadinessProbe: readiness,
					}},
				},
			},
		},
		Status: appsv1.DaemonSetStatus{DesiredNumberScheduled: desired},
	}
}

func TestAgentHealthEndpoint(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		wantHost string
		wantPort int
	}{
		{"kamaji 26.7 args", kamajiAgentArgs, "", 8134},
		{"no flag → upstream default", []string{"--proxy-server-host=x"}, "", 8093},
		{"space-separated flag", []string{"--health-server-port", "9000"}, "", 9000},
		{"loopback host (newer Kamaji, hostNetwork)", append([]string{"--health-server-host=127.0.0.1"}, kamajiAgentArgs...), "127.0.0.1", 8134},
		{"unspecified host probes the pod IP", []string{"--health-server-host=0.0.0.0", "--health-server-port=8134"}, "", 8134},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			host, port := agentHealthEndpoint(&corev1.Container{Args: c.args})
			if host != c.wantHost || port != c.wantPort {
				t.Fatalf("got (%q, %d), want (%q, %d)", host, port, c.wantHost, c.wantPort)
			}
		})
	}
}

func TestEnsureAgentReadinessProbeIsIdempotent(t *testing.T) {
	ctx := context.Background()
	cs := fake.NewClientset(agentDS(kamajiAgentArgs, nil, 3))
	r := &TenantControlPlaneReconciler{}

	patched, err := r.ensureAgentReadinessProbe(ctx, cs)
	if err != nil || !patched {
		t.Fatalf("first pass: patched=%v err=%v, want patched", patched, err)
	}
	ds, _ := cs.AppsV1().DaemonSets(agentNamespace).Get(ctx, agentDaemonSet, metav1.GetOptions{})
	p := ds.Spec.Template.Spec.Containers[0].ReadinessProbe
	if p == nil || p.HTTPGet == nil || p.HTTPGet.Path != "/readyz" || p.HTTPGet.Port.IntValue() != 8134 {
		t.Fatalf("readinessProbe not set to /readyz:8134: %+v", p)
	}
	if !dsHasReadyzProbe(ds) {
		t.Fatal("dsHasReadyzProbe should see the probe")
	}
	// Kamaji's own fields must be untouched.
	if got := ds.Spec.Template.Spec.Containers[0].Args; len(got) != len(kamajiAgentArgs) {
		t.Fatalf("args changed: %v", got)
	}

	before := len(cs.Actions())
	patched, err = r.ensureAgentReadinessProbe(ctx, cs)
	if err != nil || patched {
		t.Fatalf("second pass: patched=%v err=%v, want no-op", patched, err)
	}
	for _, a := range cs.Actions()[before:] {
		if a.GetVerb() == "patch" {
			t.Fatal("second pass must not patch an already-correct DaemonSet (that would roll the agents)")
		}
	}
}

func TestEnsureAgentReadinessProbeRepairsWrongProbe(t *testing.T) {
	ctx := context.Background()
	wrong := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/healthz"}}}
	cs := fake.NewClientset(agentDS(kamajiAgentArgs, wrong, 3))
	r := &TenantControlPlaneReconciler{}
	if patched, err := r.ensureAgentReadinessProbe(ctx, cs); err != nil || !patched {
		t.Fatalf("patched=%v err=%v, want a repair", patched, err)
	}
}

func readyCond(status corev1.ConditionStatus, since time.Time) corev1.PodCondition {
	return corev1.PodCondition{Type: corev1.PodReady, Status: status, LastTransitionTime: metav1.NewTime(since)}
}

func agentPod(name, node string, ready bool, notReadySince, startedAt time.Time) *corev1.Pod {
	st := corev1.ConditionTrue
	if !ready {
		st = corev1.ConditionFalse
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: agentNamespace, UID: types.UID(name),
			Labels: map[string]string{agentPodLabelKey: agentPodLabelValue},
		},
		Spec: corev1.PodSpec{NodeName: node},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{readyCond(st, notReadySince)},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  agentContainerName,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(startedAt)}},
			}},
		},
	}
}

func readyNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	}
}

func TestInspectAndRestartAgents(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 21, 19, 30, 0, 0, time.UTC)
	long := now.Add(-10 * time.Minute)

	stuckDeleting := agentPod("agent-terminating", "n5", false, long, long)
	stuckDeleting.DeletionTimestamp = &metav1.Time{Time: now.Add(-90 * time.Minute)}
	grace := int64(30)
	stuckDeleting.DeletionGracePeriodSeconds = &grace

	objs := []runtime.Object{
		agentDS(kamajiAgentArgs, desiredAgentReadinessProbe(&corev1.Container{Args: kamajiAgentArgs}), 5),
		agentPod("agent-ok", "n1", true, long, long),
		agentPod("agent-disconnected-a", "n2", false, long, long),
		agentPod("agent-disconnected-b", "n3", false, long, long),
		// NotReady long enough, but the container only just (re)started: give it time.
		agentPod("agent-fresh", "n4", false, long, now.Add(-20*time.Second)),
		stuckDeleting,
		readyNode("n1"), readyNode("n2"), readyNode("n3"), readyNode("n4"), readyNode("n5"), readyNode("build"),
	}
	cs := fake.NewClientset(objs...)
	r := &TenantControlPlaneReconciler{AgentRestartAfter: 90 * time.Second}

	st := r.inspectAgents(ctx, cs, now)
	if !st.known || st.desired != 5 || st.ready != 1 || !st.probeLive {
		t.Fatalf("unexpected state: %+v", st)
	}
	if len(st.notReadyTooLong) != 2 {
		t.Fatalf("notReadyTooLong = %d, want 2 (fresh agent excluded)", len(st.notReadyTooLong))
	}
	if len(st.stuckTerminating) != 1 || st.stuckTerminating[0] != "agent-terminating" {
		t.Fatalf("stuckTerminating = %v", st.stuckTerminating)
	}
	// n5's only agent is the unkillable one; "build" has none at all.
	if want := map[string]bool{"n5": true, "build": true}; len(st.nodesWithoutAgent) != 2 ||
		!want[st.nodesWithoutAgent[0]] || !want[st.nodesWithoutAgent[1]] {
		t.Fatalf("nodesWithoutAgent = %v, want n5 + build", st.nodesWithoutAgent)
	}
	if st.tunnelDown() {
		t.Fatal("one agent is connected; tunnel is not down")
	}

	// Another agent is still connected → restart only ONE per pass.
	restarted, err := r.restartStuckAgents(ctx, cs, st)
	if err != nil || len(restarted) != 1 {
		t.Fatalf("restarted=%v err=%v, want exactly one", restarted, err)
	}
	if countDeletes(cs.Actions()) != 1 {
		t.Fatalf("deletes = %d, want 1", countDeletes(cs.Actions()))
	}
}

func TestRestartAllWhenNoAgentConnected(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 21, 19, 30, 0, 0, time.UTC)
	long := now.Add(-10 * time.Minute)
	cs := fake.NewClientset(
		agentDS(kamajiAgentArgs, desiredAgentReadinessProbe(&corev1.Container{Args: kamajiAgentArgs}), 3),
		agentPod("a", "n1", false, long, long),
		agentPod("b", "n2", false, long, long),
		agentPod("c", "n3", false, long, long),
		readyNode("n1"), readyNode("n2"), readyNode("n3"),
	)
	r := &TenantControlPlaneReconciler{AgentRestartAfter: 90 * time.Second}
	st := r.inspectAgents(ctx, cs, now)
	if !st.tunnelDown() {
		t.Fatalf("0/3 ready with the probe live must read as tunnel down: %+v", st)
	}
	restarted, err := r.restartStuckAgents(ctx, cs, st)
	if err != nil || len(restarted) != 3 {
		t.Fatalf("restarted=%v err=%v, want all three", restarted, err)
	}
}

func TestNoRestartBeforeProbeIsLive(t *testing.T) {
	// Without our readinessProbe, Ready says nothing about the tunnel, so the
	// controller must neither restart agents nor call the tunnel down.
	ctx := context.Background()
	now := time.Now()
	long := now.Add(-10 * time.Minute)
	cs := fake.NewClientset(
		agentDS(kamajiAgentArgs, nil, 1),
		agentPod("a", "n1", false, long, long),
		readyNode("n1"),
	)
	r := &TenantControlPlaneReconciler{}
	st := r.inspectAgents(ctx, cs, now)
	if len(st.notReadyTooLong) != 0 || st.tunnelDown() {
		t.Fatalf("must not act before the probe is live: %+v", st)
	}
}

func countDeletes(actions []k8stesting.Action) int {
	n := 0
	for _, a := range actions {
		if a.GetVerb() == "delete" {
			n++
		}
	}
	return n
}
