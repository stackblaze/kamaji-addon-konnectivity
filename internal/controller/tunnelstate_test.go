package controller

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestTunnelStatePatchTransitions(t *testing.T) {
	t0 := time.Date(2026, 10, 21, 19, 17, 0, 0, time.UTC)

	// Healthy on a TCP that has never been annotated: write "healthy".
	p := tunnelStatePatch(nil, true, "", t0)
	if p[AnnotationTunnelState] != tunnelStateHealthy {
		t.Fatalf("first healthy verdict should publish healthy, got %v", p)
	}
	// Healthy again: no write.
	if p := tunnelStatePatch(map[string]string{AnnotationTunnelState: tunnelStateHealthy}, true, "", t0); p != nil {
		t.Fatalf("steady healthy must not write, got %v", p)
	}

	// Healthy → unhealthy: since = now.
	p = tunnelStatePatch(map[string]string{AnnotationTunnelState: tunnelStateHealthy}, false, "wedged", t0)
	if p[AnnotationTunnelState] != tunnelStateUnhealthy || p[AnnotationUnhealthySince] != "2026-10-21T19:17:00Z" || p[AnnotationTunnelReason] != "wedged" {
		t.Fatalf("unexpected transition patch: %v", p)
	}

	cur := map[string]string{
		AnnotationTunnelState:    tunnelStateUnhealthy,
		AnnotationUnhealthySince: "2026-10-21T19:17:00Z",
		AnnotationTunnelReason:   "wedged",
	}
	// Still unhealthy, same reason, later: no write, since unchanged.
	if p := tunnelStatePatch(cur, false, "wedged", t0.Add(5*time.Minute)); p != nil {
		t.Fatalf("steady unhealthy must not write, got %v", p)
	}
	// Reason changes: write, but keep the episode's original since.
	p = tunnelStatePatch(cur, false, "no agent", t0.Add(5*time.Minute))
	if p[AnnotationUnhealthySince] != "2026-10-21T19:17:00Z" || p[AnnotationTunnelReason] != "no agent" {
		t.Fatalf("reason change must keep since: %v", p)
	}
	if got := unhealthySeconds(cur, t0.Add(150*time.Second)); got != 150 {
		t.Fatalf("unhealthySeconds = %v, want 150", got)
	}

	// Recovery deletes since + reason.
	p = tunnelStatePatch(cur, true, "", t0.Add(10*time.Minute))
	if p[AnnotationTunnelState] != tunnelStateHealthy || p[AnnotationUnhealthySince] != nil || p[AnnotationTunnelReason] != nil {
		t.Fatalf("recovery patch: %v", p)
	}
	if _, ok := p[AnnotationUnhealthySince]; !ok {
		t.Fatal("recovery must explicitly delete since (nil in a merge patch)")
	}
}

func TestClearTunnelStatePatch(t *testing.T) {
	if p := clearTunnelStatePatch(map[string]string{"other": "x"}); p != nil {
		t.Fatalf("nothing to clear, got %v", p)
	}
	p := clearTunnelStatePatch(map[string]string{AnnotationTunnelState: "unhealthy", AnnotationTunnelReason: "r"})
	if len(p) != 2 || p[AnnotationTunnelState] != nil {
		t.Fatalf("unexpected clear patch: %v", p)
	}
}

func tcpObj(gen int64, ann map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(tcpGVK)
	u.SetNamespace("zone")
	u.SetName("zone")
	u.SetGeneration(gen)
	u.SetAnnotations(ann)
	return u
}

func TestTCPEventFilter(t *testing.T) {
	f := tcpEventFilter()
	cases := []struct {
		name     string
		old, new *unstructured.Unstructured
		want     bool
	}{
		{"status-only update", tcpObj(3, nil), tcpObj(3, nil), false},
		{"our own annotation write", tcpObj(3, nil), tcpObj(3, map[string]string{AnnotationTunnelState: "unhealthy"}), false},
		{"spec change", tcpObj(3, nil), tcpObj(4, nil), true},
		{"opt-out annotation", tcpObj(3, nil), tcpObj(3, map[string]string{AnnotationVerify: "false"}), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := f.Update(event.UpdateEvent{ObjectOld: c.old, ObjectNew: c.new}); got != c.want {
				t.Fatalf("Update = %v, want %v", got, c.want)
			}
		})
	}
	if !f.Create(event.CreateEvent{Object: tcpObj(1, nil)}) {
		t.Fatal("create events must pass")
	}
}

func TestTunnelVerdict(t *testing.T) {
	ok := probeResult{healthy: true, reachable: true}
	wedged := probeResult{healthy: false, reachable: true}
	full := coverage{known: true, total: 2, covered: 2}
	gap := coverage{known: true, total: 2, covered: 1}
	fine := agentState{known: true, probeLive: true, desired: 3, ready: 3}
	none := agentState{known: true, probeLive: true, desired: 3, ready: 0}

	if h, _ := tunnelVerdict(ok, full, fine); !h {
		t.Fatal("all good must be healthy")
	}
	if h, r := tunnelVerdict(wedged, full, fine); h || r == "" {
		t.Fatal("wedged dials must be unhealthy")
	}
	if h, _ := tunnelVerdict(ok, gap, fine); h {
		t.Fatal("coverage gap must be unhealthy")
	}
	if h, r := tunnelVerdict(ok, full, none); h || r == "" {
		t.Fatal("no connected agent must be unhealthy even if dials passed")
	}
	// Agent state unknown (feature off / unreadable) never decides on its own.
	if h, _ := tunnelVerdict(ok, full, agentState{}); !h {
		t.Fatal("unknown agent state must not flip the verdict")
	}
}

func TestProbeDue(t *testing.T) {
	now := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	r := &TenantControlPlaneReconciler{SteadyInterval: 3 * time.Minute}
	fine := agentState{known: true, probeLive: true, desired: 3, ready: 3}

	if !r.probeDue("k", 1, now, fine, nil) {
		t.Fatal("never probed → due")
	}
	r.rememberProbe("k", 1, now)
	if r.probeDue("k", 1, now.Add(30*time.Second), fine, nil) {
		t.Fatal("healthy 30s ago, agents fine → not due")
	}
	if !r.probeDue("k", 1, now.Add(3*time.Minute), fine, nil) {
		t.Fatal("steady interval elapsed → due")
	}
	if !r.probeDue("k", 2, now.Add(30*time.Second), fine, nil) {
		t.Fatal("TCP generation changed → due")
	}
	if !r.probeDue("k", 1, now.Add(30*time.Second), agentState{known: true, probeLive: true, desired: 3, ready: 2}, nil) {
		t.Fatal("an agent not ready → due")
	}
	r.dropProbeMemo("k")
	if !r.probeDue("k", 1, now.Add(30*time.Second), fine, nil) {
		t.Fatal("memo dropped (last verdict unhealthy) → due")
	}
}

func TestHealthyRequeue(t *testing.T) {
	r := &TenantControlPlaneReconciler{SteadyInterval: 3 * time.Minute, AgentCheckInterval: 30 * time.Second}
	if r.healthyRequeue() != 3*time.Minute {
		t.Fatal("agent management off → steady interval")
	}
	r.ManageAgents = true
	if r.healthyRequeue() != 30*time.Second {
		t.Fatal("agent management on → agent check interval")
	}
}

func TestPublishTunnelStateWritesAnnotations(t *testing.T) {
	ctx := context.Background()
	tcp := tcpObj(1, map[string]string{"keep": "me"})
	c := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).WithObjects(tcp).Build()
	r := &TenantControlPlaneReconciler{Client: c}
	t0 := time.Date(2026, 10, 21, 19, 17, 0, 0, time.UTC)

	if err := r.publishTunnelState(ctx, tcp, false, "wedged", t0); err != nil {
		t.Fatalf("publish unhealthy: %v", err)
	}
	got := tcpObj(0, nil)
	if err := c.Get(ctx, client.ObjectKeyFromObject(tcp), got); err != nil {
		t.Fatal(err)
	}
	a := got.GetAnnotations()
	if a[AnnotationTunnelState] != "unhealthy" || a[AnnotationUnhealthySince] != "2026-10-21T19:17:00Z" || a["keep"] != "me" {
		t.Fatalf("annotations after unhealthy: %v", a)
	}

	if err := r.publishTunnelState(ctx, got, true, "", t0.Add(time.Minute)); err != nil {
		t.Fatalf("publish healthy: %v", err)
	}
	final := tcpObj(0, nil)
	_ = c.Get(ctx, client.ObjectKeyFromObject(tcp), final)
	a = final.GetAnnotations()
	if a[AnnotationTunnelState] != "healthy" || a[AnnotationUnhealthySince] != "" || a[AnnotationTunnelReason] != "" || a["keep"] != "me" {
		t.Fatalf("annotations after recovery: %v", a)
	}
}

func TestManagesAgentsOf(t *testing.T) {
	r := &TenantControlPlaneReconciler{}
	if r.managesAgentsOf("a", "a") {
		t.Fatal("feature off → never")
	}
	r.ManageAgents = true
	if !r.managesAgentsOf("a", "a") {
		t.Fatal("no allowlist → every tenant")
	}
	r.AgentTenants = map[string]bool{"shared-dev-west-1-a": true, "ns/other": true}
	if !r.managesAgentsOf("shared-dev-west-1-a", "shared-dev-west-1-a") || !r.managesAgentsOf("ns", "other") {
		t.Fatal("allowlisted tenants must be managed")
	}
	if r.managesAgentsOf("shared-west-1-a", "shared-west-1-a") {
		t.Fatal("tenants outside the allowlist must be left alone")
	}
}
