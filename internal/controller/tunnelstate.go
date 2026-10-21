package controller

import (
	"context"
	"encoding/json"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// Tunnel state published on the TenantControlPlane, for alerting.
//
// The platform's alert path for Kamaji is k8s-healthz (one per region's
// management cluster): it lists TenantControlPlanes, serves
// /tenant/<name> as 200/503, and Checkmate polls one HTTP monitor per tenant
// ("kamaji-<region> · Tenant <name>") and notifies on 503. That bridge already
// reads every TCP, so the cheapest reliable hand-off is three annotations on
// the TCP itself — no new endpoint, no new RBAC on the bridge side:
//
//	konnectivity.stackblaze.com/tunnel-state           healthy | unhealthy
//	konnectivity.stackblaze.com/tunnel-unhealthy-since RFC3339, first unhealthy verdict of the current episode
//	konnectivity.stackblaze.com/tunnel-reason          short human-readable reason
//
// The bridge turns "unhealthy for longer than N seconds" (default 120) into a
// 503 on the tenant's monitor. The controller only writes on a TRANSITION (or a
// reason change), never on every verdict, and its own watch ignores updates
// that touch nothing but these keys — see tcpEventFilter.
const (
	AnnotationTunnelState     = "konnectivity.stackblaze.com/tunnel-state"
	AnnotationUnhealthySince  = "konnectivity.stackblaze.com/tunnel-unhealthy-since"
	AnnotationTunnelReason    = "konnectivity.stackblaze.com/tunnel-reason"
	tunnelStateHealthy        = "healthy"
	tunnelStateUnhealthy      = "unhealthy"
	maxTunnelReasonAnnotation = 256
)

var ownedAnnotations = []string{AnnotationTunnelState, AnnotationUnhealthySince, AnnotationTunnelReason}

// tunnelStatePatch returns the annotation merge-patch that moves the TCP to the
// given verdict, or nil when the TCP already says exactly that. A nil value in
// the map deletes the key (JSON merge patch semantics).
func tunnelStatePatch(current map[string]string, healthy bool, reason string, now time.Time) map[string]any {
	if healthy {
		if current[AnnotationTunnelState] == tunnelStateHealthy &&
			current[AnnotationUnhealthySince] == "" && current[AnnotationTunnelReason] == "" {
			return nil
		}
		return map[string]any{
			AnnotationTunnelState:    tunnelStateHealthy,
			AnnotationUnhealthySince: nil,
			AnnotationTunnelReason:   nil,
		}
	}
	if len(reason) > maxTunnelReasonAnnotation {
		reason = reason[:maxTunnelReasonAnnotation]
	}
	since := current[AnnotationUnhealthySince]
	if current[AnnotationTunnelState] != tunnelStateUnhealthy || since == "" {
		since = now.UTC().Format(time.RFC3339)
	} else if _, err := time.Parse(time.RFC3339, since); err != nil {
		since = now.UTC().Format(time.RFC3339)
	}
	if current[AnnotationTunnelState] == tunnelStateUnhealthy &&
		current[AnnotationUnhealthySince] == since && current[AnnotationTunnelReason] == reason {
		return nil
	}
	return map[string]any{
		AnnotationTunnelState:    tunnelStateUnhealthy,
		AnnotationUnhealthySince: since,
		AnnotationTunnelReason:   reason,
	}
}

// clearTunnelStatePatch removes every annotation we own (tenant opted out or
// konnectivity disabled), or nil when there is nothing to remove.
func clearTunnelStatePatch(current map[string]string) map[string]any {
	var p map[string]any
	for _, k := range ownedAnnotations {
		if _, ok := current[k]; ok {
			if p == nil {
				p = map[string]any{}
			}
			p[k] = nil
		}
	}
	return p
}

// applyTunnelAnnotations merge-patches the TCP's annotations. A nil patch is a
// no-op.
func (r *TenantControlPlaneReconciler) applyTunnelAnnotations(ctx context.Context, tcp *unstructured.Unstructured, annotations map[string]any) error {
	if annotations == nil {
		return nil
	}
	body, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": annotations}})
	if err != nil {
		return err
	}
	return r.Patch(ctx, tcp, client.RawPatch("application/merge-patch+json", body))
}

// unhealthySeconds is how long the TCP's current unhealthy episode has lasted
// per its annotations (0 when healthy / unknown).
func unhealthySeconds(annotations map[string]string, now time.Time) float64 {
	if annotations[AnnotationTunnelState] != tunnelStateUnhealthy {
		return 0
	}
	since, err := time.Parse(time.RFC3339, annotations[AnnotationUnhealthySince])
	if err != nil {
		return 0
	}
	if d := now.Sub(since); d > 0 {
		return d.Seconds()
	}
	return 0
}

// tcpEventFilter drops TenantControlPlane UPDATE events whose only change is
// in status or in the annotations this controller owns. Without it, every
// tunnel-state write would immediately re-trigger a reconcile (and an extra
// probe of the tenant), and Kamaji's frequent status writes would too. The
// periodic cadence is driven by RequeueAfter, not by events.
func tcpEventFilter() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return true
			}
			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}
			if (e.ObjectOld.GetDeletionTimestamp() == nil) != (e.ObjectNew.GetDeletionTimestamp() == nil) {
				return true
			}
			return !annotationsEqualIgnoringOwned(e.ObjectOld.GetAnnotations(), e.ObjectNew.GetAnnotations())
		},
	}
}

func annotationsEqualIgnoringOwned(a, b map[string]string) bool {
	strip := func(m map[string]string) map[string]string {
		out := make(map[string]string, len(m))
		for k, v := range m {
			out[k] = v
		}
		for _, k := range ownedAnnotations {
			delete(out, k)
		}
		return out
	}
	sa, sb := strip(a), strip(b)
	if len(sa) != len(sb) {
		return false
	}
	for k, v := range sa {
		if w, ok := sb[k]; !ok || w != v {
			return false
		}
	}
	return true
}
