package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// tunnelHealthy is 1 when the last probe of a tenant's konnectivity tunnel
	// served dials on every node, 0 when it was wedged/unreachable.
	tunnelHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "konnectivity_tunnel_healthy",
		Help: "1 if the tenant konnectivity tunnel served apiserver→in-cluster dials at the last check, else 0.",
	}, []string{"namespace", "tcp"})

	// remediationsTotal counts agent-DaemonSet bounces the controller issued.
	remediationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "konnectivity_remediations_total",
		Help: "Number of konnectivity-agent DaemonSet bounces issued to recover a wedged tunnel.",
	}, []string{"namespace", "tcp"})

	// probeErrorsTotal counts probe attempts that could not even reach the
	// tenant apiserver (CP not ready / kubeconfig missing) — distinct from a
	// reachable-but-wedged tunnel.
	probeErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "konnectivity_probe_errors_total",
		Help: "Number of probes that could not reach the tenant apiserver at all (not a tunnel wedge).",
	}, []string{"namespace", "tcp"})

	// serversTotal / serversCovered expose per-server tunnel coverage for an HA
	// control plane: how many konnectivity-server replicas exist vs. how many
	// have >=1 connected agent. covered < total is the HA coverage gap (a CP
	// replica whose apiserver egress-fails); alert on it directly.
	serversTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "konnectivity_servers_total",
		Help: "Number of konnectivity-server replicas (Running+Ready control-plane pods) for the tenant.",
	}, []string{"namespace", "tcp"})

	serversCovered = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "konnectivity_servers_covered",
		Help: "How many of the tenant's konnectivity-server replicas have >=1 connected agent (tunnel coverage).",
	}, []string{"namespace", "tcp"})
)

func init() {
	metrics.Registry.MustRegister(tunnelHealthy, remediationsTotal, probeErrorsTotal, serversTotal, serversCovered)
}

// deleteTenantMetrics drops a tenant's gauge series when its TCP is gone or
// konnectivity is disabled, so stale series don't linger. Counters are left
// alone (they're cumulative and harmless).
func deleteTenantMetrics(namespace, tcp string) {
	tunnelHealthy.DeleteLabelValues(namespace, tcp)
	serversTotal.DeleteLabelValues(namespace, tcp)
	serversCovered.DeleteLabelValues(namespace, tcp)
}
