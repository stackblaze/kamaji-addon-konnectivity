package controller

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// readyBackendsMetric is the konnectivity-server gauge for the number of
// connected agents ("backends") on THAT server process. Each Kamaji
// control-plane pod runs its own konnectivity-server sidecar, so with an HA
// (multi-replica) control plane there are N servers. A value of 0 on any one of
// them means that server has no agent tunnel — so every apiserver request the
// service load-balances to its co-located apiserver replica egress-fails
// (admission webhooks time out, logs/exec hang). That per-replica coverage gap
// is exactly what a single-VIP-in-front-of-N-servers topology produces, and it
// can't be seen from a statistical dial probe — only by reading each server.
// The current gauge (labeled by proxy_strategy) and the deprecated unlabeled
// one it replaced. We prefer the current name and fall back to the deprecated
// one, so this works across konnectivity-server versions.
const (
	readyBackendsMetric     = "konnectivity_network_proxy_server_ready_backends"
	readyBackendsMetricDepr = "konnectivity_network_proxy_server_ready_backend_connections"
)

// defaultServerMetricsPort is the konnectivity-server admin port (serves
// /metrics). Kamaji names it `adminport` on the control-plane pod.
const defaultServerMetricsPort = 8133

// coverage is the per-server tunnel-coverage result for one TenantControlPlane.
type coverage struct {
	total   int  // konnectivity-server replicas that are Running+Ready
	covered int  // …of those, how many report >=1 connected agent
	known   bool // false when coverage couldn't be determined (don't act on it)
}

// incomplete reports a real coverage gap: we know coverage, there is at least
// one server, and at least one server has no agent tunnel.
func (c coverage) incomplete() bool { return c.known && c.total > 0 && c.covered < c.total }

// checkCoverage lists a TCP's control-plane pods (each carrying a
// konnectivity-server sidecar) and scrapes every server's admin /metrics for
// its connected-agent count, so the controller can DETERMINISTICALLY tell that
// each server replica has a tunnel — rather than inferring it from a
// probabilistic dial probe that can pass while one replica is dark.
//
// Best-effort by design: if the pods can't be listed, or not every Running
// server could be scraped, it returns known=false so the caller falls back to
// the dial probe instead of bouncing on incomplete information.
func (r *TenantControlPlaneReconciler) checkCoverage(ctx context.Context, ns, name string) coverage {
	var pods corev1.PodList
	if err := r.APIReader.List(ctx, &pods,
		client.InNamespace(ns),
		client.MatchingLabels{
			"kamaji.clastix.io/name":      name,
			"kamaji.clastix.io/component": "deployment",
		},
	); err != nil {
		return coverage{known: false}
	}

	cov := coverage{}
	scraped := 0
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase != corev1.PodRunning || p.Status.PodIP == "" || !podReady(p) {
			continue
		}
		cov.total++
		n, ok := r.scrapeReadyBackends(ctx, p.Status.PodIP)
		if !ok {
			continue
		}
		scraped++
		if n >= 1 {
			cov.covered++
		}
	}
	// Only claim to KNOW coverage when we scraped every Running server; a
	// partial scrape (one server's admin port unreachable) must not be read as
	// "that server is uncovered" and trigger a needless bounce.
	cov.known = cov.total > 0 && scraped == cov.total
	return cov
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// scrapeReadyBackends fetches one konnectivity-server's admin /metrics and
// returns its ready-backend (connected-agent) count.
func (r *TenantControlPlaneReconciler) scrapeReadyBackends(ctx context.Context, podIP string) (float64, bool) {
	port := r.ServerMetricsPort
	if port == 0 {
		port = defaultServerMetricsPort
	}
	timeout := r.CoverageTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := fmt.Sprintf("http://%s/metrics", net.JoinHostPort(podIP, strconv.Itoa(port)))
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, false
	}
	resp, err := r.httpClient().Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, false
	}
	return parseReadyBackends(string(body))
}

func (r *TenantControlPlaneReconciler) httpClient() *http.Client {
	if r.HTTPClient != nil {
		return r.HTTPClient
	}
	return http.DefaultClient
}

// parseReadyBackends returns the connected-agent count from a Prometheus text
// exposition body. It prefers the current `ready_backends` gauge (summing its
// per-proxy_strategy series) and falls back to the deprecated unlabeled
// `ready_backend_connections`. ok=false when neither is present.
func parseReadyBackends(body string) (float64, bool) {
	if v, ok := sumMetric(body, readyBackendsMetric); ok {
		return v, true
	}
	return sumMetric(body, readyBackendsMetricDepr)
}

// sumMetric sums every sample of `name` in a Prometheus text body. A sample
// matches only when the char after the name starts a value (space/tab) or a
// label set ('{'), so a longer metric sharing the prefix isn't mis-counted.
func sumMetric(body, name string) (float64, bool) {
	var sum float64
	found := false
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || !strings.HasPrefix(line, name) {
			continue
		}
		rest := line[len(name):]
		if rest == "" || (rest[0] != ' ' && rest[0] != '\t' && rest[0] != '{') {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		sum += v
		found = true
	}
	return sum, found
}
