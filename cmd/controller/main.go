// kamaji-addon-konnectivity controller — runs in the management cluster.
//
// Watches Kamaji TenantControlPlane CRs and, for every konnectivity-enabled
// tenant, continuously verifies that the konnectivity tunnel actually SERVES
// dials (apiserver→in-cluster) — the path admission webhooks (CNPG/Postgres,
// MariaDB) and apiserver→kubelet (logs/exec/metrics) ride. When the tunnel is
// up-but-wedged (the agent registers on the konnectivity-server but every
// DIAL_REQ times out — e.g. an agent attached to the terminating apiserver pod
// during a control-plane roll), it self-heals by bouncing the tenant's
// konnectivity-agent DaemonSet.
//
// This is the standing, fleet-wide complement to the one-shot verification
// done at provision time (tenant-clusters create-tenant.yml): it catches
// tunnels that wedge or regress long after the cluster was first marked ready.
package main

import (
	"flag"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/stackblaze/kamaji-addon-konnectivity/internal/controller"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	// clientgoscheme registers core/v1 (Secrets) + apps/v1 used by the reconciler.
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr = flag.String("metrics-bind-address", ":8080", "metrics server bind address")
		probeAddr   = flag.String("health-probe-bind-address", ":8081", "health probe bind address")
		leaderElect = flag.Bool("leader-elect", true, "enable leader election (recommended for multi-replica)")
		leaderID    = flag.String("leader-election-id", "kamaji-addon-konnectivity.stackblaze.com", "leader election lease name")

		steadyInterval  = flag.Duration("verify-interval", 3*time.Minute, "re-verify a healthy tunnel after this interval")
		recheckInterval = flag.Duration("recheck-interval", 30*time.Second, "re-verify after a remediation/transient failure")
		bounceCooldown  = flag.Duration("bounce-cooldown", 5*time.Minute, "minimum time between agent-DaemonSet bounces for the same tenant")
		probeTimeout    = flag.Duration("probe-timeout", 10*time.Second, "per-call timeout for a tunnel dial probe (a wedged dial otherwise hangs ~30s)")
		probeRounds     = flag.Int("probe-rounds", 3, "how many rounds to probe every node (covers both round-robined apiserver replicas)")

		serverMetricsPort = flag.Int("server-metrics-port", 8133, "konnectivity-server admin port serving /metrics (per-server agent-coverage check)")
		coverageTimeout   = flag.Duration("coverage-timeout", 5*time.Second, "per-server /metrics scrape timeout for the coverage check")
	)
	zapOpts := zap.Options{Development: false}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrllog.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: *metricsAddr},
		HealthProbeBindAddress: *probeAddr,
		LeaderElection:         *leaderElect,
		LeaderElectionID:       *leaderID,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err := (&controller.TenantControlPlaneReconciler{
		Client: mgr.GetClient(),
		// Uncached reader for admin-kubeconfig Secrets — avoids watching/caching
		// every Secret in the management cluster (broad + heavy); we only ever
		// GET one known secret per tenant on demand.
		APIReader:       mgr.GetAPIReader(),
		Scheme:          mgr.GetScheme(),
		Recorder:        mgr.GetEventRecorderFor("kamaji-addon-konnectivity"),
		SteadyInterval:  *steadyInterval,
		RecheckInterval: *recheckInterval,
		BounceCooldown:  *bounceCooldown,
		ProbeTimeout:    *probeTimeout,
		ProbeRounds:     *probeRounds,

		ServerMetricsPort: *serverMetricsPort,
		CoverageTimeout:   *coverageTimeout,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to set up reconciler")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "add healthz check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "add readyz check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited with error")
		os.Exit(1)
	}
}
