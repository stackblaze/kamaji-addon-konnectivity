package controller

import "testing"

func TestParseReadyBackends(t *testing.T) {
	cases := []struct {
		name string
		body string
		want float64
		ok   bool
	}{
		{
			name: "present, no labels",
			body: "# HELP konnectivity_network_proxy_server_ready_backend_connections agents\n" +
				"# TYPE konnectivity_network_proxy_server_ready_backend_connections gauge\n" +
				"konnectivity_network_proxy_server_ready_backend_connections 3\n",
			want: 3, ok: true,
		},
		{
			name: "zero (uncovered server)",
			body: "konnectivity_network_proxy_server_ready_backend_connections 0\n",
			want: 0, ok: true,
		},
		{
			name: "with a label set",
			body: `konnectivity_network_proxy_server_ready_backend_connections{foo="bar"} 2` + "\n",
			want: 2, ok: true,
		},
		{
			name: "absent",
			body: "some_other_metric 5\n",
			want: 0, ok: false,
		},
		{
			name: "prefix collision is not matched",
			body: "konnectivity_network_proxy_server_ready_backend_connections_created 12345\n",
			want: 0, ok: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseReadyBackends(c.body)
			if ok != c.ok || got != c.want {
				t.Fatalf("parseReadyBackends = (%v, %v), want (%v, %v)", got, ok, c.want, c.ok)
			}
		})
	}
}

func TestCoverageIncomplete(t *testing.T) {
	cases := []struct {
		name string
		cov  coverage
		want bool
	}{
		{"all covered", coverage{total: 2, covered: 2, known: true}, false},
		{"one dark server", coverage{total: 2, covered: 1, known: true}, true},
		{"none covered", coverage{total: 2, covered: 0, known: true}, true},
		{"unknown never incomplete", coverage{total: 2, covered: 0, known: false}, false},
		{"no servers is not incomplete", coverage{total: 0, covered: 0, known: true}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.cov.incomplete(); got != c.want {
				t.Fatalf("incomplete() = %v, want %v", got, c.want)
			}
		})
	}
}
