package controller

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func tcpWith(addons map[string]interface{}) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if addons != nil {
		_ = unstructured.SetNestedMap(u.Object, addons, "spec", "addons")
	}
	return u
}

func TestKonnectivityEnabled(t *testing.T) {
	cases := []struct {
		name   string
		addons map[string]interface{}
		want   bool
	}{
		{"no addons", nil, false},
		{"addons without konnectivity", map[string]interface{}{"coreDNS": map[string]interface{}{}}, false},
		{"konnectivity empty object", map[string]interface{}{"konnectivity": map[string]interface{}{}}, true},
		{"konnectivity configured", map[string]interface{}{"konnectivity": map[string]interface{}{"server": map[string]interface{}{"port": int64(8132)}}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := konnectivityEnabled(tcpWith(c.addons)); got != c.want {
				t.Fatalf("konnectivityEnabled = %v, want %v", got, c.want)
			}
		})
	}
}

func TestBounceCooldown(t *testing.T) {
	r := &TenantControlPlaneReconciler{BounceCooldown: time.Minute, lastBounce: map[string]time.Time{}}
	const key = "ns/tcp"

	if r.recentlyBounced(key) {
		t.Fatal("nothing bounced yet, should not be in cooldown")
	}
	r.markBounced(key)
	if !r.recentlyBounced(key) {
		t.Fatal("just bounced, should be in cooldown")
	}
	// Expire the cooldown by backdating the record.
	r.mu.Lock()
	r.lastBounce[key] = time.Now().Add(-2 * time.Minute)
	r.mu.Unlock()
	if r.recentlyBounced(key) {
		t.Fatal("cooldown elapsed, should allow another bounce")
	}
	r.forget(key)
	if _, ok := r.lastBounce[key]; ok {
		t.Fatal("forget should drop the record")
	}
}
