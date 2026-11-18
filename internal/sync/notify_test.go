package sync

import "testing"

func TestShouldWebhookAcceptsSuccessAndOk(t *testing.T) {
	cases := []struct {
		events string
		status string
		want   bool
	}{
		{"success", "ok", true},
		{"ok", "ok", true},
		{"success", "error", false},
		{"error", "error", true},
		{"ok,error", "error", true},
		{"success,error", "ok", true},
		{"all", "expired", true},
		{"none", "ok", false},
		{"", "ok", false},
		{"success", "expired", false},
	}
	for _, c := range cases {
		if got := shouldWebhook(c.events, c.status); got != c.want {
			t.Errorf("shouldWebhook(%q, %q) = %v, want %v", c.events, c.status, got, c.want)
		}
	}
}
