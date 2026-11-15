package sync

import (
	"encoding/json"
	"strings"
	"testing"
)

// AdvisoryWarning is serialised to API clients and rendered by app.js, which
// reads w.message. Without the JSON tags the UI shows "- undefined" for every
// advisory this feature added.
func TestAdvisoryWarningJSONTags(t *testing.T) {
	got, err := json.Marshal(AdvisoryWarning{Code: "noncurrent-recommended", Message: "y"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"code":"noncurrent-recommended"`, `"message":"y"`} {
		if !strings.Contains(string(got), want) {
			t.Errorf("json = %s, want it to contain %s", got, want)
		}
	}
}
