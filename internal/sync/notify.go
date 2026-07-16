package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

type WebhookPayload struct {
	Event             string `json:"event"`
	PairID            string `json:"pair_id"`
	PairName          string `json:"pair_name"`
	Status            string `json:"status"`
	ConsecutiveErrors int    `json:"consecutive_errors"`
	LastError         string `json:"last_error,omitempty"`
	Succeeded         int    `json:"succeeded"`
	Failed            int    `json:"failed"`
	StartedAt         string `json:"started_at"`
	CompletedAt       string `json:"completed_at"`
	Source            string `json:"source"`
	Target            string `json:"target"`
}

func SendWebhook(url, events string, p WebhookPayload) {
	if url == "" {
		return
	}
	if !shouldWebhook(events, p.Status) {
		return
	}

	body, _ := json.Marshal(p)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		slog.Warn("webhook: create request", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Warn("webhook: delivery failed", "url", trunc(url, 60), "error", err)
		return
	}
	resp.Body.Close()
	slog.Debug("webhook delivered", "url", trunc(url, 60), "status", resp.StatusCode)
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func shouldWebhook(events, status string) bool {
	if events == "" || events == "none" {
		return false
	}
	if events == "all" {
		return true
	}
	for _, e := range strings.Split(events, ",") {
		e = strings.TrimSpace(e)
		if e == status || e == "all" {
			return true
		}
	}
	return false
}
