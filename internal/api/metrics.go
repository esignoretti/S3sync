package api

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricsRegistry = prometheus.NewRegistry()
	metricsFactory  = promauto.With(metricsRegistry)

	syncRunsTotal = metricsFactory.NewCounterVec(prometheus.CounterOpts{
		Name: "s3sync_sync_runs_total",
		Help: "Total sync run completions",
	}, []string{"pair_id", "pair_name", "status"})

	objectsCopiedTotal = metricsFactory.NewCounterVec(prometheus.CounterOpts{
		Name: "s3sync_objects_copied_total",
		Help: "Objects copied from source to target",
	}, []string{"pair_id", "pair_name"})

	objectsDeletedTotal = metricsFactory.NewCounterVec(prometheus.CounterOpts{
		Name: "s3sync_objects_deleted_total",
		Help: "Objects deleted from target",
	}, []string{"pair_id", "pair_name"})

	bytesTransferredTotal = metricsFactory.NewCounterVec(prometheus.CounterOpts{
		Name: "s3sync_bytes_transferred_total",
		Help: "Bytes transferred (copied) from source",
	}, []string{"pair_id", "pair_name"})

	syncDurationSeconds = metricsFactory.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3sync_sync_duration_seconds",
		Help:    "Sync run duration",
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800},
	}, []string{"pair_id", "pair_name"})
)

func RegisterSyncPairMetrics(pairID, pairName string) {
	// Metrics are created on first use — no explicit registration needed.
	// This function exists for symmetry with Unregister.
}

func UnregisterSyncPairMetrics(pairID string) {
	labels := prometheus.Labels{"pair_id": pairID}
	syncRunsTotal.DeletePartialMatch(labels)
	objectsCopiedTotal.DeletePartialMatch(labels)
	objectsDeletedTotal.DeletePartialMatch(labels)
	bytesTransferredTotal.DeletePartialMatch(labels)
	syncDurationSeconds.DeletePartialMatch(labels)
}

func RecordSyncMetrics(pairID, pairName string, succeeded bool, copied, deleted, bytes int64, duration float64) {
	status := "success"
	if !succeeded {
		status = "fail"
	}
	syncRunsTotal.WithLabelValues(pairID, pairName, status).Inc()
	if copied > 0 {
		objectsCopiedTotal.WithLabelValues(pairID, pairName).Add(float64(copied))
	}
	if deleted > 0 {
		objectsDeletedTotal.WithLabelValues(pairID, pairName).Add(float64(deleted))
	}
	if bytes > 0 {
		bytesTransferredTotal.WithLabelValues(pairID, pairName).Add(float64(bytes))
	}
	syncDurationSeconds.WithLabelValues(pairID, pairName).Observe(duration)
}
