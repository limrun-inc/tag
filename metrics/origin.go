package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// OriginOutputWait is labeled by the reader that paused: "range" for the parallel
	// origin reader, "stream" for the single origin body.
	OriginOutputWait = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tag_origin_output_wait_seconds_total", Help: "Time origin readers paused because every client broadcast queue was full.",
	}, []string{"reader"})
	OriginRangeEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tag_origin_range_events_total", Help: "Range reader admissions, completions, hedges, HTTP 429/503 responses, and budget fallbacks.",
	}, []string{"event"})
	OriginRangeBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tag_origin_range_bytes_total", Help: "Bytes read from origin bodies, delivered, and discarded by the range reader.",
	}, []string{"kind"})
	OriginRangeReservedBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "tag_origin_range_reserved_bytes", Help: "Range buffer reservations charged to the shared cache memory budget.",
	})
	OriginRangeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "tag_origin_range_duration_seconds", Help: "Range body lifetime, including downstream backpressure.",
		Buckets: []float64{.1, .25, .5, 1, 2, 4, 8, 16, 32, 64, 128, 300},
	}, []string{"result"})
)
