package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	OriginRangeOutputWait = promauto.NewCounter(prometheus.CounterOpts{Name: "tag_origin_range_output_wait_seconds_total", Help: "Time origin producers paused because all client broadcast queues were full."})
	OriginRangeEvents     = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "tag_origin_range_events_total", Help: "Range reader admissions, completions, hedges, and budget fallbacks.",
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
