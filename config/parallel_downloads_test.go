package config

import "testing"

func TestParallelDownloadsEnv(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  bool
	}{{"true", true}, {"1", true}, {"false", false}, {"0", false}, {"typo", false}} {
		t.Run(tt.value, func(t *testing.T) {
			t.Setenv("TAG_PARALLEL_DOWNLOADS", tt.value)
			cfg := NewDefault()
			applyEnvOverrides(cfg)
			if cfg.Upstream.ParallelDownloads != tt.want {
				t.Fatalf("parallel downloads = %v", cfg.Upstream.ParallelDownloads)
			}
		})
	}
}
