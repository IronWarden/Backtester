package data

import (
	"strings"
	"testing"
)

// The reason has to explain the verdict it travels with, which means it must
// name the WORST defect and not merely the first one it finds. A series with
// negative closes AND a long flat run is unusable because of the prices; being
// told about the flat run would send the reader after the wrong thing.
func TestQualityReasonNamesTheWorstDefect(t *testing.T) {
	cases := []struct {
		name  string
		stats QualityStats
		want  string
		// The verdict the reason has to be consistent with.
		verdict Quality
	}{
		{
			name:    "negative closes outrank everything else",
			stats:   QualityStats{Bars: 5000, NonPositiveCloses: 12, LongestFlatRun: 90, BigJumps: 9},
			want:    "priced at or below zero",
			verdict: QualityUnusable,
		},
		{
			name:    "too short to score",
			stats:   QualityStats{Bars: 4},
			want:    "too few",
			verdict: QualityUnusable,
		},
		{
			name:    "an impossible-bar share names the feed",
			stats:   QualityStats{Bars: 1000, ImpossibleBars: 50},
			want:    "points at the feed",
			verdict: QualityUnusable,
		},
		{
			name:    "a 500% day is a data error",
			stats:   QualityStats{Bars: 1000, MaxJump: 8},
			want:    "data error rather than a price",
			verdict: QualityUnusable,
		},
		{
			name:    "a stalled feed",
			stats:   QualityStats{Bars: 5000, LongestFlatRun: 120},
			want:    "consecutive identical closes",
			verdict: QualitySuspect,
		},
		{
			name:    "missing days",
			stats:   QualityStats{Bars: 900, MissingDays: 100, ExpectedDays: 1000},
			want:    "market days missing",
			verdict: QualitySuspect,
		},
		{
			name:    "unexplained jumps",
			stats:   QualityStats{Bars: 5000, BigJumps: 9, MaxJump: 1.2},
			want:    "single-day moves over",
			verdict: QualitySuspect,
		},
		{
			name:    "duplicates",
			stats:   QualityStats{Bars: 5000, DuplicateBars: 3},
			want:    "duplicate bars",
			verdict: QualitySuspect,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := QualityReason(tc.stats)
			if !strings.Contains(got, tc.want) {
				t.Errorf("QualityReason = %q, want it to mention %q", got, tc.want)
			}
			// The two are read together, so they must agree about severity.
			if v := ClassifyQuality(tc.stats); v != tc.verdict {
				t.Errorf("ClassifyQuality = %q but the reason is written for %q: %q",
					v, tc.verdict, got)
			}
		})
	}
}

// A clean series has nothing to say, and saying something anyway is how a
// warning channel becomes noise nobody reads.
func TestQualityReasonIsEmptyForACleanSeries(t *testing.T) {
	clean := QualityStats{Bars: 5000, ExpectedDays: 5000, MaxJump: 0.08}
	if got := QualityReason(clean); got != "" {
		t.Errorf("a clean series reported %q", got)
	}
	if v := ClassifyQuality(clean); v != QualityGood {
		t.Errorf("the clean fixture is not actually clean: %q", v)
	}
}
