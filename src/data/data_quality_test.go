package data

import "testing"

// good is a series with nothing wrong with it, so each case below can change
// exactly one thing and the verdict change is attributable.
func goodStats() QualityStats {
	return QualityStats{
		Ticker:       "TEST",
		Bars:         2000,
		ExpectedDays: 2000,
	}
}

func TestClassifyQualityLadder(t *testing.T) {
	cases := []struct {
		name string
		edit func(*QualityStats)
		want Quality
	}{
		{"nothing wrong", func(*QualityStats) {}, QualityGood},

		// Unusable: the arithmetic is broken or there is nothing to score.
		{"too few bars", func(s *QualityStats) { s.Bars = MinBars - 1 }, QualityUnusable},
		{"exactly the minimum bars", func(s *QualityStats) {
			s.Bars, s.ExpectedDays = MinBars, MinBars
		}, QualityGood},
		// Literals: 20 bars is the documented floor for scoring a series.
		{"19 bars cannot be scored", func(s *QualityStats) {
			s.Bars, s.ExpectedDays = 19, 19
		}, QualityUnusable},
		{"20 bars can", func(s *QualityStats) {
			s.Bars, s.ExpectedDays = 20, 20
		}, QualityGood},
		{"one negative close", func(s *QualityStats) { s.NonPositiveCloses = 1 },
			QualityUnusable},
		{"impossible bars past the share", func(s *QualityStats) {
			s.ImpossibleBars = 21 // > 1% of 2000
		}, QualityUnusable},
		{"impossible bars at the share", func(s *QualityStats) {
			s.ImpossibleBars = 20 // exactly 1%, so not yet unusable
		}, QualitySuspect},
		{"a 500% single-day move", func(s *QualityStats) { s.MaxJump = 5.01 },
			QualityUnusable},

		// Suspect: one finding big enough to move a metric on its own.
		{"a single impossible bar", func(s *QualityStats) { s.ImpossibleBars = 1 },
			QualitySuspect},
		{"flat run at the suspect edge", func(s *QualityStats) {
			s.LongestFlatRun = FlatRunSuspect
		}, QualitySuspect},
		// Literal, not FlatRunSuspect: a case written in terms of the constant
		// moves with the constant, so it cannot catch the threshold being
		// redefined. These two pin the documented boundary — a quarter of a
		// trading year of unchanged closes is a stalled feed — in numbers.
		{"60 identical closes is suspect", func(s *QualityStats) {
			s.LongestFlatRun = 60
		}, QualitySuspect},
		{"59 identical closes is only usable", func(s *QualityStats) {
			s.LongestFlatRun = 59
		}, QualityUsable},
		{"missing past the suspect share", func(s *QualityStats) {
			s.MissingDays = 101 // > 5% of 2000
		}, QualitySuspect},
		{"five big jumps", func(s *QualityStats) { s.BigJumps = BigJumpsSuspect },
			QualitySuspect},
		// Literals, for the same reason as the flat-run pair above.
		{"5 jumps over 50% is suspect", func(s *QualityStats) { s.BigJumps = 5 },
			QualitySuspect},
		{"4 jumps over 50% is only usable", func(s *QualityStats) { s.BigJumps = 4 },
			QualityUsable},
		{"a duplicate row", func(s *QualityStats) { s.DuplicateBars = 1 },
			QualitySuspect},

		// Usable: the ordinary roughness of free daily data.
		{"flat run at the usable edge", func(s *QualityStats) {
			s.LongestFlatRun = FlatRunUsable
		}, QualityUsable},
		{"flat run one below the usable edge", func(s *QualityStats) {
			s.LongestFlatRun = FlatRunUsable - 1
		}, QualityGood},
		// Literals again, pinning the other boundary: four weeks of unchanged
		// closes is worth reporting, three is ordinary illiquidity.
		{"20 identical closes is usable", func(s *QualityStats) {
			s.LongestFlatRun = 20
		}, QualityUsable},
		{"19 identical closes is good", func(s *QualityStats) {
			s.LongestFlatRun = 19
		}, QualityGood},
		{"missing past the usable share", func(s *QualityStats) {
			s.MissingDays = 21 // > 1% of 2000
		}, QualityUsable},
		{"one big jump", func(s *QualityStats) { s.BigJumps = 1 }, QualityUsable},
		{"four big jumps", func(s *QualityStats) { s.BigJumps = BigJumpsSuspect - 1 },
			QualityUsable},
		{"mostly zero volume", func(s *QualityStats) { s.ZeroVolumeBars = 1001 },
			QualityUsable},
		{"half zero volume", func(s *QualityStats) {
			s.ZeroVolumeBars = 1000 // exactly the share, which is not "over" it
		}, QualityGood},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := goodStats()
			tc.edit(&s)
			if got := ClassifyQuality(s); got != tc.want {
				t.Errorf("verdict %q, want %q (stats %+v)", got, tc.want, s)
			}
		})
	}
}

// The worst finding decides, not the last one checked. A series with both a
// negative close and a mild flat run is unusable, not usable.
func TestClassifyQualityTakesTheWorstFinding(t *testing.T) {
	s := goodStats()
	s.LongestFlatRun = FlatRunUsable
	s.BigJumps = 1
	s.NonPositiveCloses = 1
	if got := ClassifyQuality(s); got != QualityUnusable {
		t.Errorf("verdict %q, want %q — a lesser finding is masking a fatal one",
			got, QualityUnusable)
	}
}

// The four series below are real findings from the 2026-08-17 audit, with their
// measured figures. They are the reason the thresholds are where they are, and
// if a change ever lets one of them read as good, the audit has stopped working.
func TestClassifyQualityCatchesRealDefects(t *testing.T) {
	real := []struct {
		stats QualityStats
		want  Quality
		what  string
	}{
		{QualityStats{Ticker: "VHI", Bars: 11624, NonPositiveCloses: 5429,
			ImpossibleBars: 4223, ExpectedDays: 11624},
			QualityUnusable, "closes down to -639.30"},
		{QualityStats{Ticker: "HUBB", Bars: 13000, LongestFlatRun: 1862,
			ZeroVolumeBars: 3000, ExpectedDays: 13000},
			QualitySuspect, "1,862 identical closes, 1977-1984"},
		{QualityStats{Ticker: "EMP", Bars: 2286, BigJumps: 526, MaxJump: 102.98,
			MissingDays: 1749, ExpectedDays: 4035},
			QualityUnusable, "526 moves over 50%, largest +10,297%"},
		// The default benchmark, exactly as the audit measures it: no missing
		// days, no jumps, a three-day flat run — and zero volume throughout,
		// because it is a reconstructed index series rather than a traded
		// instrument. Zero volume is the ONLY finding against it, which is why
		// it lands on usable rather than good, and that is expected rather
		// than a defect.
		{QualityStats{Ticker: "$SP500", Bars: 24742, LongestFlatRun: 3,
			ZeroVolumeBars: 24742, ExpectedDays: 24742},
			QualityUsable, "reconstructed index: no volume, nothing else wrong"},
	}

	for _, tc := range real {
		t.Run(tc.stats.Ticker, func(t *testing.T) {
			if got := ClassifyQuality(tc.stats); got != tc.want {
				t.Errorf("%s (%s): verdict %q, want %q",
					tc.stats.Ticker, tc.what, got, tc.want)
			}
		})
	}
}

func TestQualityTradable(t *testing.T) {
	for _, q := range []Quality{QualityGood, QualityUsable, QualitySuspect,
		QualityUnknown} {
		if !q.Tradable() {
			t.Errorf("%q should be tradable", q)
		}
	}
	if QualityUnusable.Tradable() {
		t.Error("an unusable series must not be tradable")
	}
}

func TestUntradableTickers(t *testing.T) {
	q := map[string]TickerQuality{
		"ZZZ": {Verdict: QualityUnusable},
		"AAA": {Verdict: QualityUnusable},
		"MMM": {Verdict: QualitySuspect},
		"GGG": {Verdict: QualityGood},
	}
	got := UntradableTickers(q)
	if len(got) != 2 || got[0] != "AAA" || got[1] != "ZZZ" {
		t.Errorf("UntradableTickers = %v, want [AAA ZZZ] sorted", got)
	}
}

// A zero denominator must not produce a NaN, which compares false against every
// threshold and would silently pass a series with no calendar at all.
func TestShareHandlesZeroTotal(t *testing.T) {
	if got := share(5, 0); got != 0 {
		t.Errorf("share(5, 0) = %v, want 0", got)
	}
	if got := share(1, 4); got != 0.25 {
		t.Errorf("share(1, 4) = %v, want 0.25", got)
	}
}
