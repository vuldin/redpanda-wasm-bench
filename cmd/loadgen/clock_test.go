package main

import (
	"strings"
	"testing"
)

// Every case below is a REAL measurement from run 1788316828 (Round 4,
// 2026-09-01), not a synthetic value. The point of the test is that the guard
// separates the one clock step from nine normal levels whose uncertainty is
// already the same order as the latency being measured - if it flagged those
// too it would void every level ever run and be useless.
func TestJudgeCrossClock(t *testing.T) {
	const step = 1000.0

	normal := []struct {
		name     string
		est, unc float64
	}{
		{"spread f5", -19.0, 388.7},
		{"spread f8", 19.5, 407.6},
		{"spread f9", 29.2, 421.4},
		{"spread f10", 41.3, 443.8},
		{"pinned f5", 43.3, 587.9},
		{"pinned f8", 61.8, 587.0},
		{"pinned f9", 54.3, 629.8},
		{"pinned f10", 57.1, 609.2},
		{"pinned f12", 81.0, 637.5},
	}
	for _, c := range normal {
		usable, verdict := judgeCrossClock(c.est, c.unc, step)
		if !usable {
			t.Errorf("%s: offset %.1fus +/-%.1fus was flagged unusable, but this is "+
				"ordinary chrony-grade sync; flagging it would void every level. verdict=%q",
				c.name, c.est, c.unc, verdict)
		}
	}

	// The step that produced a 25,456us total_p50 while the single-clock stages
	// at the same level were the best in the arm.
	if usable, verdict := judgeCrossClock(12493.5, 13003.4, step); usable {
		t.Errorf("the 12,493us clock step was NOT flagged; this is the exact case "+
			"that nearly produced a false 37x regression. verdict=%q", verdict)
	}

	// Negative offsets are normal (the sign depends on which way the clocks
	// drifted) and must be judged on magnitude.
	if usable, _ := judgeCrossClock(-12493.5, 13003.4, step); usable {
		t.Error("a negative clock step of the same magnitude was not flagged; " +
			"the check must use magnitude, not the signed value")
	}

	// A large uncertainty with a small offset is still unreadable.
	if usable, _ := judgeCrossClock(10, 5000, step); usable {
		t.Error("offset small but uncertainty 5000us (> 2x step) should be unusable")
	}

	// Escape hatch, for deliberately accepting cross-clock noise.
	if usable, _ := judgeCrossClock(999999, 999999, 0); !usable {
		t.Error("-skew-step-micros=0 must disable step detection entirely")
	}
}

// A saturated run shows a huge apparent clock offset because the skew estimator
// is fed the same backlog-contaminated timestamps as everything else. Measured
// at 6,173,562us and 4,107,356us of "offset" on two saturated fanout-12 trials
// (2026-09-01) whose actual problem was a 375,596-record backlog. The report
// must blame backlog, not the clock - a confident wrong diagnosis is worse than
// a vague right one.
func TestSaturationOutranksClockStepInVerdict(t *testing.T) {
	mk := func(lagGrew bool, skew float64) *report {
		r := &report{}
		r.ReceivedReceipts = 1
		r.ExpectedReceipts = 1
		r.Lag.GrewDuringRun = lagGrew
		r.Lag.FirstLag, r.Lag.FinalLag, r.Lag.MaxLag = 21716, 375596, 375596
		r.Lag.BacklogThreshold = 5000
		r.Clock.SkewEstimateMicros = skew
		r.Clock.CrossClockUnusable = true
		r.Clock.CrossClockVerdict = "UNUSABLE: clock offset ..."
		return r
	}

	_, reasons := judge(mk(true, 6173561.9))
	var joined string
	for _, r := range reasons {
		joined += r + "\n"
	}
	if !strings.Contains(joined, "SATURATION IS THE PRIMARY CAUSE") {
		t.Errorf("saturated run must attribute the unreadable total to backlog, not the clock; got:\n%s", joined)
	}

	_, reasons2 := judge(mk(false, 12493.5))
	var joined2 string
	for _, r := range reasons2 {
		joined2 += r + "\n"
	}
	if strings.Contains(joined2, "SATURATION IS THE PRIMARY CAUSE") {
		t.Errorf("an unsaturated run with a real clock step must NOT blame saturation; got:\n%s", joined2)
	}
	if !strings.Contains(joined2, "cross-clock stages unusable") {
		t.Errorf("an unsaturated clock step must still be reported; got:\n%s", joined2)
	}
}

// TestJudgeNegativeMatch pins the distinction that a negative `match` has two
// possible causes, using the real 2026-09-02 numbers for both sides of it.
func TestJudgeNegativeMatch(t *testing.T) {
	// Run 1788398875, fanout 500. chrony had every host synced to 2-9us
	// against the same AWS reference, so a 227us negative match cannot be
	// clock error - it is the in-broker pre-commit read.
	real := stageStats{Samples: 2999, MinMicros: -227.4, P50Micros: -61.6}

	mag, note := judgeNegativeMatch(real, 6.7)
	if note == "" {
		t.Fatal("a negative match with tight clock sync should still be reported")
	}
	if mag != 227.4 {
		t.Errorf("magnitude=%v want 227.4", mag)
	}
	if !strings.Contains(note, "PRE-COMMIT READ") {
		t.Errorf("with 6.7us sync the note should identify the pre-commit read, got: %s", note)
	}
	if !strings.Contains(note, "`total` is unaffected") {
		t.Errorf("the note must say total stays valid, got: %s", note)
	}

	// Same samples, but hosts genuinely badly synced: now the clocks are a
	// sufficient explanation and total should not be quoted.
	_, note = judgeNegativeMatch(real, 200)
	if strings.Contains(note, "PRE-COMMIT READ") {
		t.Errorf("with 200us sync the pre-commit read is not established, got: %s", note)
	}
	if !strings.Contains(note, "should not be quoted") {
		t.Errorf("with comparable offset the note should warn off match/total, got: %s", note)
	}

	// Unknown offset: must say it cannot tell, not pick one.
	_, note = judgeNegativeMatch(real, 0)
	if !strings.Contains(note, "cannot be distinguished") {
		t.Errorf("with no offset supplied the note must admit ambiguity, got: %s", note)
	}

	// A healthy all-positive match is silent.
	if _, note = judgeNegativeMatch(stageStats{Samples: 3000, MinMicros: 12, P50Micros: 40}, 6.7); note != "" {
		t.Errorf("positive match should be silent, got: %s", note)
	}
	if _, note = judgeNegativeMatch(stageStats{Samples: 0}, 6.7); note != "" {
		t.Errorf("empty match should be silent, got: %s", note)
	}
}

// TestNegativeMatchDoesNotSuppressTotal is the regression for the false
// positive: an earlier version marked the cross-clock stages unusable on any
// negative sample, which suppressed a valid `total` on well-synced hosts.
func TestNegativeMatchDoesNotSuppressTotal(t *testing.T) {
	rep := &report{
		Match: stageStats{Samples: 2999, MinMicros: -227.4, P50Micros: -61.6},
	}
	usable, verdict := judgeCrossClock(188.7625, 627.546, 1000)
	rep.Clock.CrossClockUnusable, rep.Clock.CrossClockVerdict = !usable, verdict
	if mag, note := judgeNegativeMatch(rep.Match, 6.7); note != "" {
		rep.Clock.NegativeMatchMagnitudeMicros = mag
		rep.Clock.NegativeMatchNote = note
	}
	if rep.Clock.CrossClockUnusable {
		t.Error("a negative match on well-synced hosts must not mark cross-clock unusable")
	}
	if rep.Clock.NegativeMatchNote == "" {
		t.Error("...but it must still be reported")
	}
}

// TestPacingFidelityGate pins the gap that every other gate missed on the
// 2026-09-03 traditional-deployment leg: the right NUMBER of orders sent, at
// the wrong TIMES, with all existing checks green.
func TestPacingFidelityGate(t *testing.T) {
	base := func() *report {
		r := &report{ReceivedReceipts: 29997, ExpectedReceipts: 29997}
		r.Config.Pacing = "fixed"
		r.Config.OfferedRate = 1000 // 1000us interval
		r.Rates.AttemptedRatio = 0.9999
		r.Rates.AchievedRatio = 0.9999
		r.Resources.ScrapedBeforeAndAfter = true
		return r
	}

	// The real contaminated run.
	bad := base()
	bad.SendLateness = stageStats{Samples: 29997, P50Micros: 1.7, P90Micros: 533.8, P99Micros: 2486.1}
	ok, reasons := judge(bad)
	if ok {
		t.Fatal("a run 2.5 pacing slots late at the tail must not be clean")
	}
	var found bool
	for _, r := range reasons {
		if strings.Contains(r, "pacing not held") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected a pacing reason, got %v", reasons)
	}

	// The healthy in-broker reference run's lateness must stay clean.
	good := base()
	good.SendLateness = stageStats{Samples: 30000, P50Micros: 0.3, P90Micros: 2.1, P99Micros: 4.3}
	if ok, reasons := judge(good); !ok {
		t.Errorf("healthy pacing (p99 4.3us vs 1000us interval) must stay clean, got %v", reasons)
	}

	// Burst pacing has no per-order schedule, so the gate must not fire.
	burst := base()
	burst.Config.Pacing = "burst"
	burst.SendLateness = stageStats{Samples: 29997, P99Micros: 2486.1}
	for _, r := range mustReasons(judge(burst)) {
		if strings.Contains(r, "pacing not held") {
			t.Error("pacing gate must not apply to burst pacing")
		}
	}
}

func mustReasons(_ bool, reasons []string) []string { return reasons }
