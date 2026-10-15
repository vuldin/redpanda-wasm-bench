package main

import (
	"testing"
	"time"

	"github.com/vuldin/redpanda-wasm-bench/wire"
)

func pr(waves []float64, per int) placeReport {
	p := placeReport{RunID: 9, Waves: waves, PerWave: per}
	i := 0
	for _, w := range waves {
		for k := 0; k < per; k++ {
			p.Placed = append(p.Placed, placed{ID: sentinelID(9, i), Wave: w, AckMs: 1})
			i++
		}
	}
	return p
}

func fill(aggr, rest string) wire.Fill {
	return wire.Fill{AggressorID: aggr, RestingID: rest, Price: sentinelPrice, Qty: 1}
}

func TestJudgeAllRecovered(t *testing.T) {
	p := pr([]float64{10, 1}, 2)
	var fs []wire.Fill
	var at []time.Duration
	for i, s := range p.Placed {
		fs = append(fs, fill("9y-b"+string(rune('0'+i))+"-1", s.ID))
		at = append(at, time.Duration(i+1)*time.Millisecond)
	}
	v := judge(p, "9z-b0-1", fs, at)
	if !v.OK || v.Recovered != 4 || v.Lost != 0 || v.FirstFillMs != 1 || v.LastFillMs != 4 {
		t.Fatalf("%+v", v)
	}
}

func TestJudgeLossIsAttributedToItsWave(t *testing.T) {
	p := pr([]float64{10, 0.3}, 2)
	// only the 10s wave filled
	fs := []wire.Fill{fill("a", p.Placed[0].ID), fill("b", p.Placed[1].ID)}
	v := judge(p, "sweep", fs, []time.Duration{1, 2})
	if v.OK || v.Lost != 2 || v.Waves[0].Recovered != 2 || v.Waves[1].Lost != 2 {
		t.Fatalf("%+v", v)
	}
}

func TestJudgeUnackedSentinelsAreNotCountedAsLost(t *testing.T) {
	p := pr([]float64{1}, 2)
	p.Placed[1].Err = "timeout"
	v := judge(p, "sweep", []wire.Fill{fill("a", p.Placed[0].ID)}, []time.Duration{1})
	if !v.OK || v.Placed != 1 {
		t.Fatalf("%+v", v)
	}
}

func TestJudgeStraysAndDuplicates(t *testing.T) {
	p := pr([]float64{1}, 1)
	fs := []wire.Fill{
		fill("sweep", "7t-s5-0"), // an orphan sell swept below 50,000
		fill("a", p.Placed[0].ID),
		fill("b", p.Placed[0].ID), // the same sentinel filled twice
		fill("c", "not-a-sentinel"),
	}
	v := judge(p, "sweep", fs, []time.Duration{1, 2, 3, 4})
	if v.StraysSwept != 1 || v.Duplicated != 1 || v.UnexpectedFill != 1 || v.OK {
		t.Fatalf("%+v", v)
	}
}
