// Package hlc implements a Hybrid Logical Clock per Kulkarni et al.
// (https://cse.buffalo.edu/tech-reports/2014-04.pdf).
//
// An HLC gives a total order on events even when wall clocks are skewed.
// The clock advances on every local Tick() and on every Update() with a
// remote clock. The (region) tag is the third tiebreak so two regions
// generating events at the same physical_ms+logical still order
// deterministically.
package hlc

import (
	"sync"
	"time"
)

type Timestamp struct {
	PhysicalMs int64
	Logical    int32
	Region     string
}

func (a Timestamp) Less(b Timestamp) bool {
	switch {
	case a.PhysicalMs != b.PhysicalMs:
		return a.PhysicalMs < b.PhysicalMs
	case a.Logical != b.Logical:
		return a.Logical < b.Logical
	default:
		return a.Region < b.Region
	}
}

func (a Timestamp) Equal(b Timestamp) bool {
	return a.PhysicalMs == b.PhysicalMs && a.Logical == b.Logical && a.Region == b.Region
}

type Clock struct {
	mu     sync.Mutex
	region string
	now    func() int64
	state  state
}

type state struct {
	l int64
	c int32
}

// New returns a Clock tagged with region. now defaults to time.Now if nil.
func New(region string, now func() int64) *Clock {
	if now == nil {
		now = func() int64 { return time.Now().UnixMilli() }
	}
	return &Clock{region: region, now: now}
}

// Tick advances the clock for a locally-generated event and returns the
// resulting timestamp.
func (c *Clock) Tick() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	pt := c.now()
	if pt > c.state.l {
		c.state = state{l: pt, c: 0}
	} else {
		c.state.c++
	}
	return Timestamp{PhysicalMs: c.state.l, Logical: c.state.c, Region: c.region}
}

// Update advances the clock on receipt of a remote timestamp and returns
// the resulting local timestamp (used when re-emitting an event derived
// from a remote one).
func (c *Clock) Update(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	pt := c.now()
	lOld := c.state.l
	cOld := c.state.c

	lNew := lOld
	if remote.PhysicalMs > lNew {
		lNew = remote.PhysicalMs
	}
	if pt > lNew {
		lNew = pt
	}

	var cNew int32
	switch {
	case lNew == lOld && lNew == remote.PhysicalMs:
		if cOld > remote.Logical {
			cNew = cOld + 1
		} else {
			cNew = remote.Logical + 1
		}
	case lNew == lOld:
		cNew = cOld + 1
	case lNew == remote.PhysicalMs:
		cNew = remote.Logical + 1
	default:
		cNew = 0
	}

	c.state = state{l: lNew, c: cNew}
	return Timestamp{PhysicalMs: lNew, Logical: cNew, Region: c.region}
}

// Observe is like Update but does not return the local timestamp; useful
// when applying remote events without producing a derived event.
func (c *Clock) Observe(remote Timestamp) {
	_ = c.Update(remote)
}
