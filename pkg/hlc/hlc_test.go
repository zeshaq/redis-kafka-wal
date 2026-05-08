package hlc

import "testing"

func TestTickMonotonic(t *testing.T) {
	now := int64(100)
	c := New("us", func() int64 { return now })
	a := c.Tick()
	b := c.Tick()
	if !a.Less(b) {
		t.Fatalf("expected %v < %v", a, b)
	}
	now = 99
	d := c.Tick()
	if !b.Less(d) {
		t.Fatalf("clock must not regress: %v -> %v", b, d)
	}
}

func TestUpdateAdvancesPastRemote(t *testing.T) {
	now := int64(50)
	c := New("us", func() int64 { return now })
	remote := Timestamp{PhysicalMs: 200, Logical: 7, Region: "eu"}
	got := c.Update(remote)
	if got.PhysicalMs != 200 || got.Logical != 8 || got.Region != "us" {
		t.Fatalf("got %+v", got)
	}
}

func TestRegionTiebreak(t *testing.T) {
	a := Timestamp{PhysicalMs: 1, Logical: 0, Region: "ap"}
	b := Timestamp{PhysicalMs: 1, Logical: 0, Region: "us"}
	if !a.Less(b) {
		t.Fatalf("ap should sort before us at equal physical/logical")
	}
}
