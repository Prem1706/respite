// Package zset implements Redis sorted sets: unique members, each with a
// score, kept in score order.
//
// Two structures hold the same data, as in Redis. A map from member to
// score answers "what is this member's score?" in O(1). A skip list keeps
// the members sorted, for range queries and ranks in O(log n). Every update
// changes both.
//
// A ZSet is not safe for concurrent use. The store guards it with its lock.
package zset

import (
	"math"
	"strconv"
)

type ZSet struct {
	scores map[string]float64
	sl     *skipList
}

type Member struct {
	Name  string
	Score float64
}

func New() *ZSet {
	return &ZSet{scores: make(map[string]float64), sl: newSkipList()}
}

func (z *ZSet) Len() int { return len(z.scores) }

func (z *ZSet) Score(member string) (float64, bool) {
	s, ok := z.scores[member]
	return s, ok
}

// Add sets member's score and reports whether member is new. Changing an
// existing member's score moves it: delete, then re-insert in its new place.
func (z *ZSet) Add(member string, score float64) (added bool) {
	old, exists := z.scores[member]
	if exists {
		if old == score {
			return false
		}
		z.sl.delete(old, member)
	}
	z.sl.insert(score, member)
	z.scores[member] = score
	return !exists
}

func (z *ZSet) Remove(member string) bool {
	score, ok := z.scores[member]
	if !ok {
		return false
	}
	z.sl.delete(score, member)
	delete(z.scores, member)
	return true
}

// Rank returns member's 0-based position in ascending score order.
func (z *ZSet) Rank(member string) (int, bool) {
	score, ok := z.scores[member]
	if !ok {
		return 0, false
	}
	return z.sl.rank(score, member) - 1, true
}

// Range returns the members with ranks start to stop inclusive, in
// ascending order or, if reverse, descending. Negative indexes count from
// the end, as in Redis: -1 is the last member.
func (z *ZSet) Range(start, stop int, reverse bool) []Member {
	n := z.Len()
	if start < 0 {
		start = max(n+start, 0)
	}
	if stop < 0 {
		stop = n + stop
	}
	stop = min(stop, n-1)
	if start > stop {
		return nil
	}

	out := make([]Member, 0, stop-start+1)
	if reverse {
		x := z.sl.byRank(n - start) // rank counted from the end
		for i := start; i <= stop; i++ {
			out = append(out, Member{x.member, x.score})
			x = x.backward
		}
		return out
	}
	x := z.sl.byRank(start + 1)
	for i := start; i <= stop; i++ {
		out = append(out, Member{x.member, x.score})
		x = x.level[0].forward
	}
	return out
}

// Bound is one end of a score range. Exclusive bounds are written "(1.5" in
// Redis commands.
type Bound struct {
	Value     float64
	Exclusive bool
}

func (b Bound) allowsAsMin(score float64) bool {
	return score > b.Value || (!b.Exclusive && score == b.Value)
}

func (b Bound) allowsAsMax(score float64) bool {
	return score < b.Value || (!b.Exclusive && score == b.Value)
}

// RangeByScore returns members with scores between lo and hi, skipping
// the first offset matches and returning at most count (count < 0 means no
// limit).
func (z *ZSet) RangeByScore(lo, hi Bound, offset, count int) []Member {
	var out []Member
	for x := z.sl.firstFrom(lo); x != nil && hi.allowsAsMax(x.score) && count != 0; x = x.level[0].forward {
		if offset > 0 {
			offset--
			continue
		}
		out = append(out, Member{x.member, x.score})
		count--
	}
	return out
}

// Count returns how many members have scores between lo and hi. Both ends
// are found in O(log n) and their ranks subtracted, so no members are walked.
func (z *ZSet) Count(lo, hi Bound) int {
	first, last := z.sl.firstFrom(lo), z.sl.lastUpTo(hi)
	if first == nil || last == nil {
		return 0
	}
	return max(z.sl.rank(last.score, last.member)-z.sl.rank(first.score, first.member)+1, 0)
}

// All returns every member in ascending order.
func (z *ZSet) All() []Member { return z.Range(0, -1, false) }

// FormatScore prints the shortest form that parses back to exactly the same
// float, so a score survives the AOF and replication without rounding.
func FormatScore(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
