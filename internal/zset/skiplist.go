package zset

import "math/rand/v2"

// A skip list is a sorted linked list with extra "express lane" links. Every
// node is on level 0; each node is also on the next level up with
// probability 1/4, and so on. A search starts on the highest level and drops
// down a level whenever the next step would overshoot, so it skips most of
// the list. Insert, delete and search are O(log n) on average, with no
// rebalancing (which is why Redis chose it over a balanced tree).
//
// Each link also stores its span: how many level-0 steps it jumps. Adding up
// spans along a search path gives an element's rank in O(log n), which is
// what ZRANK and ZRANGE by index need.
//
// Elements are ordered by score, then by member, so equal scores still have
// a well-defined order.

const (
	maxLevel = 32
	branch   = 4 // a node on level i is also on level i+1 with probability 1/branch
)

type node struct {
	member   string
	score    float64
	backward *node // previous node on level 0, for walking in reverse
	level    []link
}

type link struct {
	forward *node
	span    int
}

type skipList struct {
	head   *node // sentinel; holds no element
	tail   *node
	length int
	level  int // number of levels in use
}

func newSkipList() *skipList {
	return &skipList{head: &node{level: make([]link, maxLevel)}, level: 1}
}

// before reports whether n sorts strictly before an element with this
// score and member.
func (n *node) before(score float64, member string) bool {
	return n.score < score || (n.score == score && n.member < member)
}

func randomLevel() int {
	lvl := 1
	for lvl < maxLevel && rand.IntN(branch) == 0 {
		lvl++
	}
	return lvl
}

// insert adds an element. The caller makes sure member isn't already present.
func (sl *skipList) insert(score float64, member string) {
	// update[i] is the last node on level i before the new element, and
	// rank[i] is that node's rank. The new node is linked in after them.
	var update [maxLevel]*node
	var rank [maxLevel]int
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		if i < sl.level-1 {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil && x.level[i].forward.before(score, member) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	lvl := randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.head
			update[i].level[i].span = sl.length // the head's new link spans the whole list
		}
		sl.level = lvl
	}

	x = &node{member: member, score: score, level: make([]link, lvl)}
	for i := range lvl {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x
		// The old link from update[i] is split in two around x.
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}
	// Links on higher levels now jump over one more node.
	for i := lvl; i < sl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] != sl.head {
		x.backward = update[0]
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		sl.tail = x
	}
	sl.length++
}

// delete removes an element and reports whether it was there.
func (sl *skipList) delete(score float64, member string) bool {
	var update [maxLevel]*node
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.before(score, member) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x == nil || x.score != score || x.member != member {
		return false
	}

	for i := range sl.level {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span-- // the link jumped over x
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		sl.tail = x.backward
	}
	for sl.level > 1 && sl.head.level[sl.level-1].forward == nil {
		sl.level--
	}
	sl.length--
	return true
}

// rank returns the 1-based position of an element, or 0 if it isn't there.
func (sl *skipList) rank(score float64, member string) int {
	r := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for f := x.level[i].forward; f != nil && (f.before(score, member) || (f.score == score && f.member == member)); f = x.level[i].forward {
			r += x.level[i].span
			x = f
		}
		if x != sl.head && x.member == member {
			return r
		}
	}
	return 0
}

// byRank returns the element at a 1-based position, or nil.
func (sl *skipList) byRank(r int) *node {
	traversed := 0
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && traversed+x.level[i].span <= r {
			traversed += x.level[i].span
			x = x.level[i].forward
		}
		if traversed == r && x != sl.head {
			return x
		}
	}
	return nil
}

// firstFrom returns the first element whose score is within min, or nil.
func (sl *skipList) firstFrom(min Bound) *node {
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && !min.allowsAsMin(x.level[i].forward.score) {
			x = x.level[i].forward
		}
	}
	return x.level[0].forward
}

// lastUpTo returns the last element whose score is within max, or nil.
func (sl *skipList) lastUpTo(max Bound) *node {
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && max.allowsAsMax(x.level[i].forward.score) {
			x = x.level[i].forward
		}
	}
	if x == sl.head {
		return nil
	}
	return x
}
