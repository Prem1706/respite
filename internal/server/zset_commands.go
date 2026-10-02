package server

import (
	"math"
	"strconv"
	"strings"

	"github.com/Prem1706/respite/internal/zset"
)

// Sorted-set commands. Writes are logged by effect, like SET: ZINCRBY is
// logged as a ZADD of the new score, and ZADD only logs the members it
// actually changed.

const (
	errNotFloat  = "ERR value is not a valid float"
	errBadBound  = "ERR min or max is not a float"
	errNaNResult = "ERR resulting score is not a number (NaN)"
)

func parseScore(b []byte) (float64, bool) {
	f, err := strconv.ParseFloat(string(b), 64)
	return f, err == nil && !math.IsNaN(f)
}

func formatScore(f float64) string { return zset.FormatScore(f) }

// parseBound reads a score range bound: "5", "(5" (exclusive), "-inf", "+inf".
func parseBound(b []byte) (zset.Bound, bool) {
	var bound zset.Bound
	if len(b) > 0 && b[0] == '(' {
		bound.Exclusive = true
		b = b[1:]
	}
	f, ok := parseScore(b)
	bound.Value = f
	return bound, ok
}

// ZADD key [NX|XX] [CH] score member [score member ...]
func cmdZAdd(s *Server, c *client, args [][]byte) {
	var nx, xx, ch bool
	i := 2
flags: // flags come first; the first argument that isn't one starts the pairs
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "CH":
			ch = true
		default:
			break flags
		}
	}
	rest := args[i:]
	if len(rest) == 0 || len(rest)%2 != 0 || (nx && xx) {
		c.w.Error(errSyntax)
		return
	}
	// Parse every score before changing anything, so a bad score leaves the
	// set untouched.
	scores := make([]float64, len(rest)/2)
	for j := range scores {
		var ok bool
		if scores[j], ok = parseScore(rest[2*j]); !ok {
			c.w.Error(errNotFloat)
			return
		}
	}

	added, changed := 0, 0
	logged := [][]byte{[]byte("ZADD"), args[1]}
	err := s.store.UpdateZSet(string(args[1]), !xx, func(z *zset.ZSet) {
		for j, score := range scores {
			member := string(rest[2*j+1])
			old, exists := z.Score(member)
			if (nx && exists) || (xx && !exists) || (exists && old == score) {
				continue
			}
			if z.Add(member, score) {
				added++
			} else {
				changed++
			}
			logged = append(logged, []byte(formatScore(score)), rest[2*j+1])
		}
	})
	if err != nil {
		replyErr(c, err)
		return
	}
	if added+changed > 0 {
		s.propagate(c, logged...)
	}
	if ch {
		c.w.Integer(int64(added + changed))
	} else {
		c.w.Integer(int64(added))
	}
}

// ZINCRBY key increment member
func cmdZIncrBy(s *Server, c *client, args [][]byte) {
	incr, ok := parseScore(args[2])
	if !ok {
		c.w.Error(errNotFloat)
		return
	}
	var score float64
	nan := false
	err := s.store.UpdateZSet(string(args[1]), true, func(z *zset.ZSet) {
		old, _ := z.Score(string(args[3]))
		score = old + incr
		if math.IsNaN(score) { // inf + -inf
			nan = true
			return
		}
		z.Add(string(args[3]), score)
	})
	switch {
	case err != nil:
		replyErr(c, err)
	case nan:
		c.w.Error(errNaNResult)
	default:
		s.propagate(c, []byte("ZADD"), args[1], []byte(formatScore(score)), args[3])
		c.w.BulkString(formatScore(score))
	}
}

// ZREM key member [member ...]
func cmdZRem(s *Server, c *client, args [][]byte) {
	logged := [][]byte{[]byte("ZREM"), args[1]}
	err := s.store.UpdateZSet(string(args[1]), false, func(z *zset.ZSet) {
		for _, m := range args[2:] {
			if z.Remove(string(m)) {
				logged = append(logged, m)
			}
		}
	})
	if err != nil {
		replyErr(c, err)
		return
	}
	if removed := len(logged) - 2; removed > 0 {
		s.propagate(c, logged...)
		c.w.Integer(int64(removed))
	} else {
		c.w.Integer(0)
	}
}

func cmdZScore(s *Server, c *client, args [][]byte) {
	var score float64
	found := false
	err := s.store.ReadZSet(string(args[1]), func(z *zset.ZSet) {
		score, found = z.Score(string(args[2]))
	})
	switch {
	case err != nil:
		replyErr(c, err)
	case found:
		c.w.BulkString(formatScore(score))
	default:
		c.w.Null()
	}
}

func cmdZCard(s *Server, c *client, args [][]byte) {
	n := 0
	if err := s.store.ReadZSet(string(args[1]), func(z *zset.ZSet) { n = z.Len() }); err != nil {
		replyErr(c, err)
		return
	}
	c.w.Integer(int64(n))
}

// zrank builds ZRANK and ZREVRANK.
func zrank(reverse bool) func(*Server, *client, [][]byte) {
	return func(s *Server, c *client, args [][]byte) {
		rank, found := 0, false
		err := s.store.ReadZSet(string(args[1]), func(z *zset.ZSet) {
			if rank, found = z.Rank(string(args[2])); found && reverse {
				rank = z.Len() - 1 - rank
			}
		})
		switch {
		case err != nil:
			replyErr(c, err)
		case found:
			c.w.Integer(int64(rank))
		default:
			c.w.Null()
		}
	}
}

// zrange builds ZRANGE and ZREVRANGE: key start stop [WITHSCORES].
func zrange(reverse bool) func(*Server, *client, [][]byte) {
	return func(s *Server, c *client, args [][]byte) {
		start, err1 := strconv.Atoi(string(args[2]))
		stop, err2 := strconv.Atoi(string(args[3]))
		if err1 != nil || err2 != nil {
			c.w.Error(errNotInteger)
			return
		}
		withScores := false
		for _, a := range args[4:] {
			if !strings.EqualFold(string(a), "WITHSCORES") {
				c.w.Error(errSyntax)
				return
			}
			withScores = true
		}
		// Copy the results out while holding the lock and write the reply
		// afterwards. Writing can block on a slow client, and nothing slow
		// should happen while holding the store's lock.
		var members []zset.Member
		err := s.store.ReadZSet(string(args[1]), func(z *zset.ZSet) {
			members = z.Range(start, stop, reverse)
		})
		if err != nil {
			replyErr(c, err)
			return
		}
		writeMembers(c, members, withScores)
	}
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func cmdZRangeByScore(s *Server, c *client, args [][]byte) {
	lo, ok1 := parseBound(args[2])
	hi, ok2 := parseBound(args[3])
	if !ok1 || !ok2 {
		c.w.Error(errBadBound)
		return
	}
	withScores, offset, count := false, 0, -1
	for i := 4; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				c.w.Error(errSyntax)
				return
			}
			var err1, err2 error
			offset, err1 = strconv.Atoi(string(args[i+1]))
			count, err2 = strconv.Atoi(string(args[i+2]))
			if err1 != nil || err2 != nil {
				c.w.Error(errNotInteger)
				return
			}
			i += 2
		default:
			c.w.Error(errSyntax)
			return
		}
	}
	if offset < 0 {
		c.w.Array(0)
		return
	}
	var members []zset.Member
	err := s.store.ReadZSet(string(args[1]), func(z *zset.ZSet) {
		members = z.RangeByScore(lo, hi, offset, count)
	})
	if err != nil {
		replyErr(c, err)
		return
	}
	writeMembers(c, members, withScores)
}

func cmdZCount(s *Server, c *client, args [][]byte) {
	lo, ok1 := parseBound(args[2])
	hi, ok2 := parseBound(args[3])
	if !ok1 || !ok2 {
		c.w.Error(errBadBound)
		return
	}
	n := 0
	if err := s.store.ReadZSet(string(args[1]), func(z *zset.ZSet) { n = z.Count(lo, hi) }); err != nil {
		replyErr(c, err)
		return
	}
	c.w.Integer(int64(n))
}

func writeMembers(c *client, members []zset.Member, withScores bool) {
	if withScores {
		c.w.Array(2 * len(members))
	} else {
		c.w.Array(len(members))
	}
	for _, m := range members {
		c.w.BulkString(m.Name)
		if withScores {
			c.w.BulkString(formatScore(m.Score))
		}
	}
}
