package zset

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"testing"
)

func names(ms []Member) string {
	s := ""
	for _, m := range ms {
		s += fmt.Sprintf("%s:%g ", m.Name, m.Score)
	}
	return s
}

func TestAddAndRange(t *testing.T) {
	z := New()
	z.Add("carol", 30)
	z.Add("alice", 10)
	z.Add("bob", 20)
	z.Add("dave", 20) // ties are ordered by member name
	if got := names(z.All()); got != "alice:10 bob:20 dave:20 carol:30 " {
		t.Fatalf("got %q", got)
	}
	if z.Add("alice", 40) {
		t.Fatal("updating a score should not count as an add")
	}
	if got := names(z.Range(0, 1, true)); got != "alice:40 carol:30 " {
		t.Fatalf("reverse range: got %q", got)
	}
	if got := names(z.Range(-2, -1, false)); got != "carol:30 alice:40 " {
		t.Fatalf("negative indexes: got %q", got)
	}
	if r, _ := z.Rank("carol"); r != 2 {
		t.Fatalf("rank = %d, want 2", r)
	}
}

func TestRangeByScoreAndCount(t *testing.T) {
	z := New()
	for i := 1; i <= 10; i++ {
		z.Add(fmt.Sprint("m", i), float64(i))
	}
	got := z.RangeByScore(Bound{Value: 3}, Bound{Value: 6, Exclusive: true}, 0, -1)
	if names(got) != "m3:3 m4:4 m5:5 " {
		t.Fatalf("got %q", names(got))
	}
	got = z.RangeByScore(Bound{Value: 1}, Bound{Value: 10}, 2, 3)
	if names(got) != "m3:3 m4:4 m5:5 " {
		t.Fatalf("with LIMIT: got %q", names(got))
	}
	if n := z.Count(Bound{Value: 2, Exclusive: true}, Bound{Value: 9}); n != 7 {
		t.Fatalf("count = %d, want 7", n)
	}
	if n := z.Count(Bound{Value: 50}, Bound{Value: 60}); n != 0 {
		t.Fatalf("count outside range = %d", n)
	}
}

// TestMatchesSortedSlice does thousands of random operations against both
// the skip list and a plain sorted slice, and checks after every step that
// they agree on order, ranks and spans.
func TestMatchesSortedSlice(t *testing.T) {
	z := New()
	model := map[string]float64{}
	for step := range 5000 {
		member := fmt.Sprint("m", rand.IntN(300))
		if rand.IntN(3) == 0 {
			z.Remove(member)
			delete(model, member)
		} else {
			score := float64(rand.IntN(50)) // small range, so lots of ties
			z.Add(member, score)
			model[member] = score
		}

		want := make([]Member, 0, len(model))
		for m, s := range model {
			want = append(want, Member{m, s})
		}
		sort.Slice(want, func(i, j int) bool {
			return want[i].Score < want[j].Score || (want[i].Score == want[j].Score && want[i].Name < want[j].Name)
		})
		if !slices.Equal(z.All(), want) {
			t.Fatalf("step %d: order differs from the model", step)
		}
		if step%100 == 0 {
			for i, m := range want {
				if r, _ := z.Rank(m.Name); r != i {
					t.Fatalf("step %d: rank of %s = %d, want %d", step, m.Name, r, i)
				}
			}
			if len(want) > 0 && !slices.Equal(z.Range(0, -1, true), reversed(want)) {
				t.Fatalf("step %d: reverse walk differs", step)
			}
		}
	}
}

func reversed(ms []Member) []Member {
	out := slices.Clone(ms)
	slices.Reverse(out)
	return out
}

func BenchmarkAdd(b *testing.B) {
	z := New()
	for i := range b.N {
		z.Add(fmt.Sprint(i), rand.Float64())
	}
}

func BenchmarkRank(b *testing.B) {
	z := New()
	for i := range 100_000 {
		z.Add(fmt.Sprint(i), float64(i))
	}
	b.ResetTimer()
	for i := range b.N {
		z.Rank(fmt.Sprint(i % 100_000))
	}
}
