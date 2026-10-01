package store

// Match reports whether s matches a Redis glob pattern. * matches any run of
// characters, ? matches exactly one, [abc], [a-z] and [^a] match one
// character from a set, and \ escapes the next character.
//
// Go's path.Match looks similar but treats '/' specially, while Redis keys
// commonly contain '/', so it can't be used here.
func Match(pattern, s string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		case '*':
			for len(pattern) > 1 && pattern[1] == '*' {
				pattern = pattern[1:]
			}
			if len(pattern) == 1 {
				return true // a trailing * matches whatever is left
			}
			// Try matching the rest of the pattern at every possible position.
			for i := 0; i <= len(s); i++ {
				if Match(pattern[1:], s[i:]) {
					return true
				}
			}
			return false
		case '?':
			if len(s) == 0 {
				return false
			}
		case '[':
			if len(s) == 0 {
				return false
			}
			rest, ok := matchClass(pattern[1:], s[0])
			if !ok {
				return false
			}
			pattern, s = rest, s[1:]
			continue
		case '\\':
			if len(pattern) > 1 {
				pattern = pattern[1:]
			}
			fallthrough
		default:
			if len(s) == 0 || s[0] != pattern[0] {
				return false
			}
		}
		pattern, s = pattern[1:], s[1:]
	}
	return len(s) == 0
}

// matchClass matches c against a [...] set. p starts just after the '['. It
// returns the pattern after the closing ']' and whether c was in the set.
func matchClass(p string, c byte) (rest string, ok bool) {
	negate := len(p) > 0 && p[0] == '^'
	if negate {
		p = p[1:]
	}
	matched := false
	for i := 0; i < len(p); i++ {
		switch {
		case p[i] == ']':
			return p[i+1:], matched != negate
		case p[i] == '\\' && i+1 < len(p):
			i++
			matched = matched || p[i] == c
		case i+2 < len(p) && p[i+1] == '-' && p[i+2] != ']':
			lo, hi := p[i], p[i+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			matched = matched || (lo <= c && c <= hi)
			i += 2
		default:
			matched = matched || p[i] == c
		}
	}
	return "", false // no closing ']'
}
