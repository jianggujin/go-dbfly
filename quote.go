package dbfly

import (
	"strings"
)

type Quoter struct {
	prefix     byte
	suffix     byte
	isReserved func(string) bool
}

var (
	// AlwaysNoReserve always think it's not a reverse word
	AlwaysNoReserve = func(string) bool { return false }

	// AlwaysReserve always reverse the word
	AlwaysReserve = func(string) bool { return true }

	// CommanQuoteMark represnets the common quote mark
	CommanQuoteMark byte = '`'

	CommonQuoter = &Quoter{CommanQuoteMark, CommanQuoteMark, AlwaysReserve}
)

func NewQuoter(prefix, suffix byte, isReserved func(string) bool) *Quoter {
	return &Quoter{prefix, suffix, isReserved}
}

func (q *Quoter) IsEmpty() bool {
	return q.prefix == 0 && q.suffix == 0
}

func (q *Quoter) Quote(s string) (string, error) {
	var buf strings.Builder
	if err := q.QuoteTo(&buf, s); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func (q *Quoter) Trim(s string) string {
	if len(s) < 2 {
		return s
	}

	var buf strings.Builder
	for i := 0; i < len(s); i++ {
		switch {
		case i == 0 && s[i] == q.prefix:
		case i == len(s)-1 && s[i] == q.suffix:
		case s[i] == q.suffix && s[i+1] == '.':
		case s[i] == q.prefix && s[i-1] == '.':
		default:
			buf.WriteByte(s[i])
		}
	}
	return buf.String()
}

func (q *Quoter) Join(a []string, sep string) (string, error) {
	var b strings.Builder
	if err := q.JoinWrite(&b, a, sep); err != nil {
		return "", err
	}
	return b.String(), nil
}

func (q *Quoter) JoinWrite(b *strings.Builder, a []string, sep string) error {
	if len(a) == 0 {
		return nil
	}

	n := len(sep) * (len(a) - 1)
	for i := 0; i < len(a); i++ {
		n += len(a[i])
	}

	b.Grow(n)
	for i, s := range a {
		if i > 0 {
			if _, err := b.WriteString(sep); err != nil {
				return err
			}
		}
		if err := q.QuoteTo(b, strings.TrimSpace(s)); err != nil {
			return err
		}
	}
	return nil
}

func findWord(v string, start int) int {
	for j := start; j < len(v); j++ {
		switch v[j] {
		case '.', ' ':
			return j
		}
	}
	return len(v)
}

func findStart(value string, start int) int {
	if value[start] == '.' {
		return start + 1
	}
	if value[start] != ' ' {
		return start
	}

	k := -1
	for j := start; j < len(value); j++ {
		if value[j] != ' ' {
			k = j
			break
		}
	}
	if k == -1 {
		return len(value)
	}

	if k+1 < len(value) &&
		(value[k] == 'A' || value[k] == 'a') &&
		(value[k+1] == 'S' || value[k+1] == 's') {
		k += 2
	}

	for j := k; j < len(value); j++ {
		if value[j] != ' ' {
			return j
		}
	}
	return len(value)
}

func (q *Quoter) quoteWordTo(buf *strings.Builder, word string) error {
	realWord := word
	if (word[0] == CommanQuoteMark && word[len(word)-1] == CommanQuoteMark) ||
		(word[0] == q.prefix && word[len(word)-1] == q.suffix) {
		realWord = word[1 : len(word)-1]
	}

	if q.IsEmpty() {
		_, err := buf.WriteString(realWord)
		return err
	}

	isReserved := q.isReserved(realWord)
	if isReserved && realWord != "*" {
		if err := buf.WriteByte(q.prefix); err != nil {
			return err
		}
	}
	if _, err := buf.WriteString(realWord); err != nil {
		return err
	}
	if isReserved && realWord != "*" {
		return buf.WriteByte(q.suffix)
	}

	return nil
}

func (q *Quoter) QuoteTo(buf *strings.Builder, value string) error {
	var i int
	for i < len(value) {
		start := findStart(value, i)
		if start > i {
			if _, err := buf.WriteString(value[i:start]); err != nil {
				return err
			}
		}
		if start == len(value) {
			return nil
		}

		nextEnd := findWord(value, start)
		if err := q.quoteWordTo(buf, value[start:nextEnd]); err != nil {
			return err
		}
		i = nextEnd
	}
	return nil
}

func (q *Quoter) Strings(s []string) ([]string, error) {
	res := make([]string, 0, len(s))
	for _, a := range s {
		r, err := q.Quote(a)
		if err != nil {
			return nil, err
		}
		res = append(res, r)
	}
	return res, nil
}

type QuoterOption func(*Quoter)

func WithPrefix(prefix byte) QuoterOption {
	return func(quoter *Quoter) {
		quoter.prefix = prefix
	}
}

func WithSuffix(suffix byte) QuoterOption {
	return func(quoter *Quoter) {
		quoter.suffix = suffix
	}
}

func WithIsReserved(isReserved func(string) bool) QuoterOption {
	return func(quoter *Quoter) {
		quoter.isReserved = isReserved
	}
}

func (q *Quoter) Clone(opts ...QuoterOption) *Quoter {
	nq := NewQuoter(q.prefix, q.suffix, q.isReserved)
	for _, opt := range opts {
		opt(nq)
	}
	return nq
}

func ReplaceRemarks(remarks string) string {
	return strings.ReplaceAll(remarks, "'", "''")
}
