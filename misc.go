package sqlh

import (
	"strings"
)

func repeat(s, sep string, n int) string {
	if n < 0 {
		panic("n < 0")
	}
	v := ""
	ssep := ""
	for i := 0; i < n; i++ {
		v += ssep + s
		ssep = sep
	}
	return v
}

// parseTag returns the column name and whether the field should be ignored
// based on the context. Context being a string like insert, select, or update.
func parseTag(tag string, context string) (name string, ignore bool) {
	ss := strings.Split(tag, "/")
	if len(ss) == 1 {
		return ss[0], ss[0] == "-"
	}
	context = strings.ToLower(context)
	for _, v := range ss[1:] {
		if strings.ToLower(v) == context {
			return ss[0], true
		}
	}
	return ss[0], false
}
