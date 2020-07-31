package sqlh

import (
	"fmt"
	"testing"
)

func TestReindex(t *testing.T) {
	type T struct {
		in     string
		base   int
		expect string
	}
	tests := []T{
		{`where a = $1 and b = $2`, 10, `where a = $11 and b = $12`},
		{`$100`, 100, `$200`},
		{`"a$c" = $2`, 1, `"a$c" = $3`},
		{`"$2" $2`, 1, `"$2" $3`},
		{`"$1\"" $2`, 1, `"$1\"" $3`},
		{`'$1\''`, 1, `'$1\''`},
	}
	for i, v := range tests {
		t.Run(fmt.Sprintf("Case %d", i), func(t *testing.T) {
			result := reindex(v.in, v.base)
			if result == v.expect {
				return
			}
			t.Fatalf("expect %#v, got %#v", v.expect, result)
		})
	}
}
