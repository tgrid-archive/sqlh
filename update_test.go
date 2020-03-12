package sqlh

import (
	"database/sql"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestUpdate(t *testing.T) {

	type e struct {
		A string
		B *string
	}

	type update struct {
		e
		C int
		D *int
		E bool
		F *bool
		y string // Should be ignored
		Z string `sql:"-"` // Should be ignored
	}

	type test struct {
		name string
		u    update
		set  string
		vals []interface{}
	}

	const schema = `
create table Z(a text, b text, c int, d int, e bool, f bool);
insert into Z values(null, null, null, null, null, null),('test', 'test', 1, 1, true, true);
`
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range strings.Split(schema, ";") {
		if _, err := db.Exec(v); err != nil {
			t.Fatalf("schema %d: %s", i+1, err)
		}
	}

	t.Run("update with only zero values fails", func(t *testing.T) {
		update := Update("Z").Set(update{}).Where("rowid = 1")
		if len(update.set) != 0 {
			t.Fatalf("expected 0 columns to set, got: %#v", update.set)
		}
		match := regexp.MustCompile(`^no fields to update$`)
		_, err := update.Exec(db)
		if !match.MatchString(err.Error()) {
			t.Fatalf("expected error matching %v, got: %s", match, err)
		}
	})

	t.Run(`sql:"-" tag ignored`, func(t *testing.T) {
		u := Update("Z").Set(update{Z: "test"})
		if len(u.args) != 0 {
			t.Fatalf("expected empty set, got: %#v", u.args)
		}
	})

	t.Run("unexported field ignored", func(t *testing.T) {
		u := Update("Z").Set(update{y: "test"})
		if len(u.args) != 0 {
			t.Fatalf("expected empty set, got: %#v", u.args)
		}
	})

	sp := "test string pointer"
	ip := 999
	bp := false
	tests := []test{
		test{"update w/ string", update{e: e{A: "x"}}, "a = ?", []interface{}{"x"}},
		test{"update w/ *string", update{e: e{B: &sp}}, "b = ?", []interface{}{&sp}},
		test{"update w/ int", update{C: 1}, "c = ?", []interface{}{1}},
		test{"update w/ *int", update{D: &ip}, "d = ?", []interface{}{&ip}},
		test{"update w/ bool", update{E: true}, "e = ?", []interface{}{true}},
		test{"update w/ *bool", update{F: &bp}, "f = ?", []interface{}{&bp}},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := Update("Z").Set(tt.u)
			if tt.set != u.set {
				t.Fatalf("test %d: expected %#v, got: %#v", i, tt.set, u.set)
			}
			if !reflect.DeepEqual(tt.vals, u.args) {
				t.Fatalf("test %d: expected %#v, got: %#v", i, tt.vals, u.args)
			}
			if res, err := u.Where("rowid = 1").Exec(db); err != nil {
				t.Fatalf("exec: %s", err)
			} else if n, err := res.RowsAffected(); err != nil {
				t.Fatalf("get row count: %s", err)
			} else if n != 1 {
				t.Fatalf("expected 1 row affected, got: %d", n)
			}
		})
	}
}
