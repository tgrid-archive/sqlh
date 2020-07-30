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
		A string  `sql:"a"`
		B *string `sql:"b"`
	}

	type up struct {
		e
		C int    `sql:"c"`
		D *int   `sql:"d"`
		E bool   `sql:"e"`
		F *bool  `sql:"f"`
		y string // Should be ignored
		Z string `sql:"-"` // Should be ignored
	}

	type test struct {
		name string
		u    up
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

	match := regexp.MustCompile(`^no fields to update$`)

	t.Run("update with only zero values fails", func(t *testing.T) {
		_, err := Update(db, "Z", up{}, "rowid = 1")
		if !match.MatchString(err.Error()) {
			t.Fatalf("expected error matching %v, got: %s", match, err)
		}
	})

	t.Run(`sql:"-" tag ignored`, func(t *testing.T) {
		_, err := update("Z", up{Z: "test"}, "skdjfsd")
		if !match.MatchString(err.Error()) {
			t.Fatalf("expected error matching %v, got: %s", match, err)
		}
	})

	t.Run("unexported field ignored", func(t *testing.T) {
		_, err := update("Z", up{y: "test"}, "fsfds")
		if !match.MatchString(err.Error()) {
			t.Fatalf("expected error matching %v, got: %s", match, err)
		}
	})

	sp := "test string pointer"
	ip := 999
	bp := false
	tests := []test{
		{"update w/ string", up{e: e{A: "x"}}, "a = $1", []interface{}{"x"}},
		{"update w/ *string", up{e: e{B: &sp}}, "b = $1", []interface{}{&sp}},
		{"update w/ int", up{C: 1}, "c = $1", []interface{}{1}},
		{"update w/ *int", up{D: &ip}, "d = $1", []interface{}{&ip}},
		{"update w/ bool", up{E: true}, "e = $1", []interface{}{true}},
		{"update w/ *bool", up{F: &bp}, "f = $1", []interface{}{&bp}},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := update("Z", tt.u, "rowid = 1")
			if err != nil {
				t.Fatal(err)
			}
			exp := "UPDATE Z SET " + tt.set + " WHERE rowid = 1"
			if exp != u.statement {
				t.Fatalf("test %d:\nexp %#v\ngot %#v", i, exp, u.statement)
			}
			if !reflect.DeepEqual(tt.vals, u.args) {
				t.Fatalf("test %d: expected %#v, got: %#v", i, tt.vals, u.args)
			}
			if res, err := db.Exec(u.statement, u.args...); err != nil {
				t.Fatalf("exec: %s", err)
			} else if n, err := res.RowsAffected(); err != nil {
				t.Fatalf("get row count: %s", err)
			} else if n != 1 {
				t.Fatalf("expected 1 row affected, got: %d", n)
			}
		})
	}
}

func TestUpdateExplicitIgnore(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`create table T(id integer primary key autoincrement, a text)`); err != nil {
		t.Fatal(err)
	}
	x := struct {
		ID int64  `sql:"id/update"`
		A  string `sql:"a"`
	}{999, "999"}
	if _, err := Insert(db, "T", x); err != nil {
		t.Fatal(err)
	}
	x.ID = 2
	if _, err := Update(db, "T", x, "id = 999"); err != nil {
		t.Fatal(err)
	}
	if err := Scan(&x, db, "select * from T"); err != nil {
		t.Fatal(err)
	}
	if x.ID != 999 {
		t.Fatalf("id should be 999, got %d", x.ID)
	}
}

func TestUpdateStatementWithWhere(t *testing.T) {
	type T struct {
		A int `sql:"a"`
	}
	u, err := update("T", T{2}, "a = $1", 1)
	if err != nil {
		t.Fatal(err)
	}
	exp := preUpdate{
		statement: `UPDATE T SET a = $2 WHERE a = $1`,
		args:      []interface{}{1, 2},
	}
	if !reflect.DeepEqual(exp, *u) {
		t.Fatalf("expected: %#v\ngot: %#v", exp, *u)
	}
}
