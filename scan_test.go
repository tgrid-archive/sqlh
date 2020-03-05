package sqlh

import (
	"database/sql"
	"reflect"
	"regexp"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

const schema = `create table A(a text, b int, c text)`
const data = `
insert into A(a, b, c) values
('one', 1, 'red'),
('two', 2, 'red'),
('three', 3, 'blue')`

type A struct {
	A string
	B int
	Z string `sql:"c"`
}

var expect = []A{
	A{"one", 1, "red"},
	A{"two", 2, "red"},
	A{"three", 3, "blue"},
}

func TestScan(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:?_fk=1")
	Panic(err)
	defer db.Close()
	_, err = db.Exec(schema)
	Panic(err)
	_, err = db.Exec(data)
	Panic(err)

	t.Run("scan into slice of base type", func(t *testing.T) {
		dest := make([]string, 0)
		if err := Scan(&dest, R(db.Query(`select a from A`))); err != nil {
			t.Fatalf("scan into base slice: %s", err)
		}
		for i := range expect {
			if dest[i] != expect[i].A {
				t.Fatalf("row %d %s != %s", i, dest[i], expect[i].A)
			}
		}
	})

	t.Run("scan into scalar base type", func(t *testing.T) {
		var dest string
		err := Scan(&dest, R(db.Query(`select a from A limit 1`)))
		if err != nil {
			t.Fatalf("scanning into scalar base type: %s", err)
		}
		if dest != expect[0].A {
			t.Fatalf("%s != %s", dest, expect[0].A)
		}
	})

	t.Run("multiple columns into scalar should fail", func(t *testing.T) {
		var dest string
		test := regexp.MustCompile(`can't scan [0-9]+ columns into.*`)
		err := Scan(&dest, R(db.Query(`select * from A limit 1`)))
		if !test.MatchString(err.Error()) {
			t.Fatalf("expected match for %v, got: %s", test, err)
		}
	})

	t.Run("dest is scalar struct", func(t *testing.T) {
		var dest A
		err := Scan(&dest, R(db.Query(`select * from A limit 1`)))
		if err != nil {
			t.Fatalf("scan into scalar struct: %s", err)
		}
		if !reflect.DeepEqual(expect[0], dest) {
			t.Fatalf("expected: %#v\ngot: %#v", expect[0], dest)
		}
	})

	t.Run("dest is slice of struct", func(t *testing.T) {
		var dest []A
		if err := Scan(&dest, R(db.Query(`select * from A`))); err != nil {
			t.Fatalf("scan into slice of struct: %s", err)
		}
		if !reflect.DeepEqual(expect, dest) {
			t.Fatalf("expected: %#v\ngot: %#v", expect, dest)
		}
	})

	t.Run("unmatched columns cause error", func(t *testing.T) {
		var dest struct {
			A string
		}
		test := regexp.MustCompile(`no suitable field for column`)
		err := Scan(&dest, R(db.Query(`select * from A`)))
		if !test.MatchString(err.Error()) {
			t.Fatalf("expected match for %v, got: %s", test, err)
		}
	})
}
