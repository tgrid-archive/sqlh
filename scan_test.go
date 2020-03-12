package sqlh

import (
	"database/sql"
	"reflect"
	"regexp"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

const schema = `
create table A(a text, b int, c text);

create table B(id text, gr text);

insert into A(a, b, c) values
('one', 1, 'red'),
('two', 2, 'red'),
('three', 3, 'blue');

insert into B(id, gr) values
('one', 'group1'),
('two', 'group1'),
('two', 'group2');
`

type a struct {
	A string
	B int
	Z string `sql:"c"`
}

var expect = []a{
	a{"one", 1, "red"},
	a{"two", 2, "red"},
	a{"three", 3, "blue"},
}

func TestScan(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:?_fk=1")
	_panic(err)
	defer db.Close()
	for i, v := range strings.Split(schema, ";") {
		if _, err := db.Exec(v); err != nil {
			t.Fatalf("exec schema %d: %s:\n%s", i, err, v)
		}
	}

	t.Run("scan into slice of base type", func(t *testing.T) {
		dest := make([]string, 0)

		if err := Scan(&dest, `select a from A`).Query(db); err != nil {
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
		err := Scan(&dest, `select a from A limit 1`).Query(db)
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
		err := Scan(&dest, `select * from A limit 1`).Query(db)
		if !test.MatchString(err.Error()) {
			t.Fatalf("expected match for %v, got: %s", test, err)
		}
	})

	t.Run("dest is scalar struct", func(t *testing.T) {
		var dest a
		err := Scan(&dest, `select * from A limit 1`).Query(db)
		if err != nil {
			t.Fatalf("scan into scalar struct: %s", err)
		}
		if !reflect.DeepEqual(expect[0], dest) {
			t.Fatalf("expected: %#v\ngot: %#v", expect[0], dest)
		}
	})

	t.Run("dest is slice of struct", func(t *testing.T) {
		var dest []a
		if err := Scan(&dest, `select * from A`).Query(db); err != nil {
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
		test := regexp.MustCompile(`^no field for column b$`)
		err := Scan(&dest, `select * from A`).Query(db)
		if !test.MatchString(err.Error()) {
			t.Fatalf("expected match for %v, got: %s", test, err)
		}
	})

	t.Run("existing unmatched fields left alone", func(t *testing.T) {
		expect := expect[0]
		expect.A = "untouched"
		initial := a{A: "untouched"}
		if err := Scan(&initial, `select B, C from A limit 1`).Query(db); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(expect, initial) {
			t.Fatalf("expected: %#v\ngot: %#v", expect, initial)
		}
	})

	t.Run("embedded struct fields can be targeted", func(*testing.T) {
		type embed struct {
			A string
		}
		var dest struct {
			embed
			B int
		}
		if err := Scan(&dest, `select a, b from A limit 1`).Query(db); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("aggregate into slice fields", func(t *testing.T) {
		type d struct {
			A     string
			Group []string `sql:"gr"`
		}
		expect := []d{
			d{"one", []string{"group1"}},
			d{"two", []string{"group1", "group2"}},
			d{"three", nil},
		}
		var dest []d
		if err := Scan(&dest, `select a, gr from A left join B on a = id`).Query(db); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(expect, dest) {
			t.Fatalf("expected: %#v\ngot: %#v", expect, dest)
		}
	})
}

func BenchmarkScan(b *testing.B) {
	db, err := sql.Open("sqlite3", ":memory:?_fk=1")
	_panic(err)
	defer db.Close()
	for i, v := range strings.Split(schema, ";") {
		if _, err = db.Exec(v); err != nil {
			b.Fatalf("exec schema %d: %s", i, err)
		}
	}
	_, err = db.Exec(`delete from A`)
	_panic(err)
	for i := 0; i < b.N; i++ {
		_, err := db.Exec(`insert into A(a,b,c) values(?,?,?)`, "testing", 1, "testing")
		_panic(err)
	}

	b.ResetTimer()

	var dest []a
	err = Scan(&dest, `select * from A`).Query(db)
	_panic(err)
	if len(dest) != b.N {
		panic("incorrect number of rows returned")
	}
}
