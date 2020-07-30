package sqlh

import (
	"database/sql"
	"reflect"
	"testing"
)

func TestInsert(t *testing.T) {

	type e struct {
		A int `sql:"a"`
	}

	type row struct {
		e
		B string `sql:"b"`
		y string `sql:"y/insert/update/select"` // Should be ignored
		Z string `sql:"-"`                      // Should be ignored
	}

	schema := `create table X(a int, b string)`

	rows := []row{
		{e{1}, "testing", "ignored", ""},
		{e{2}, "testing", "ignored", ""},
		{e{3}, "testing", "ignored", ""},
	}

	// Build up expected statement and values for insert
	statement := `insert into X(a, b) values`
	values := make([]interface{}, 0)
	sep := ""
	for i := range rows {
		values = append(values, rows[i].A, rows[i].B)
		statement += sep + "($1, $2)"
		sep = ", "
	}

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}

	t.Run("insert scalar", func(t *testing.T) {
		x, err := insert("X", rows[0])
		if err != nil {
			t.Fatal(err)
		}
		statement := `insert into X(a, b) values($1, $2)`
		if statement != x.statement {
			t.Fatalf("expected: %#v, got: %#v", statement, x.statement)
		}
		if len(x.args) != 2 {
			t.Fatalf("expected 2 values, got: %d", len(x.args))
		}
		if !reflect.DeepEqual(values[0:2], x.args) {
			t.Fatalf("expected: %#v, got: %#v", values[0:2], x.args)
		}
	})

	t.Run("insert slice", func(t *testing.T) {
		x, err := insert("X", rows)
		if err != nil {
			t.Fatal(err)
		}
		if statement != x.statement {
			t.Fatalf("expected: %#v, got: %#v", statement, x.statement)
		}
		if len(x.args) != len(rows)*2 {
			t.Fatalf("expected %d values, got: %d", len(rows)*2, len(x.args))
		}
		if !reflect.DeepEqual(values, x.args) {
			t.Fatalf("expected: %#v, got: %#v", values, x.args)
		}
	})

	t.Run("view SQL statement and values before exec", func(t *testing.T) {
		x, err := insert("X", rows)
		if err != nil {
			t.Fatal(err)
		}
		if x.statement != statement {
			t.Fatalf("expected %#v, got: %#v", statement, x.statement)
		}
		if !reflect.DeepEqual(x.args, values) {
			t.Fatalf("expected %#v, got: %#v", values, x.args)
		}
	})

	t.Run("exec successful", func(t *testing.T) {
		if _, err := Insert(db, "X", rows); err != nil {
			t.Fatal(err)
		}
		r, err := db.Query(`select * from X`)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for r.Next() {
			n++
		}
		if n != len(rows) {
			t.Fatalf("expected %d rows, got %d", len(rows), n)
		}
	})

}

func TestInsertExplicitIgnore(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`create table T(id integer primary key autoincrement, a text)`); err != nil {
		t.Fatal(err)
	}
	x := struct {
		ID int64  `sql:"id/insert"`
		A  string `sql:"a"`
	}{999, "999"}
	if _, err := Insert(db, "T", x); err != nil {
		t.Fatal(err)
	}
	if err := Scan(&x, db, "select * from T"); err != nil {
		t.Fatal(err)
	}
	if x.ID != 1 {
		t.Fatalf("id should be 1, got %d", x.ID)
	}
}
