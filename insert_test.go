package sqlh

import (
	"database/sql"
	"reflect"
	"testing"
)

func TestInsert(t *testing.T) {

	type e struct {
		A int
	}

	type row struct {
		e
		B string
		y string // Should be ignored
		Z string `sql:"-"` // Should be ignored
	}

	schema := `create table X(a int, b string)`

	rows := []row{
		row{e{1}, "testing", "ignored", ""},
		row{e{2}, "testing", "ignored", ""},
		row{e{3}, "testing", "ignored", ""},
	}

	// Build up expected statement and values for insert
	columns := []string{"a", "b"}
	statement := `insert into X(a, b) values`
	values := make([]interface{}, 0)
	sep := ""
	for i := range rows {
		values = append(values, rows[i].A, rows[i].B)
		statement += sep + "(?, ?)"
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
		x := Insert("X", rows[0])
		if x.err != nil {
			t.Fatal(x.err)
		}
		if !reflect.DeepEqual(columns, x.columns) {
			t.Fatalf("expected: %#v, got: %#v", columns, x.columns)
		}
		if len(x.args) != 2 {
			t.Fatalf("expected 2 values, got: %d", len(x.args))
		}
		if !reflect.DeepEqual(values[0:2], x.args) {
			t.Fatalf("expected: %#v, got: %#v", values[0:2], x.args)
		}
	})

	t.Run("insert slice", func(t *testing.T) {
		x := Insert("X", rows)
		if x.err != nil {
			t.Fatal(x.err)
		}
		if !reflect.DeepEqual(columns, x.columns) {
			t.Fatalf("expected: %#v, got: %#v", columns, x.columns)
		}
		if len(x.args) != len(rows)*2 {
			t.Fatalf("expected %d values, got: %d", len(rows)*2, len(x.args))
		}
		if !reflect.DeepEqual(values, x.args) {
			t.Fatalf("expected: %#v, got: %#v", values, x.args)
		}
	})

	t.Run("view SQL statement and values before exec", func(t *testing.T) {
		x := Insert("X", rows)
		if x.err != nil {
			t.Fatal(x.err)
		}
		if x.Statement() != statement {
			t.Fatalf("expected %#v, got: %#v", statement, x.Statement())
		}
		if !reflect.DeepEqual(x.Args(), values) {
			t.Fatalf("expected %#v, got: %#v", values, x.Args())
		}
	})

	t.Run("exec successful", func(t *testing.T) {
		if _, err := Insert("X", rows).Exec(db); err != nil {
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
