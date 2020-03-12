package sqlh

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// PensingInsert represents an INSERT query which is ready for
// execution via its' Exec method.
type PendingInsert struct {
	columns []string
	args    []interface{}
	table   string
	err     error
}

// Insert created a pending INSERT query given a table name and set of
// values to insert.
//
//   type val struct {
//     A int `sql:"id"`
//     B string
//   }
//   values := []val{val{1, "test"}, val{2, "test"}}
//
//   insert := Insert("X", values)
//   insert.Statement() // => `insert into X(id, b) values(?, ?), (?, ?)`
//   insert.Args() // => []interface{}{1, "test", 2, "test"}
//   insert.Exec(db) // Execute the INSERT statement
//
//   res, err := Insert("X", values).Exec(db) // Shorthand
func Insert(table string, values interface{}) PendingInsert {
	err := func(format string, args ...interface{}) PendingInsert {
		return PendingInsert{err: fmt.Errorf(format, args...)}
	}
	var vs []reflect.Value

	switch k := reflect.ValueOf(values).Kind(); k {
	case reflect.Struct:
		vs = append(vs, reflect.ValueOf(values))
	case reflect.Slice:
		v := reflect.ValueOf(values)
		for i := 0; i < v.Len(); i++ {
			w := v.Index(i)
			if w.Kind() != reflect.Struct {
				return err("values must be struct or []struct, not: %v", k)
			}
			vs = append(vs, w)
		}
	default:
		return err("values must be struct or []struct, not: %v", k)
	}

	if len(vs) < 1 {
		return err("no values given")
	}

	for _, v := range vs {
		if vs[0].Type() != v.Type() {
			return err("type mismatch: %v and %v", vs[0].Type(), v.Type())
		}
	}

	// Build columns and values arrays
	var columns []string
	var columnIdx [][]int

	// Build up set of columns we are using
	var recurseFields func(t reflect.Type, index []int)
	recurseFields = func(t reflect.Type, index []int) {
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if field.Anonymous {
				recurseFields(field.Type, append(index, field.Index...))
				continue
			}
			name := strings.ToLower(field.Name)
			if tag, ok := field.Tag.Lookup("sql"); ok {
				name = tag
			}
			// Ignore - tag, or unexported
			if name == "-" || field.PkgPath != "" {
				continue
			}
			columns = append(columns, name)
			columnIdx = append(columnIdx, append(index, field.Index...))
		}
	}
	recurseFields(vs[0].Type(), []int{})

	if len(columns) < 1 {
		return err("no columns available for insert")
	}

	// Build set of arguments for statement
	argset := make([]interface{}, len(columns)*len(vs))
	for i, v := range vs {
		for j := range columns {
			value := v.FieldByIndex(columnIdx[j])
			x := i * len(columns)
			argset[x+j] = value.Interface()
		}
	}

	return PendingInsert{
		columns: columns,
		args:    argset,
		table:   table,
	}
}

// Statement returns the SQL statement which will be exected when Exec
// is called on the pending INSERT query.
func (r PendingInsert) Statement() string {
	placeholders := repeat("?", ", ", len(r.columns))
	value := "(" + placeholders + ")"
	values := repeat(value, ", ", len(r.args)/len(r.columns))
	columns := strings.Join(r.columns, ", ")
	statement := fmt.Sprintf("insert into %s(%s) values%s", r.table, columns, values)
	return statement
}

// Args returns the array of arguments which will be passed when Exec
// is called on the pending INSERT query.
func (r PendingInsert) Args() []interface{} {
	return r.args
}

// Exec will execute the pending INSERT query against the given
// database or transaction.
//
//   res, err := Insert("X", values).Exec(db)
//
// It is shorthand for the following:
//
//   insert := Insert("X", values)
//   rows, err := db.Exec(insert.Statement(), insert.Args())
func (i PendingInsert) Exec(db Executor) (sql.Result, error) {
	if i.err != nil {
		return nil, i.err
	}
	return db.Exec(i.Statement(), i.Args()...)
}
