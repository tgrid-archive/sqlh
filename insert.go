package sqlh

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// Insert runs an INSERT query given a db, table name, and set of
// values to insert.
//
//   type val struct {
//     A int `sql:"id"`
//     B string `sql:"b"`
//   }
//   values := []val{{1, "test"}, {2, "test"}}
//
//   res, err := Insert(db, "X", values)
//   // = db.Exec(`insert into X(id, b) values($1, $2), ($3, $4)`, 1, "test", 2, "test")
func Insert(db Executor, table string, values interface{}) (sql.Result, error) {
	i, err := insert(table, values)
	if err != nil {
		return nil, err
	}
	return db.Exec(i.statement, i.args...)
}

type preInsert struct {
	statement string
	args      []interface{}
}

func insert(table string, values interface{}) (*preInsert, error) {
	var vs []reflect.Value

	switch k := reflect.ValueOf(values).Kind(); k {
	case reflect.Struct:
		vs = append(vs, reflect.ValueOf(values))
	case reflect.Slice:
		v := reflect.ValueOf(values)
		for i := 0; i < v.Len(); i++ {
			w := v.Index(i)
			if w.Kind() != reflect.Struct {
				return nil, fmt.Errorf("values must be struct or []struct, not: %v", k)
			}
			vs = append(vs, w)
		}
	default:
		return nil, fmt.Errorf("values must be struct or []struct, not: %v", k)
	}

	if len(vs) < 1 {
		return nil, fmt.Errorf("no values given")
	}

	for _, v := range vs {
		if vs[0].Type() != v.Type() {
			return nil, fmt.Errorf("type mismatch: %v and %v", vs[0].Type(), v.Type())
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
			tag, ok := field.Tag.Lookup("sql")
			if !ok {
				continue // Ignore untagged
			}
			name, ignore := parseTag(tag, "insert")
			if ignore {
				continue // Explicitly ignored
			}
			columns = append(columns, name)
			columnIdx = append(columnIdx, append(index, field.Index...))
		}
	}
	recurseFields(vs[0].Type(), []int{})

	if len(columns) < 1 {
		return nil, fmt.Errorf("no columns available for insert")
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

	placeholders := repeatWithIndex("$", ", ", len(columns))
	value := "(" + placeholders + ")"
	valueList := repeat(value, ", ", len(argset)/len(columns))
	columnList := strings.Join(columns, ", ")
	statement := fmt.Sprintf("insert into %s(%s) values%s", table, columnList, valueList)

	return &preInsert{
		statement: statement,
		args:      argset,
	}, nil
}

func repeatWithIndex(prefix, separator string, n int) string {
	if n < 0 {
		panic("n < 0")
	}
	v := ""
	ssep := ""
	for i := 1; i <= n; i++ {
		v += ssep + fmt.Sprintf("%v%v", prefix, i)
		ssep = separator
	}
	return v
}
