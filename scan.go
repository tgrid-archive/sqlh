package sqlh

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// RowPasser represents the encapsulated results of an sql.Query. (See
// R and Scan).
type RowPasser func() (*sql.Rows, error)

// R encapsulates the results of an sql.Query in a RowPasser, such
// that are passed inline to Scan. E.g.,
//
//   Scan(&dest, R(db.Query(`select "testing"`))
func R(rows *sql.Rows, err error) RowPasser {
	return func() (*sql.Rows, error) {
		return rows, err
	}
}

// Scan is a short-hand for scanning a set of rows into a slice,
// or a single row into a scalar. Example:
//
//   var dest struct{A, B string}
//   _ = Scan(&dest, R(db.Query(`select a, b from C`)))
//   var dest2 struct{A, B string}
//   _ = Scan(&dest2, R(db.Query(`select a, b from C limit 1`)))
//   var dest3 []int
//   _ = Scan(&dest3, R(db.Query(`select a from C`)))
//   var dest4 int
//   _ = Scan(&dest4, R(db.Query(`select a from C limit 1`)))
//
// Scan will match columns to struct fields based on the lower-cased
// name, or an `sql:""` struct tag (which takes precedence).
//
// If only a single column is returned by the query, the destination
// can be a base type (e.g., a string).
func Scan(dest interface{}, pass RowPasser) error {
	rows, err := pass()
	if err != nil {
		return err
	}
	defer rows.Close()

	// Ensure dest is a pointer
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("dest is not a pointer type")
	}
	v = v.Elem()
	t := v.Type()
	if v.Kind() == reflect.Slice {
		t = t.Elem()
	}
	// v is the writable destination slice or struct
	// t is the destination element type

	scan := scanStruct
	if t.Kind() != reflect.Struct {
		scan = scanBase
	}
	result, err := scan(v, t, rows)
	if err != nil {
		return err
	}
	v.Set(result)
	return nil
}

func scanBase(v reflect.Value, t reflect.Type, rows *sql.Rows) (reflect.Value, error) {
	var null reflect.Value
	if cols, err := rows.Columns(); err != nil {
		return null, err
	} else if len(cols) != 1 {
		return null, fmt.Errorf("can't scan %d columns into %s", len(cols), t)
	}
	result := reflect.MakeSlice(reflect.SliceOf(t), 0, 0)
	for rows.Next() {
		elem := reflect.New(t).Elem()
		if err := rows.Scan(elem.Addr().Interface()); err != nil {
			return null, err
		}
		result = reflect.Append(result, elem)
		if v.Kind() != reflect.Slice {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return null, err
	}
	if v.Kind() != reflect.Slice {
		if result.Len() != 1 {
			return null, sql.ErrNoRows
		}
		return result.Index(0), nil
	}
	return result, nil
}

func scanStruct(v reflect.Value, t reflect.Type, rows *sql.Rows) (reflect.Value, error) {
	var null reflect.Value
	// Create name -> field number map
	fields := make(map[string]int)
	for i := 0; i < t.NumField(); i++ {
		name, ok := t.Field(i).Tag.Lookup("sql")
		if !ok {
			name = strings.ToLower(t.Field(i).Name)
		}
		fields[name] = i
	}

	// Build receiver index array
	cols, err := rows.Columns()
	if err != nil {
		return null, fmt.Errorf("error getting column names: %s", err)
	}
	receivers := make([]int, len(cols))
	for i, col := range cols {
		r, ok := fields[col]
		if !ok {
			return null, fmt.Errorf("no suitable field for column %s", col)
		}
		receivers[i] = r
	}

	result := reflect.MakeSlice(reflect.SliceOf(t), 0, 0)
	for rows.Next() {
		elem := reflect.New(t).Elem()
		r := make([]interface{}, len(receivers))
		for i := range r {
			r[i] = elem.Field(receivers[i]).Addr().Interface()
		}
		if err := rows.Scan(r...); err != nil {
			return null, err
		}

		result = reflect.Append(result, elem)

		if v.Kind() != reflect.Slice {
			break // We only need 1 row if dest is not a slice
		}
	}
	if err := rows.Err(); err != nil {
		return null, err
	}
	if v.Kind() == reflect.Struct {
		if result.Len() != 1 {
			return null, sql.ErrNoRows
		}
		return result.Index(0), nil
	}
	return result, nil
}
