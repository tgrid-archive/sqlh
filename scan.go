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
	atleastOneRow := false

	// Execute the query in RowPasser
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

	// Get element (row) type
	t := v.Type()
	if v.Kind() == reflect.Slice {
		t = t.Elem()
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	for rows.Next() {
		// Choose target for this row. If the destination is a
		// slice, the target is a new value of the row
		// type. If the destination is a scalar, the target is
		// the desination itself.
		target := v
		if v.Kind() == reflect.Slice {
			target = reflect.New(t).Elem()
		}

		// Build an array of receiver pointers which are
		// passed to rows.Scan. If the destination row type is
		// a struct, receivers will contain pointers to
		// appropriate fields within the struct. If it is a
		// base type, receivers will contain a pointer to the
		// destination itself.
		receivers := make([]interface{}, len(columns))
		if t.Kind() == reflect.Struct {
			for i, col := range columns {
				field := target.FieldByNameFunc(func(name string) bool {
					if field, ok := t.FieldByName(name); ok {
						if field.Tag.Get("sql") == col {
							return true
						}
					}
					return strings.ToLower(name) == col
				})
				if !field.IsValid() {
					return fmt.Errorf("no field for column %s", col)
				}
				receivers[i] = field.Addr().Interface()
			}
		} else if len(columns) != 1 {
			return fmt.Errorf("can't scan %d columns into %s", len(columns), t)
		} else {
			receivers[0] = target.Addr().Interface()
		}

		// Scan into the target
		if err := rows.Scan(receivers...); err != nil {
			return err
		}

		// If destination was a scalar, we only need the first row
		if v.Kind() != reflect.Slice {
			atleastOneRow = true
			break
		}

		// If destination was a slice, append the current row
		v.Set(reflect.Append(v, target))
	}

	if err := rows.Err(); err != nil {
		return err
	}

	// If destination was scalar, ensure we got atleast one row
	if v.Kind() != reflect.Slice && !atleastOneRow {
		return sql.ErrNoRows
	}

	return nil
}
