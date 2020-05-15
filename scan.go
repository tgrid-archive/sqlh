package sqlh

import (
	"database/sql"
	"fmt"
	"reflect"
)

//go:generate sh -c "./readme.awk <README > sqlh.gen.go"

// Querist is the minimal set of function needed from an *sql.DB.
type Querist interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

// Scan is a short-hand for scanning a set of rows into a slice,
// or a single row into a scalar. Example:
//
//   var dest struct{A, B string}
//   _ = Scan(&dest, db, `select a, b from C`)
//   var dest2 struct{A, B string}
//   _ = Scan(&dest2, db, `select a, b from C limit 1`)
//   var dest3 []int
//   _ = Scan(&dest3, db, `select a from C`)
//   var dest4 int
//   _ = Scan(&dest4, db, `select a from C limit 1`)
//
// If only a single column is returned by the query, the destination
// can be a base type (e.g., a string).
//
// If some fields in the destination struct are slices; then the
// results will be grouped by unique tuples of all non-slice fields,
// the slice fields will contain an aggregate of values from the
// corresponsing column.
//
//   var dest struct{A string, B []string}
//   _ = Scan(&dest, db, `select a, b from C`)
//   // => [{"red", ["one", "two"]}, {"blue", ["three", "four", "five"]}]
func Scan(dest interface{}, db Querist, query string, args ...interface{}) error {
	atleastOneRow := false

	rows, err := db.Query(query, args...)
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
		aggregates := make([]string, 0) // Fields which are slices to aggregate into
		aggrVals := make([]reflect.Value, 0)
		keys := make([]string, 0) // Fields to use as grouping key
		if t.Kind() == reflect.Struct {
			for i, col := range columns {
				structField, ok := t.FieldByNameFunc(func(s string) bool {
					if field, ok := t.FieldByName(s); ok {
						tag, ok := field.Tag.Lookup("sql")
						if !ok {
							return false
						}
						name, ignore := parseTag(tag, "select")
						if name == col {
							return !ignore
						}
						return false
					}
					return false
				})
				if !ok {
					return fmt.Errorf("no field for column %s", col)
				}
				// If the field is a slice; scan into
				// a temporary value of the element
				// type, for later aggregation.
				field := target.FieldByName(structField.Name)
				if field.Kind() == reflect.Slice {
					field = reflect.New(reflect.PtrTo(field.Type().Elem()))
					aggregates = append(aggregates, structField.Name)
					aggrVals = append(aggrVals, field)
				} else {
					field = field.Addr()
					keys = append(keys, structField.Name)
				}
				receivers[i] = field.Interface()
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

		// Try to find an existing row in the result set, to
		// which we can aggregate the current row.
		if v.Kind() == reflect.Slice {
			aggregated := false
		rows:
			for i := 0; i < v.Len() && len(aggregates) > 0; i++ {
				// Check that all key fields match current row
				for _, name := range keys {
					x := v.Index(i).FieldByName(name).Interface()
					y := target.FieldByName(name).Interface()
					if !reflect.DeepEqual(x, y) {
						continue rows // Keys don't match on this row, so skip
					}
				}
				// Keys have all matched, so add to
				// this row instead of appending a new
				// row
				for j, name := range aggregates {
					existing := v.Index(i).FieldByName(name)
					// If result wasn't NULL; add to aggregate
					new := aggrVals[j].Elem()
					if !new.IsNil() {
						existing.Set(reflect.Append(existing, new.Elem()))
					}
				}
				aggregated = true
			}
			// If we couldn't aggregate current row with
			// an existing, append current row to result
			// set.
			if !aggregated {
				for i, name := range aggregates {
					field := target.FieldByName(name)
					new := aggrVals[i].Elem()
					if !new.IsNil() {
						field.Set(reflect.Append(field, new.Elem()))
					}
				}
				v.Set(reflect.Append(v, target))
			}
		} else {
			// If destination was a scalar, we only need the first row
			atleastOneRow = true
			break
		}
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
