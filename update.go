package sqlh

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// Executor is an interface with just *sql.DB.Exec behaviour.
type Executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

// UpdatePendingSet is an SQL UPDATE query which is waiting for its'
// Set() method to be called, supplying a struct containing fields to
// update.
type UpdatePendingSet struct {
	db    Executor
	table string
}

// UpdatePendingWhere is an SQL UPDATE query which is waiting for a
// WHERE condition to be supplied via its' Where() method.
type UpdatePendingWhere struct {
	UpdatePendingSet
	set    string
	values []interface{}
	err    error
}

// Update is used to begin constructing a SQL UPDATE query. It takes a
// database and a table name. Method calls on the result allow the
// query to be finalized and executed.
//
//   type row struct{
//       Id int
//       Name string
//   }
//   res, err := Update(db, "X").Set(row{Name: "updated"}).Where("Id = ?", 1)
//
// Zero-values in the struct passed to Set() are ignored.
func Update(db Executor, table string) UpdatePendingSet {
	return UpdatePendingSet{db, table}
}

// Set allows a struct containing updated field/column values to be
// added to an UPDATE query. Fields in the struct which have their
// zero-value will be ignored.
//
// Be careful to use types reflecting valid values for the underlying
// row. E.g., if "" is a valid value for a column of TEXT type, then
// it would be better for a field to use *string than string.
func (u UpdatePendingSet) Set(update interface{}) UpdatePendingWhere {
	err := func(format string, args ...interface{}) UpdatePendingWhere {
		return UpdatePendingWhere{err: fmt.Errorf(format, args...)}
	}
	v := reflect.ValueOf(update)
	if v.Kind() != reflect.Struct {
		return err("update was not a struct: %v", v.Type())
	}
	var set []string
	var vals []interface{}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.IsZero() {
			continue
		}
		name, ok := v.Type().Field(i).Tag.Lookup("sql")
		if !ok {
			name = strings.ToLower(v.Type().Field(i).Name)
		}
		if name == "-" {
			continue
		}
		set = append(set, name+" = ?")
		vals = append(vals, field.Interface())
	}
	if len(set) < 1 {
		return err("no fields to update")
	}

	return UpdatePendingWhere{
		UpdatePendingSet: u,
		set:              strings.Join(set, ", "),
		values:           vals,
	}
}

// Where allows a WHERE condition to be added to a pending UPDATE
// query. It also executes the query and returns the results.
func (u UpdatePendingWhere) Where(where string, args ...interface{}) (sql.Result, error) {
	if u.err != nil {
		return nil, u.err
	}
	args = append(u.values, args...)
	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", u.table, u.set, where)

	return u.db.Exec(query, args...)
}
