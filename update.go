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
	table string
}

// UpdatePendingWhere is an SQL UPDATE query which is waiting for a
// WHERE condition to be supplied via its' Where() method.
type UpdatePendingWhere struct {
	UpdatePendingSet
	set  string
	args []interface{}
	err  error
}

// PendingUpdate is an UPDATE query which is ready to execute. Its'
// statement and arguments can be inspected with its' Statement() and
// Args() methods.
type PendingUpdate struct {
	UpdatePendingWhere
	where     string
	whereArgs []interface{}
}

// Update is used to begin constructing a SQL UPDATE query. It takes a
// a target table name. Method calls on the result allow the query to
// be finalized and executed.
//
//   type row struct{
//       Id int
//       Name string
//   }
//   res, err := Update("X").Set(row{Name: "updated"}).Where("Id = ?", 1).Exec(db)
//
// Zero-values in the struct passed to Set() are ignored.
func Update(table string) UpdatePendingSet {
	return UpdatePendingSet{table}
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
			name, ignore := parseTag(tag, "update")
			if ignore {
				continue // Explicitly ignored
			}
			value := v.FieldByIndex(append(index, field.Index...))
			if value.IsZero() {
				continue // Ignore zero-value
			}
			set = append(set, name+" = ?")
			vals = append(vals, value.Interface())
		}
	}
	recurseFields(v.Type(), []int{})

	if len(set) < 1 {
		return err("no fields to update")
	}

	return UpdatePendingWhere{
		UpdatePendingSet: u,
		set:              strings.Join(set, ", "),
		args:             vals,
	}
}

// Where allows a WHERE condition to be added to a pending UPDATE
// query. It also executes the query and returns the results.
func (u UpdatePendingWhere) Where(where string, args ...interface{}) PendingUpdate {
	p := PendingUpdate{
		UpdatePendingWhere: u,
		where:              where,
	}
	p.args = append(p.args, args...)
	return p
}

func (p PendingUpdate) Statement() string {
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", p.table, p.set, p.where)
}

func (p PendingUpdate) Args() []interface{} {
	return p.args
}

func (p PendingUpdate) Exec(db Executor) (sql.Result, error) {
	if p.err != nil {
		return nil, p.err
	}
	return db.Exec(p.Statement(), p.Args()...)
}
