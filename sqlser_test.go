package sqlser

import (
	"fmt"
	"github.com/Masterminds/squirrel"
	"reflect"
	"testing"
)

type TestCase struct {
	Name   string
	Query  string
	Parser WhereParser
}

func TestInvalidQueries(t *testing.T) {
	testCases := []TestCase{
		{
			Name:   "Invalid sql statement",
			Query:  "2",
			Parser: NewWhereParser(nil),
		},
		{
			Name:   "Several sql statements",
			Query:  "; SELECT 1;",
			Parser: NewWhereParser(nil),
		},
		{
			Name:   "Invalid statement: statement with only 'where' clause are allowed",
			Query:  "LIMIT 10;",
			Parser: NewWhereParser(nil),
		},
		{
			Name:   "Invalid statement: statement with no 'where' clause are not allowed",
			Query:  "",
			Parser: NewWhereParser(nil),
		},
		{
			Name:   "Unsupported expression in 'where' clause",
			Query:  "WHERE testDB.test_table.test_data IN (1,2,3) AND testDB.test_table.other_test_data = 'test'",
			Parser: NewWhereParser(nil),
		},

	}
	qb := squirrel.SelectBuilder{}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			_, err := tc.Parser.Parse(tc.Query, qb)
			if err == nil {
				t.Errorf("should be error")
			}
			t.Log(err)
		})
	}
}

func TestQueriesWithMismatchedTypes(t *testing.T) {
	var validatorFunc ValidatorFunc = func(columnInfo ColumnInfo, value interface{}) error {
		typeOf := reflect.TypeOf(value)
		if columnInfo.Kind == typeOf.Kind() {
			return nil
		}
		return fmt.Errorf("mismatched types: %v and %v", columnInfo.Kind, typeOf.Kind())
	}
	testCases := []TestCase{
		{
			Name:  "Not bool column's type",
			Query: `WHERE testDB.test_table.not_bool_type_col`,
			Parser: NewWhereParser(map[string]Validator{
				"testDB.test_table.not_bool_type_col": {
					ColumnInfo: ColumnInfo{
						Name:       "testDB.test_table.not_bool_type_col",
						DBTypeName: "VARCHAR",
						Kind:       reflect.String,
					},
					ValidatorFunc: validatorFunc,
				},
			}),
		},
		{
			Name:  "Not integer column's type",
			Query: `WHERE testDB.test_table.not_int_type_col = 1`,
			Parser: NewWhereParser(map[string]Validator{
				"testDB.test_table.not_int_type_col": {
					ColumnInfo: ColumnInfo{
						Name:       "testDB.test_table.not_int_type_col",
						DBTypeName: "VARCHAR",
						Kind:       reflect.String,
					},
					ValidatorFunc: validatorFunc,
				},
			}),
		},
		{
			Name:  "Not string column's type",
			Query: `WHERE testDB.test_table.not_str_type_col = 'test''`,
			Parser: NewWhereParser(map[string]Validator{
				"testDB.test_table.not_str_type_col": {
					ColumnInfo: ColumnInfo{
						Name:       "testDB.test_table.not_str_type_col",
						DBTypeName: "SMALLINT",
						Kind:       reflect.Int16,
					},
					ValidatorFunc: validatorFunc,
				},
			}),
		},
	}
	qb := squirrel.SelectBuilder{}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			_, err := tc.Parser.Parse(tc.Query, qb)
			if err == nil {
				t.Errorf("should be error")
			}
			t.Log(err)
		})
	}
}

func TestParseCorrectness(t *testing.T) {
	var validatorFunc ValidatorFunc = func(columnInfo ColumnInfo, value interface{}) error {
		typeOf := reflect.TypeOf(value)
		if columnInfo.Kind == typeOf.Kind() {
			return nil
		}
		return fmt.Errorf("mismatched types: %v and %v", columnInfo.Kind, typeOf.Kind())
	}
	testCases := []TestCase{
		{
			Name:  "Boolean column's type",
			Query: `WHERE testDB.test_table.test_data`,
			Parser: NewWhereParser(map[string]Validator{
				"testDB.test_table.test_data": {
					ColumnInfo: ColumnInfo{
						Name:       "testDB.test_table.test_data",
						DBTypeName: "BOOLEAN",
						Kind:       reflect.Bool,
					},
					ValidatorFunc: validatorFunc,
				},
			}),
		},
		{
			Name:  "Integer column's type",
			Query: `WHERE testDB.test_table.test_data = 1`,
			Parser: NewWhereParser(map[string]Validator{
				"testDB.test_table.test_data": {
					ColumnInfo: ColumnInfo{
						Name:       "testDB.test_table.test_data",
						DBTypeName: "SMALLINT",
						Kind:       reflect.Int32,
					},
					ValidatorFunc: validatorFunc,
				},
			}),
		},
		{
			Name:  "String column's type",
			Query: `WHERE testDB.test_table.test_data = 'test'`,
			Parser: NewWhereParser(map[string]Validator{
				"testDB.test_table.test_data": {
					ColumnInfo: ColumnInfo{
						Name:       "testDB.test_table.test_data",
						DBTypeName: "VARCHAR",
						Kind:       reflect.String,
					},
					ValidatorFunc: validatorFunc,
				},
			}),
		},
		{
			Name:  "String column's type with no validation",
			Query: `WHERE testDB.test_table.test_data = 'test'`,
			Parser: NewWhereParser(nil),
		},
		{
			Name:  "TT #1",
			Query: `WHERE Foo.Bar.Beta > 21 AND Alpha.Bar != 'hello'`,
			Parser: NewWhereParser(map[string]Validator{
				"Foo.Bar.Beta": {
					ColumnInfo: ColumnInfo{
						Name:       "Foo.Bar.Beta",
						DBTypeName: "SMALLINT",
						Kind:       reflect.Int32,
					},
					ValidatorFunc: validatorFunc,
				},
				"Alpha.Bar": {
					ColumnInfo: ColumnInfo{
						Name:       "Alpha.Bar",
						DBTypeName: "VARCHAR",
						Kind:       reflect.String,
					},
					ValidatorFunc: validatorFunc,
				},
			}),
		},
		{
			Name:  "TT #2",
			Query: `WHERE Alice.IsActive AND Bob.LastHash = 'ab5534b'`,
			Parser: NewWhereParser(map[string]Validator{
				"Alice.IsActive": {
					ColumnInfo: ColumnInfo{
						Name:       "Alice.IsActive",
						DBTypeName: "BOOLEAN",
						Kind:       reflect.Bool,
					},
					ValidatorFunc: validatorFunc,
				},
				"Bob.LastHash": {
					ColumnInfo: ColumnInfo{
						Name:       "Bob.LastHash",
						DBTypeName: "VARCHAR",
						Kind:       reflect.String,
					},
					ValidatorFunc: validatorFunc,
				},
			}),
		},
		{
			Name:  "TT #3",
			Query: `WHERE Alice.Name ~ 'A.*' OR Bob.LastName !~ 'Bill.*'`,
			Parser: NewWhereParser(map[string]Validator{
				"Alice.Name": {
					ColumnInfo: ColumnInfo{
						Name:       "Alice.Name",
						DBTypeName: "VARCHAR",
						Kind:       reflect.String,
					},
					ValidatorFunc: validatorFunc,
				},
				"Bob.LastName": {
					ColumnInfo: ColumnInfo{
						Name:       "Bob.LastName",
						DBTypeName: "VARCHAR",
						Kind:       reflect.String,
					},
					ValidatorFunc: validatorFunc,
				},
			}),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {

			qb, err := tc.Parser.Parse(tc.Query, squirrel.Select("*").From("testDB.test_table"))
			if err != nil {
				t.Errorf("should not be error")
				t.Log(err)
				return
			}
			query, _, err := qb.ToSql()
			if err != nil {
				return
			}
			t.Log(query)
		})
	}
}
