package sqlser

import (
	"fmt"
	"github.com/Masterminds/squirrel"
	pg_query "github.com/pganalyze/pg_query_go/v2"
	"reflect"
	"strings"
)

type ColumnInfo struct {
	Name       string
	DBTypeName string
	Kind       reflect.Kind
}

type Validator struct {
	ColumnInfo    ColumnInfo
	ValidatorFunc ValidatorFunc
}

type ValidatorFunc func(columnInfo ColumnInfo, value interface{}) error

type WhereParser interface {
	Parse(query string, qb squirrel.SelectBuilder) (*squirrel.SelectBuilder, error)
}

type parser struct {
	validators map[string]Validator
}

func NewWhereParser(validators map[string]Validator) WhereParser {
	p := &parser{}
	p.validators = make(map[string]Validator, len(validators))
	for k, v := range validators {
		p.validators[strings.ToLower(k)] = v
	}
	return p
}

func (p *parser) Parse(query string, qb squirrel.SelectBuilder) (*squirrel.SelectBuilder, error) {
	parseResult, err := pg_query.Parse("SELECT 1 " + query)
	if err != nil {
		return nil, err
	}
	selectStmt, err := getSelectStmt(parseResult)
	if err != nil {
		return nil, err
	}
	whereClause, err := getOnlyWhereClause(selectStmt)
	if err != nil {
		return nil, err
	}
	err = p.validateWhereClause(whereClause)
	if err != nil {
		return nil, err
	}
	query = strings.ReplaceAll(query, "WHERE", "")
	query = strings.ReplaceAll(query, "where", "")
	qb = qb.Where(strings.Trim(query," "))
	return &qb, nil
}

func (p *parser) validateWhereClause(whereClause *pg_query.Node) error {
	return p.recursiveValidation(whereClause)
}

func (p *parser) recursiveValidation(node *pg_query.Node) error {
	switch node.GetNode().(type) {
	case *pg_query.Node_BoolExpr:
		for _, arg := range node.GetBoolExpr().GetArgs() {
			err := p.recursiveValidation(arg)
			if err != nil {
				return err
			}
		}
	case *pg_query.Node_AExpr:
		aExpr := node.GetAExpr()
		if aExpr.GetKind() != pg_query.A_Expr_Kind_AEXPR_OP {
			return fmt.Errorf("unsupported AEXPR_OP")
		}

		columnRef := aExpr.GetLexpr().GetColumnRef()
		field := ""
		for i, f := range columnRef.GetFields() {
			field += f.GetString_().GetStr()
			if i < len(columnRef.Fields)-1 {
				field += "."
			}
		}

		aConst := aExpr.GetRexpr().GetAConst()
		var val interface{}
		switch aConst.GetVal().GetNode().(type) {
		case *pg_query.Node_Integer:
			val = aConst.GetVal().GetInteger().GetIval()
		case *pg_query.Node_String_:
			val = aConst.GetVal().GetString_().GetStr()
		}
		return p.validateField(field, val)

	case *pg_query.Node_ColumnRef:
		columnRef := node.GetColumnRef()
		field := ""
		for i, f := range columnRef.GetFields() {
			field += f.GetString_().GetStr()
			if i < len(columnRef.Fields)-1 {
				field += "."
			}
		}
		return p.validateField(field, true)
	}
	return nil
}

func (p *parser) validateField(field string, val interface{}) error {
	if _, ok := p.validators[field]; !ok {
		return nil
	}
	return p.validators[field].ValidatorFunc(p.validators[field].ColumnInfo, val)
}

func getSelectStmt(parseResult *pg_query.ParseResult) (*pg_query.SelectStmt, error) {
	if len(parseResult.Stmts) != 1 {
		return nil, fmt.Errorf("several statements")
	}

	return parseResult.Stmts[0].Stmt.GetSelectStmt(), nil
}

func getOnlyWhereClause(selectStmt *pg_query.SelectStmt) (*pg_query.Node, error) {
	if len(selectStmt.DistinctClause) > 0 ||
		len(selectStmt.GroupClause) > 0 ||
		len(selectStmt.LockingClause) > 0 ||
		len(selectStmt.WindowClause) > 0 ||
		len(selectStmt.SortClause) > 0 ||
		len(selectStmt.FromClause) > 0 ||
		selectStmt.LimitOffset != nil ||
		selectStmt.LimitCount != nil ||
		selectStmt.HavingClause != nil ||
		selectStmt.IntoClause != nil {
		return nil, fmt.Errorf("supports select with only 'where' clause")
	}

	whereClause := selectStmt.WhereClause
	if whereClause == nil {
		return nil, fmt.Errorf("no 'where' clause")
	}
	return whereClause, nil
}
