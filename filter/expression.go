package filter

import (
	"fmt"
	"github.com/uwork/bingo/mysql/binlog"
	"strconv"
	"strings"
)

const (
	OP_EQ  = "="
	OP_NE  = "!="
	OP_LE  = "<="
	OP_LT  = "<"
	OP_GE  = ">="
	OP_GT  = ">"
	OP_OR  = "or"
	OP_AND = "and"
)

type Expression struct {
	Left  interface{} `json:"left"`
	Op    string      `json:"op"`
	Right interface{} `json:"right"`
}

func NewExpression(left interface{}, op string, right interface{}) Expression {
	return Expression{left, op, right}
}

func (exp Expression) Evaluate(row binlog.Row) (bool, error) {
	var result bool
	switch exp.Op {
	case OP_EQ, OP_NE, OP_LE, OP_LT, OP_GE, OP_GT:
		return exp.doCompare(row)

	case OP_OR, OP_AND:

		exp1, ok := exp.Left.(Expression)
		if !ok {
			return false, fmt.Errorf("left is not valid expression: %#v", exp.Left)
		}
		exp2, ok := exp.Right.(Expression)
		if !ok {
			return false, fmt.Errorf("right is not valid expression: %#v", exp.Right)
		}

		rs1, err := exp1.Evaluate(row)
		if err != nil {
			return false, err
		}
		rs2, err := exp2.Evaluate(row)
		if err != nil {
			return false, err
		}

		if exp.Op == OP_AND {
			return rs1 && rs2, nil
		} else {
			return rs1 || rs2, nil
		}
	default:
		return false, fmt.Errorf("invalid operator: %#v", exp)
	}

	return result, nil
}

func (exp Expression) convertVars(row binlog.Row) (interface{}, interface{}, error) {
	left := exp.Left
	right := exp.Right
	if v, ok := exp.Left.(string); ok {
		if strings.HasPrefix(v, "$$") {
			colIndex, err := strconv.Atoi(v[2:])
			if err != nil {
				return nil, nil, fmt.Errorf("invalid column index: %s (%#v)", v, err)
			}
			left = row.Columns[colIndex]
		}
	}
	if v, ok := exp.Right.(string); ok {
		if strings.HasPrefix(v, "$$") {
			colIndex, err := strconv.Atoi(v[2:])
			if err != nil {
				return nil, nil, fmt.Errorf("invalid column index: %s (%#v)", v, err)
			}
			right = row.Columns[colIndex]
		}
	}

	// right data type convert to int
	if _, ok := left.(int); ok {
		if _, ok := right.(int); ok {
			// int == int
		} else if sright, ok := right.(string); ok {
			iright, err := strconv.Atoi(sright)
			if err != nil {
				return nil, nil, err
			}
			right = iright
		} else if cright, ok := right.(binlog.Column); ok {
			iright := cright.Int()
			right = iright
		}
	} else if _, ok := left.(string); ok {
		// right data type convert to string
		if iright, ok := right.(int); ok {
			sright := strconv.Itoa(iright)
			right = sright
		} else if _, ok := right.(string); ok {
			// string == string
		} else if cright, ok := right.(binlog.Column); ok {
			sright := cright.String()
			right = sright
		}
	} else if cleft, ok := left.(binlog.Column); ok {
		if _, ok := right.(int); ok {
			// left data type convert to int (right data type)
			left = cleft.Int()
		} else if _, ok := right.(string); ok {
			// left data type convert to string (right data type)
			left = cleft.String()
		} else if _, ok := right.(binlog.Column); ok {
			// Column == Column
		}
	}

	return left, right, nil
}

func (exp Expression) doCompare(row binlog.Row) (bool, error) {
	left, right, err := exp.convertVars(row)
	if err != nil {
		return false, err
	}

	switch exp.Op {
	case OP_EQ:
		return isEqValues(left, right)
	case OP_NE:
		b, err := isEqValues(left, right)
		return !b, err
	case OP_GE:
		return isGeValues(left, right)
	case OP_GT:
		return isGtValues(left, right)
	case OP_LE:
		b, err := isGtValues(left, right)
		return !b, err
	case OP_LT:
		b, err := isGeValues(left, right)
		return !b, err
	}

	return false, nil
}

func isEqValues(left interface{}, right interface{}) (bool, error) {
	if cleft, ok := left.(binlog.Column); ok {
		cright, _ := right.(binlog.Column)
		return cleft.Equals(cright), nil
	}
	return left == right, nil
}

func isGeValues(left interface{}, right interface{}) (bool, error) {
	if cleft, ok := left.(binlog.Column); ok {
		cright, _ := right.(binlog.Column)
		return cleft.GreaterEquals(cright), nil
	} else if ileft, ok := left.(int); ok {
		iright, _ := right.(int)
		return ileft >= iright, nil
	} else if sleft, ok := left.(string); ok {
		sright, _ := right.(string)
		return sleft >= sright, nil
	}
	return false, fmt.Errorf("unsupported type. left: %#v, right: %#v", left, right)
}

func isGtValues(left interface{}, right interface{}) (bool, error) {
	if cleft, ok := left.(binlog.Column); ok {
		cright, _ := right.(binlog.Column)
		return cleft.GreaterThan(cright), nil
	} else if ileft, ok := left.(int); ok {
		iright, _ := right.(int)
		return ileft > iright, nil
	} else if sleft, ok := left.(string); ok {
		sright, _ := right.(string)
		return sleft > sright, nil
	}
	return false, fmt.Errorf("unsupported type. left: %#v, right: %#v", left, right)
}
