package filter

import (
	"github.com/uwork/bingo/mysql/binlog"
	"testing"
)

func checkResult(t *testing.T, exp Expression, row binlog.Row, expect bool) {
	ok, err := exp.Evaluate(row)
	if err != nil {
		t.Error(err)
	}
	if ok != expect {
		t.Error("not valid: ", exp)
	}
}

func TestEvalExpression(t *testing.T) {
	row := binlog.Row{}
	row.Columns = make([]binlog.Column, 2)
	row.Columns[0] = binlog.NewColumn(binlog.TYPE_LONG, 10)
	row.Columns[1] = binlog.NewColumn(binlog.TYPE_STRING, "10")

	exp1 := Expression{10, OP_EQ, 10}
	checkResult(t, exp1, row, true)

	exp2 := Expression{10, OP_EQ, 11}
	checkResult(t, exp2, row, false)

	expOr := Expression{exp1, OP_OR, exp2}
	checkResult(t, expOr, row, true)

	expAnd := Expression{exp1, OP_AND, exp2}
	checkResult(t, expAnd, row, false)

	checkResult(t, Expression{10, OP_GE, 9}, row, true)
	checkResult(t, Expression{10, OP_GE, 10}, row, true)
	checkResult(t, Expression{10, OP_GE, 11}, row, false)
	checkResult(t, Expression{10, OP_GT, 9}, row, true)
	checkResult(t, Expression{10, OP_GT, 10}, row, false)
	checkResult(t, Expression{10, OP_GT, 11}, row, false)

	checkResult(t, Expression{9, OP_LE, 10}, row, true)
	checkResult(t, Expression{10, OP_LE, 10}, row, true)
	checkResult(t, Expression{11, OP_LE, 10}, row, false)
	checkResult(t, Expression{9, OP_LT, 10}, row, true)
	checkResult(t, Expression{10, OP_LT, 10}, row, false)
	checkResult(t, Expression{11, OP_LT, 10}, row, false)

	checkResult(t, Expression{9, OP_EQ, "9"}, row, true)
	checkResult(t, Expression{9, OP_EQ, "99"}, row, false)
	checkResult(t, Expression{"9", OP_EQ, 9}, row, true)
	checkResult(t, Expression{"9", OP_EQ, 99}, row, false)

	checkResult(t, Expression{9, OP_GE, "9"}, row, true)
	checkResult(t, Expression{9, OP_GE, "99"}, row, false)
	checkResult(t, Expression{"9", OP_GE, 9}, row, true)
	checkResult(t, Expression{"9", OP_GE, 99}, row, false)

	checkResult(t, Expression{"$$0", OP_EQ, 10}, row, true)
	checkResult(t, Expression{10, OP_EQ, "$$0"}, row, true)
	checkResult(t, Expression{"$$0", OP_EQ, "$$0"}, row, true)
	checkResult(t, Expression{"$$0", OP_EQ, "$$1"}, row, true)
}
