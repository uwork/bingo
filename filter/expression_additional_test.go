package filter

import (
	"testing"

	"github.com/uwork/bingo/mysql/binlog"
)

func emptyRow() binlog.Row {
	return binlog.Row{}
}

func rowWith(cols ...binlog.Column) binlog.Row {
	return binlog.Row{Columns: cols}
}

func TestNewExpression(t *testing.T) {
	exp := NewExpression("$$0", "=", 10)
	if exp.Left != "$$0" {
		t.Errorf("Left = %v, want $$0", exp.Left)
	}
	if exp.Op != "=" {
		t.Errorf("Op = %v, want =", exp.Op)
	}
	if exp.Right != 10 {
		t.Errorf("Right = %v, want 10", exp.Right)
	}
}

func TestEvaluateInvalidOperator(t *testing.T) {
	exp := Expression{Left: 1, Op: "INVALID", Right: 2}
	_, err := exp.Evaluate(emptyRow())
	if err == nil {
		t.Error("expected error for invalid operator")
	}
}

func TestEvaluateOrWithInvalidLeft(t *testing.T) {
	exp := Expression{Left: "not_an_expression", Op: OP_OR, Right: Expression{1, OP_EQ, 1}}
	_, err := exp.Evaluate(emptyRow())
	if err == nil {
		t.Error("expected error for OR with non-Expression left")
	}
}

func TestEvaluateOrWithInvalidRight(t *testing.T) {
	exp := Expression{Left: Expression{1, OP_EQ, 1}, Op: OP_OR, Right: "not_an_expression"}
	_, err := exp.Evaluate(emptyRow())
	if err == nil {
		t.Error("expected error for OR with non-Expression right")
	}
}

func TestEvaluateAndWithInvalidLeft(t *testing.T) {
	exp := Expression{Left: 42, Op: OP_AND, Right: Expression{1, OP_EQ, 1}}
	_, err := exp.Evaluate(emptyRow())
	if err == nil {
		t.Error("expected error for AND with non-Expression left")
	}
}

func TestEvaluateAndWithInvalidRight(t *testing.T) {
	exp := Expression{Left: Expression{1, OP_EQ, 1}, Op: OP_AND, Right: 42}
	_, err := exp.Evaluate(emptyRow())
	if err == nil {
		t.Error("expected error for AND with non-Expression right")
	}
}

func TestConvertVarsInvalidLeftColumnIndex(t *testing.T) {
	// $$abc は無効なカラムインデックス
	exp := Expression{Left: "$$abc", Op: OP_EQ, Right: 10}
	row := rowWith(binlog.NewColumn(binlog.TYPE_LONG, 5))
	_, err := exp.Evaluate(row)
	if err == nil {
		t.Error("expected error for invalid left column index $$abc")
	}
}

func TestConvertVarsInvalidRightColumnIndex(t *testing.T) {
	// Right に無効なカラムインデックス
	exp := Expression{Left: 10, Op: OP_EQ, Right: "$$xyz"}
	row := rowWith(binlog.NewColumn(binlog.TYPE_LONG, 5))
	_, err := exp.Evaluate(row)
	if err == nil {
		t.Error("expected error for invalid right column index $$xyz")
	}
}

func TestConvertVarsIntLeftStringRightParseFail(t *testing.T) {
	// left=int, right=string で数値変換失敗
	exp := Expression{Left: 10, Op: OP_EQ, Right: "not_a_number"}
	_, err := exp.Evaluate(emptyRow())
	if err == nil {
		t.Error("expected error for int left with non-numeric string right")
	}
}

func TestConvertVarsIntLeftIntRight(t *testing.T) {
	// left=int, right=int のパス
	exp := Expression{Left: 5, Op: OP_EQ, Right: 5}
	ok, err := exp.Evaluate(emptyRow())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("5 == 5 should be true")
	}
}

func TestConvertVarsIntLeftColumnRight(t *testing.T) {
	// left=int, right=Column のパス
	row := rowWith(binlog.NewColumn(binlog.TYPE_LONG, 42))
	exp := Expression{Left: 42, Op: OP_EQ, Right: "$$0"}
	ok, err := exp.Evaluate(row)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("int 42 == Column(42) should be true")
	}
}

func TestConvertVarsStringLeftIntRight(t *testing.T) {
	// left=string, right=int -> right を string に変換
	exp := Expression{Left: "42", Op: OP_EQ, Right: 42}
	ok, err := exp.Evaluate(emptyRow())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("string '42' == int 42 (converted) should be true")
	}
}

func TestConvertVarsStringLeftStringRight(t *testing.T) {
	// left=string, right=string
	exp := Expression{Left: "hello", Op: OP_EQ, Right: "hello"}
	ok, err := exp.Evaluate(emptyRow())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("string 'hello' == string 'hello' should be true")
	}
}

func TestConvertVarsStringLeftColumnRight(t *testing.T) {
	// left=string, right=Column -> Column を string に変換
	row := rowWith(binlog.NewColumn(binlog.TYPE_LONG, 99))
	exp := Expression{Left: "99", Op: OP_EQ, Right: "$$0"}
	ok, err := exp.Evaluate(row)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("string '99' == Column(99).String() should be true")
	}
}

func TestConvertVarsColumnLeftIntRight(t *testing.T) {
	// left=Column, right=int -> Column を int に変換
	row := rowWith(binlog.NewColumn(binlog.TYPE_LONG, 7))
	exp := Expression{Left: "$$0", Op: OP_EQ, Right: 7}
	ok, err := exp.Evaluate(row)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("Column(7) == int 7 should be true")
	}
}

func TestConvertVarsColumnLeftStringRight(t *testing.T) {
	// left=Column, right=string -> Column を string に変換
	row := rowWith(binlog.NewColumn(binlog.TYPE_LONG, 7))
	exp := Expression{Left: "$$0", Op: OP_EQ, Right: "7"}
	ok, err := exp.Evaluate(row)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("Column(7).String() == '7' should be true")
	}
}

func TestConvertVarsColumnLeftColumnRight(t *testing.T) {
	// left=Column, right=Column
	row := rowWith(
		binlog.NewColumn(binlog.TYPE_LONG, 5),
		binlog.NewColumn(binlog.TYPE_LONG, 5),
	)
	exp := Expression{Left: "$$0", Op: OP_EQ, Right: "$$1"}
	ok, err := exp.Evaluate(row)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("Column(5) == Column(5) should be true")
	}
}

func TestNEOperator(t *testing.T) {
	exp := Expression{Left: 5, Op: OP_NE, Right: 10}
	ok, err := exp.Evaluate(emptyRow())
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("5 != 10 should be true")
	}

	exp2 := Expression{Left: 5, Op: OP_NE, Right: 5}
	ok, err = exp2.Evaluate(emptyRow())
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("5 != 5 should be false")
	}
}

func TestLTOperator(t *testing.T) {
	checkResult(t, Expression{5, OP_LT, 10}, emptyRow(), true)
	checkResult(t, Expression{10, OP_LT, 10}, emptyRow(), false)
}

func TestIsGeValuesUnsupportedType(t *testing.T) {
	// float64 は unsupported type -> エラー
	_, err := isGeValues(3.14, 3.14)
	if err == nil {
		t.Error("expected error for unsupported type in isGeValues")
	}
}

func TestIsGtValuesUnsupportedType(t *testing.T) {
	// float64 は unsupported type -> エラー
	_, err := isGtValues(3.14, 3.14)
	if err == nil {
		t.Error("expected error for unsupported type in isGtValues")
	}
}

func TestIsGeValuesString(t *testing.T) {
	ok, err := isGeValues("b", "a")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("'b' >= 'a' should be true")
	}

	ok, err = isGeValues("a", "b")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("'a' >= 'b' should be false")
	}
}

func TestIsGtValuesString(t *testing.T) {
	ok, err := isGtValues("b", "a")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("'b' > 'a' should be true")
	}
}

func TestIsEqValuesColumn(t *testing.T) {
	c1 := binlog.NewColumn(binlog.TYPE_LONG, 42)
	c2 := binlog.NewColumn(binlog.TYPE_LONG, 42)
	ok, err := isEqValues(c1, c2)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("Column(42) == Column(42) should be true")
	}
}

func TestIsGeValuesColumn(t *testing.T) {
	c1 := binlog.NewColumn(binlog.TYPE_LONG, 10)
	c2 := binlog.NewColumn(binlog.TYPE_LONG, 5)
	ok, err := isGeValues(c1, c2)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("Column(10) >= Column(5) should be true")
	}
}

func TestIsGtValuesColumn(t *testing.T) {
	c1 := binlog.NewColumn(binlog.TYPE_LONG, 10)
	c2 := binlog.NewColumn(binlog.TYPE_LONG, 5)
	ok, err := isGtValues(c1, c2)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("Column(10) > Column(5) should be true")
	}
}
