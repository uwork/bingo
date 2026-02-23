package filter

import (
	"encoding/json"
	"testing"

	"github.com/uwork/bingo/mysql/binlog"
)

// テスト用ヘルパー
func makeRow(cols ...binlog.Column) binlog.Row {
	return binlog.Row{Columns: cols}
}

func intCol(v int) binlog.Column {
	return binlog.NewColumn(binlog.TYPE_LONG, v)
}

func strCol(v string) binlog.Column {
	return binlog.NewColumn(binlog.TYPE_STRING, v)
}

func makeEvent(schema, table string, rows []binlog.Row) *binlog.BinlogEvent {
	return &binlog.BinlogEvent{
		Header: &binlog.BinlogEventHeader{},
		Rows: &binlog.BinlogEventRows{
			Schema: schema,
			Table:  table,
			Rows:   rows,
		},
	}
}

func TestNewFilteredRow(t *testing.T) {
	row := makeRow(intCol(42), strCol("hello"))
	fr := NewFilteredRow(row)

	if len(fr.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(fr.Columns))
	}
	if fr.Columns[0] != "42" {
		t.Errorf("expected '42', got %s", fr.Columns[0])
	}
	if fr.Columns[1] != "hello" {
		t.Errorf("expected 'hello', got %s", fr.Columns[1])
	}
}

func TestNewFilteredRowEmpty(t *testing.T) {
	row := makeRow()
	fr := NewFilteredRow(row)
	if len(fr.Columns) != 0 {
		t.Errorf("expected 0 columns, got %d", len(fr.Columns))
	}
}

func TestFilterEventNoFilters(t *testing.T) {
	rows := []binlog.Row{makeRow(intCol(1)), makeRow(intCol(2))}
	ev := makeEvent("testdb", "testtable", rows)
	fc := &FilterConfig{Filters: []Filter{}}

	data, err := fc.FilterEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("expected data, got nil")
	}

	var result []FilteredRow
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result))
	}
	// database/table はイベントから設定される
	if result[0].Database != "testdb" {
		t.Errorf("expected database testdb, got %s", result[0].Database)
	}
	if result[0].Table != "testtable" {
		t.Errorf("expected table testtable, got %s", result[0].Table)
	}
}

func TestFilterEventNoRows(t *testing.T) {
	ev := makeEvent("testdb", "testtable", []binlog.Row{})
	fc := &FilterConfig{Filters: []Filter{}}

	data, err := fc.FilterEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Error("expected nil for empty rows")
	}
}

func TestFilterEventDatabaseMatch(t *testing.T) {
	rows := []binlog.Row{makeRow(intCol(1))}
	ev := makeEvent("mydb", "mytable", rows)
	fc := &FilterConfig{Filters: []Filter{
		{Database: "mydb", Table: "", Columns: nil, Where: Expression{}},
	}}

	data, err := fc.FilterEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Error("expected data for matching database")
	}
}

func TestFilterEventDatabaseNoMatch(t *testing.T) {
	rows := []binlog.Row{makeRow(intCol(1))}
	ev := makeEvent("mydb", "mytable", rows)
	fc := &FilterConfig{Filters: []Filter{
		{Database: "otherdb", Table: "", Columns: nil, Where: Expression{}},
	}}

	data, err := fc.FilterEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Error("expected nil for non-matching database")
	}
}

func TestFilterEventTableMatch(t *testing.T) {
	rows := []binlog.Row{makeRow(intCol(1))}
	ev := makeEvent("mydb", "mytable", rows)
	fc := &FilterConfig{Filters: []Filter{
		{Database: "", Table: "mytable", Columns: nil, Where: Expression{}},
	}}

	data, err := fc.FilterEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Error("expected data for matching table")
	}
}

func TestFilterEventTableNoMatch(t *testing.T) {
	rows := []binlog.Row{makeRow(intCol(1))}
	ev := makeEvent("mydb", "mytable", rows)
	fc := &FilterConfig{Filters: []Filter{
		{Database: "", Table: "othertable", Columns: nil, Where: Expression{}},
	}}

	data, err := fc.FilterEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Error("expected nil for non-matching table")
	}
}

func TestFilterEventColumnSelection(t *testing.T) {
	row := makeRow(intCol(1), strCol("hello"), intCol(3))
	ev := makeEvent("mydb", "mytable", []binlog.Row{row})
	fc := &FilterConfig{Filters: []Filter{
		{Database: "mydb", Table: "mytable", Columns: []int{0, 2}, Where: Expression{}},
	}}

	data, err := fc.FilterEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("expected data")
	}

	var result []FilteredRow
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result))
	}
	if len(result[0].Columns) != 2 {
		t.Errorf("expected 2 columns after selection, got %d", len(result[0].Columns))
	}
	if result[0].Columns[0] != "1" || result[0].Columns[1] != "3" {
		t.Errorf("unexpected columns: %v", result[0].Columns)
	}
}

func TestFilterEventWhereMatch(t *testing.T) {
	row1 := makeRow(intCol(10))
	row2 := makeRow(intCol(20))
	ev := makeEvent("mydb", "mytable", []binlog.Row{row1, row2})
	fc := &FilterConfig{Filters: []Filter{
		{Database: "mydb", Table: "mytable", Columns: nil,
			Where: Expression{Left: "$$0", Op: OP_EQ, Right: 10}},
	}}

	data, err := fc.FilterEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if data == nil {
		t.Fatal("expected data")
	}

	var result []FilteredRow
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 row after where filter, got %d", len(result))
	}
}

func TestFilterEventWhereNoMatch(t *testing.T) {
	row := makeRow(intCol(99))
	ev := makeEvent("mydb", "mytable", []binlog.Row{row})
	fc := &FilterConfig{Filters: []Filter{
		{Database: "mydb", Table: "mytable", Columns: nil,
			Where: Expression{Left: "$$0", Op: OP_EQ, Right: 10}},
	}}

	data, err := fc.FilterEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if data != nil {
		t.Error("expected nil for non-matching where condition")
	}
}

func TestFilterEventWhereError(t *testing.T) {
	row := makeRow(intCol(10))
	ev := makeEvent("mydb", "mytable", []binlog.Row{row})
	// OP_OR に Expression でない値を渡すとエラー
	fc := &FilterConfig{Filters: []Filter{
		{Database: "mydb", Table: "mytable", Columns: nil,
			Where: Expression{Left: "not_expression", Op: OP_OR, Right: "also_not"}},
	}}

	_, err := fc.FilterEvent(ev)
	if err == nil {
		t.Error("expected error for invalid OR expression")
	}
}

func TestFilterEventAllColumns(t *testing.T) {
	// Columns が空（nil）のとき全列を転送する
	row := makeRow(intCol(1), strCol("a"), intCol(3))
	ev := makeEvent("db", "tbl", []binlog.Row{row})
	fc := &FilterConfig{Filters: []Filter{
		{Database: "db", Table: "tbl", Columns: nil, Where: Expression{}},
	}}

	data, err := fc.FilterEvent(ev)
	if err != nil {
		t.Fatal(err)
	}

	var result []FilteredRow
	json.Unmarshal(data, &result)
	if len(result[0].Columns) != 3 {
		t.Errorf("expected 3 columns for nil Columns filter, got %d", len(result[0].Columns))
	}
}
