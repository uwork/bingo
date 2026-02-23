package binlog

import (
	"testing"
	"time"
)

// ---- NewColumn / Int ----

func TestNewColumnInt(t *testing.T) {
	c := NewColumn(TYPE_LONG, 42)
	if c.Int() != 42 {
		t.Errorf("TYPE_LONG Int() = %d, want 42", c.Int())
	}
	c2 := NewColumn(TYPE_LONGLONG, 9999)
	if c2.Int() != 9999 {
		t.Errorf("TYPE_LONGLONG Int() = %d, want 9999", c2.Int())
	}
}

func TestColumnIntFromFloat(t *testing.T) {
	c := NewColumn(TYPE_FLOAT, 3.7)
	if c.Int() != 3 {
		t.Errorf("TYPE_FLOAT Int() = %d, want 3", c.Int())
	}
	c2 := NewColumn(TYPE_DOUBLE, 5.9)
	if c2.Int() != 5 {
		t.Errorf("TYPE_DOUBLE Int() = %d, want 5", c2.Int())
	}
}

func TestColumnIntFromTime(t *testing.T) {
	t1 := time.Unix(1000000, 0)
	c := NewColumn(TYPE_DATETIME, t1)
	if c.Int() != 1000000 {
		t.Errorf("TYPE_DATETIME Int() = %d, want 1000000", c.Int())
	}
	c2 := NewColumn(TYPE_TIMESTAMP2, t1)
	if c2.Int() != 1000000 {
		t.Errorf("TYPE_TIMESTAMP2 Int() = %d, want 1000000", c2.Int())
	}
}

func TestColumnIntFromString(t *testing.T) {
	c := NewColumn(TYPE_STRING, "42")
	if c.Int() != 42 {
		t.Errorf("TYPE_STRING Int('42') = %d, want 42", c.Int())
	}
	// 数値変換失敗 -> 0
	c2 := NewColumn(TYPE_STRING, "not_a_number")
	if c2.Int() != 0 {
		t.Errorf("TYPE_STRING Int('not_a_number') = %d, want 0", c2.Int())
	}
}

func TestColumnIntFromNull(t *testing.T) {
	c := NewColumn(TYPE_NULL, nil)
	c.IsNull = true
	if c.Int() != 1 {
		t.Errorf("TYPE_NULL(null) Int() = %d, want 1", c.Int())
	}
	c2 := NewColumn(TYPE_NULL, nil)
	c2.IsNull = false
	if c2.Int() != 0 {
		t.Errorf("TYPE_NULL(non-null) Int() = %d, want 0", c2.Int())
	}
}

func TestColumnIntFromBlob(t *testing.T) {
	// 1バイト: 0x0a = 10
	c := NewColumn(TYPE_BLOB, []byte{0x0a})
	if c.Int() != 10 {
		t.Errorf("TYPE_BLOB Int() = %d, want 10", c.Int())
	}
}

// ---- Double ----

func TestColumnDoubleFromInt(t *testing.T) {
	c := NewColumn(TYPE_LONG, 100)
	if c.Double() != 100.0 {
		t.Errorf("TYPE_LONG Double() = %f, want 100.0", c.Double())
	}
}

func TestColumnDoubleFromFloat(t *testing.T) {
	c := NewColumn(TYPE_DOUBLE, 3.14)
	if c.Double() != 3.14 {
		t.Errorf("TYPE_DOUBLE Double() = %f, want 3.14", c.Double())
	}
}

func TestColumnDoubleFromTime(t *testing.T) {
	t1 := time.Unix(1000000, 0)
	c := NewColumn(TYPE_DATETIME, t1)
	if c.Double() != 1000000.0 {
		t.Errorf("TYPE_DATETIME Double() = %f, want 1000000.0", c.Double())
	}
}

func TestColumnDoubleFromString(t *testing.T) {
	c := NewColumn(TYPE_STRING, "3.14")
	if c.Double() != 3.14 {
		t.Errorf("TYPE_STRING Double('3.14') = %f, want 3.14", c.Double())
	}
	// 変換失敗 -> 0.0
	c2 := NewColumn(TYPE_STRING, "not_a_float")
	if c2.Double() != 0.0 {
		t.Errorf("TYPE_STRING Double('not_a_float') = %f, want 0.0", c2.Double())
	}
}

func TestColumnDoubleFromNull(t *testing.T) {
	c := NewColumn(TYPE_NULL, nil)
	c.IsNull = true
	if c.Double() != 1.0 {
		t.Errorf("TYPE_NULL(null) Double() = %f, want 1.0", c.Double())
	}
	c2 := NewColumn(TYPE_NULL, nil)
	c2.IsNull = false
	if c2.Double() != 0.0 {
		t.Errorf("TYPE_NULL(non-null) Double() = %f, want 0.0", c2.Double())
	}
}

func TestColumnDoubleFromBlob(t *testing.T) {
	c := NewColumn(TYPE_BLOB, []byte{0x0a})
	if c.Double() != 10.0 {
		t.Errorf("TYPE_BLOB Double() = %f, want 10.0", c.Double())
	}
}

// ---- Bytes ----

func TestColumnBytesBlob(t *testing.T) {
	c := NewColumn(TYPE_BLOB, []byte{0x01, 0x02, 0x03})
	b := c.Bytes()
	if len(b) != 3 || b[0] != 0x01 {
		t.Errorf("TYPE_BLOB Bytes() = %v, want [1 2 3]", b)
	}
}

func TestColumnBytesDouble(t *testing.T) {
	c := NewColumn(TYPE_DOUBLE, 0.0)
	b := c.Bytes()
	if len(b) != 8 {
		t.Errorf("TYPE_DOUBLE Bytes() len = %d, want 8", len(b))
	}
}

func TestColumnBytesFloat(t *testing.T) {
	c := NewColumn(TYPE_FLOAT, 1.5)
	b := c.Bytes()
	if len(b) != 8 {
		t.Errorf("TYPE_FLOAT Bytes() len = %d, want 8 (double encoding)", len(b))
	}
}

func TestColumnBytesNullTrue(t *testing.T) {
	c := NewColumn(TYPE_NULL, nil)
	c.IsNull = true
	b := c.Bytes()
	if len(b) != 1 || b[0] != 1 {
		t.Errorf("TYPE_NULL(null) Bytes() = %v, want [1]", b)
	}
}

func TestColumnBytesNullFalse(t *testing.T) {
	c := NewColumn(TYPE_NULL, nil)
	c.IsNull = false
	b := c.Bytes()
	if len(b) != 1 || b[0] != 0 {
		t.Errorf("TYPE_NULL(non-null) Bytes() = %v, want [0]", b)
	}
}

func TestColumnBytesDefault(t *testing.T) {
	// デフォルトケース: bin フィールドを返す
	c := NewColumn(TYPE_LONG, 42)
	b := c.Bytes()
	// TYPE_LONG は NewColumn で bin を設定しないので nil
	_ = b // パニックしないことを確認
}

// ---- Time ----

func TestColumnTimeFromDatetime(t *testing.T) {
	t1 := time.Unix(1000000, 0).UTC()
	c := NewColumn(TYPE_DATETIME, t1)
	if !c.Time().Equal(t1) {
		t.Errorf("TYPE_DATETIME Time() = %v, want %v", c.Time(), t1)
	}
}

func TestColumnTimeFromTimestamp2(t *testing.T) {
	t1 := time.Unix(2000000, 0).UTC()
	c := NewColumn(TYPE_TIMESTAMP2, t1)
	if !c.Time().Equal(t1) {
		t.Errorf("TYPE_TIMESTAMP2 Time() = %v, want %v", c.Time(), t1)
	}
}

func TestColumnTimeDefault(t *testing.T) {
	// デフォルト: Int() -> time.Unix
	c := NewColumn(TYPE_LONG, 1000000)
	expected := time.Unix(1000000, 0)
	if !c.Time().Equal(expected) {
		t.Errorf("TYPE_LONG Time() = %v, want %v", c.Time(), expected)
	}
}

// ---- String ----

func TestColumnStringTypes(t *testing.T) {
	t1 := time.Date(2020, 6, 15, 10, 30, 45, 0, time.UTC)

	tests := []struct {
		col  Column
		want string
	}{
		{NewColumn(TYPE_LONG, 42), "42"},
		{NewColumn(TYPE_TINY, -1), "-1"},
		{NewColumn(TYPE_STRING, "hello"), "hello"},
		{NewColumn(TYPE_VAR_STRING, "var"), "var"},
		{NewColumn(TYPE_VARCHAR, "varchar"), "varchar"},
		{NewColumn(TYPE_BLOB, []byte("blob data")), "blob data"},
		{NewColumn(TYPE_TINY_BLOB, []byte("tiny")), "tiny"},
		{NewColumn(TYPE_NULL, nil), "[NULL]"},
	}

	for _, tt := range tests {
		got := tt.col.String()
		if got != tt.want {
			t.Errorf("Column.String() = %q, want %q (Type=%d)", got, tt.want, tt.col.Type)
		}
	}

	// 時刻型
	c := NewColumn(TYPE_DATETIME, t1)
	if c.String() != "2020-06-15 10:30:45" {
		t.Errorf("TYPE_DATETIME String() = %s", c.String())
	}
	c2 := NewColumn(TYPE_DATETIME2, t1)
	if c2.String() != "2020-06-15 10:30:45" {
		t.Errorf("TYPE_DATETIME2 String() = %s", c2.String())
	}
	c3 := NewColumn(TYPE_TIME2, t1)
	if c3.String() != "10:30:45" {
		t.Errorf("TYPE_TIME2 String() = %s", c3.String())
	}
	c4 := NewColumn(TYPE_DATE, t1)
	if c4.String() != "2020-06-15" {
		t.Errorf("TYPE_DATE String() = %s", c4.String())
	}
	c5 := NewColumn(TYPE_TIMESTAMP2, t1)
	if c5.String() != "2020-06-15 10:30:45" {
		t.Errorf("TYPE_TIMESTAMP2 String() = %s", c5.String())
	}
}

func TestColumnStringFloat(t *testing.T) {
	c := NewColumn(TYPE_FLOAT, 1.5)
	s := c.String()
	if len(s) == 0 {
		t.Error("TYPE_FLOAT String() should not be empty")
	}
}

// ---- Equals ----

func TestColumnEqualsBothNull(t *testing.T) {
	c1 := NewColumn(TYPE_NULL, nil)
	c1.IsNull = true
	c2 := NewColumn(TYPE_NULL, nil)
	c2.IsNull = true
	if !c1.Equals(c2) {
		t.Error("null Equals(null) should be true")
	}
}

func TestColumnEqualsNullNonNull(t *testing.T) {
	c1 := NewColumn(TYPE_NULL, nil)
	c1.IsNull = true
	c2 := NewColumn(TYPE_LONG, 0)
	if c1.Equals(c2) {
		t.Error("null Equals(non-null) should be false")
	}
}

func TestColumnEqualsNonNullNull(t *testing.T) {
	c1 := NewColumn(TYPE_LONG, 10)
	c2 := NewColumn(TYPE_LONG, 10)
	c2.IsNull = true
	if c1.Equals(c2) {
		t.Error("non-null Equals(null) should be false")
	}
}

func TestColumnEqualsInt(t *testing.T) {
	c1 := NewColumn(TYPE_LONG, 42)
	c2 := NewColumn(TYPE_LONG, 42)
	if !c1.Equals(c2) {
		t.Error("Column(42) Equals Column(42) should be true")
	}
	c3 := NewColumn(TYPE_LONG, 43)
	if c1.Equals(c3) {
		t.Error("Column(42) Equals Column(43) should be false")
	}
}

func TestColumnEqualsDouble(t *testing.T) {
	c1 := NewColumn(TYPE_DOUBLE, 3.14)
	c2 := NewColumn(TYPE_DOUBLE, 3.14)
	if !c1.Equals(c2) {
		t.Error("Column(3.14) Equals Column(3.14) should be true")
	}
}

func TestColumnEqualsDatetime(t *testing.T) {
	t1 := time.Unix(1000000, 0)
	c1 := NewColumn(TYPE_DATETIME, t1)
	c2 := NewColumn(TYPE_DATETIME, t1)
	if !c1.Equals(c2) {
		t.Error("Column(time) Equals Column(same_time) should be true")
	}
}

func TestColumnEqualsBlob(t *testing.T) {
	c1 := NewColumn(TYPE_BLOB, []byte{0x01, 0x02})
	c2 := NewColumn(TYPE_BLOB, []byte{0x01, 0x02})
	if !c1.Equals(c2) {
		t.Error("Column(blob) Equals Column(same_blob) should be true")
	}
	c3 := NewColumn(TYPE_BLOB, []byte{0x01, 0x03})
	if c1.Equals(c3) {
		t.Error("Column(blob) Equals Column(diff_blob) should be false")
	}
}

func TestColumnEqualsString(t *testing.T) {
	c1 := NewColumn(TYPE_STRING, "hello")
	c2 := NewColumn(TYPE_STRING, "hello")
	if !c1.Equals(c2) {
		t.Error("Column('hello') Equals Column('hello') should be true")
	}
}

func TestColumnEqualsNullType(t *testing.T) {
	// Type=TYPE_NULL で両方 IsNull=false
	c1 := NewColumn(TYPE_NULL, nil)
	c1.IsNull = false
	c2 := NewColumn(TYPE_NULL, nil)
	c2.IsNull = false
	if !c1.Equals(c2) {
		t.Error("TYPE_NULL(false) Equals TYPE_NULL(false) should be true")
	}
}

func TestColumnEqualsUnknown(t *testing.T) {
	c1 := NewColumn(TYPE_UNKNOWN, nil)
	c2 := NewColumn(TYPE_UNKNOWN, nil)
	if c1.Equals(c2) {
		t.Error("TYPE_UNKNOWN Equals should be false")
	}
}

// ---- GreaterThan ----

func TestColumnGreaterThanNullLeft(t *testing.T) {
	c1 := NewColumn(TYPE_LONG, 10)
	c1.IsNull = true
	c2 := NewColumn(TYPE_LONG, 5)
	if c1.GreaterThan(c2) {
		t.Error("null GreaterThan should be false")
	}
}

func TestColumnGreaterThanNullRight(t *testing.T) {
	c1 := NewColumn(TYPE_LONG, 10)
	c2 := NewColumn(TYPE_LONG, 5)
	c2.IsNull = true
	if c1.GreaterThan(c2) {
		t.Error("GreaterThan null should be false")
	}
}

func TestColumnGreaterThanInt(t *testing.T) {
	c1 := NewColumn(TYPE_LONG, 10)
	c2 := NewColumn(TYPE_LONG, 5)
	if !c1.GreaterThan(c2) {
		t.Error("10 GreaterThan 5 should be true")
	}
	if c2.GreaterThan(c1) {
		t.Error("5 GreaterThan 10 should be false")
	}
}

func TestColumnGreaterThanDouble(t *testing.T) {
	c1 := NewColumn(TYPE_DOUBLE, 3.14)
	c2 := NewColumn(TYPE_DOUBLE, 2.71)
	if !c1.GreaterThan(c2) {
		t.Error("3.14 GreaterThan 2.71 should be true")
	}
}

func TestColumnGreaterThanDatetime(t *testing.T) {
	t1 := time.Unix(2000000, 0)
	t2 := time.Unix(1000000, 0)
	c1 := NewColumn(TYPE_DATETIME, t1)
	c2 := NewColumn(TYPE_DATETIME, t2)
	if !c1.GreaterThan(c2) {
		t.Error("TYPE_DATETIME newer GreaterThan older should be true")
	}
}

func TestColumnGreaterThanBlob(t *testing.T) {
	c1 := NewColumn(TYPE_BLOB, []byte("z"))
	c2 := NewColumn(TYPE_BLOB, []byte("a"))
	if !c1.GreaterThan(c2) {
		t.Error("blob 'z' GreaterThan blob 'a' should be true")
	}
}

func TestColumnGreaterThanString(t *testing.T) {
	c1 := NewColumn(TYPE_STRING, "z")
	c2 := NewColumn(TYPE_STRING, "a")
	if !c1.GreaterThan(c2) {
		t.Error("'z' GreaterThan 'a' should be true")
	}
}

func TestColumnGreaterThanUnknown(t *testing.T) {
	c1 := NewColumn(TYPE_UNKNOWN, nil)
	c2 := NewColumn(TYPE_UNKNOWN, nil)
	if c1.GreaterThan(c2) {
		t.Error("TYPE_UNKNOWN GreaterThan should be false")
	}
}

// ---- GreaterEquals ----

func TestColumnGreaterEqualsNullLeft(t *testing.T) {
	c1 := NewColumn(TYPE_LONG, 10)
	c1.IsNull = true
	c2 := NewColumn(TYPE_LONG, 5)
	if c1.GreaterEquals(c2) {
		t.Error("null GreaterEquals should be false")
	}
}

func TestColumnGreaterEqualsInt(t *testing.T) {
	c1 := NewColumn(TYPE_LONG, 10)
	c2 := NewColumn(TYPE_LONG, 10)
	if !c1.GreaterEquals(c2) {
		t.Error("10 GreaterEquals 10 should be true")
	}
	c3 := NewColumn(TYPE_LONG, 5)
	if c3.GreaterEquals(c1) {
		t.Error("5 GreaterEquals 10 should be false")
	}
}

func TestColumnGreaterEqualsDouble(t *testing.T) {
	c1 := NewColumn(TYPE_DOUBLE, 3.14)
	c2 := NewColumn(TYPE_DOUBLE, 3.14)
	if !c1.GreaterEquals(c2) {
		t.Error("3.14 GreaterEquals 3.14 should be true")
	}
}

func TestColumnGreaterEqualsDatetime(t *testing.T) {
	t1 := time.Unix(1000000, 0)
	c1 := NewColumn(TYPE_DATETIME, t1)
	c2 := NewColumn(TYPE_DATETIME, t1)
	if !c1.GreaterEquals(c2) {
		t.Error("same time GreaterEquals should be true")
	}
}

func TestColumnGreaterEqualsBlob(t *testing.T) {
	c1 := NewColumn(TYPE_BLOB, []byte("a"))
	c2 := NewColumn(TYPE_BLOB, []byte("a"))
	if !c1.GreaterEquals(c2) {
		t.Error("same blob GreaterEquals should be true")
	}
}

func TestColumnGreaterEqualsString(t *testing.T) {
	c1 := NewColumn(TYPE_STRING, "hello")
	c2 := NewColumn(TYPE_STRING, "hello")
	if !c1.GreaterEquals(c2) {
		t.Error("same string GreaterEquals should be true")
	}
}

func TestColumnGreaterEqualsNullType(t *testing.T) {
	// c.IsNull=false, c2.IsNull=false -> c.IsNull || (c2.IsNull == false) = false || true = true
	c1 := NewColumn(TYPE_NULL, nil)
	c1.IsNull = false
	c2 := NewColumn(TYPE_NULL, nil)
	c2.IsNull = false
	if !c1.GreaterEquals(c2) {
		t.Error("TYPE_NULL(false) GreaterEquals TYPE_NULL(false) should be true")
	}
}

func TestColumnGreaterEqualsUnknown(t *testing.T) {
	c1 := NewColumn(TYPE_UNKNOWN, nil)
	c2 := NewColumn(TYPE_UNKNOWN, nil)
	if c1.GreaterEquals(c2) {
		t.Error("TYPE_UNKNOWN GreaterEquals should be false")
	}
}

func TestColumnDoubleUnknownType(t *testing.T) {
	// TYPE_UNKNOWN は switch に case がないので最後の return c.double を返す
	c := NewColumn(TYPE_UNKNOWN, nil)
	if c.Double() != 0.0 {
		t.Errorf("TYPE_UNKNOWN Double() = %f, want 0.0", c.Double())
	}
}

func TestColumnGreaterThanNullTypeBothNonNull(t *testing.T) {
	// Type=TYPE_NULL, IsNull=false の場合は switch 内の case TYPE_NULL が実行される
	c1 := NewColumn(TYPE_NULL, nil)
	c1.IsNull = false
	c2 := NewColumn(TYPE_NULL, nil)
	c2.IsNull = false
	// c.IsNull && !c2.IsNull = false && true = false
	if c1.GreaterThan(c2) {
		t.Error("TYPE_NULL(false) GreaterThan TYPE_NULL(false) should be false")
	}
}

func TestColumnStringNewdecimal(t *testing.T) {
	// TYPE_NEWDECIMAL は str フィールドを返すが、NewColumn では time に保存してしまう
	// そのため str は空文字 -> String() は "" を返す
	c := NewColumn(TYPE_NEWDECIMAL, nil)
	_ = c.String() // パニックしないことを確認
}

// ---- IsRowsUpdateEvent ----

func TestIsRowsUpdateEvent(t *testing.T) {
	h := &BinlogEventHeader{EventType: BINLOG_EVENT_UPDATE_ROWSv1}
	if !h.IsRowsUpdateEvent() {
		t.Error("UPDATE_ROWSv1 should be IsRowsUpdateEvent")
	}
	h2 := &BinlogEventHeader{EventType: BINLOG_EVENT_UPDATE_ROWSv2}
	if !h2.IsRowsUpdateEvent() {
		t.Error("UPDATE_ROWSv2 should be IsRowsUpdateEvent")
	}
	h3 := &BinlogEventHeader{EventType: BINLOG_EVENT_WRITE_ROWSv1}
	if h3.IsRowsUpdateEvent() {
		t.Error("WRITE_ROWS should not be IsRowsUpdateEvent")
	}
}

// ---- ParseBinlogEvent 追加パス ----

func TestParseBinlogEventTooShort(t *testing.T) {
	p := &BinlogParser{TableMaps: map[uint64]*BinlogEventTableMap{}}
	_, _, err := p.ParseBinlogEvent([]byte{0x01, 0x02})
	if err == nil {
		t.Error("expected error for data < 19 bytes")
	}
}

func TestParseBinlogEventRowsFloatError(t *testing.T) {
	p := getParser(t)

	// 手動で TableMap を登録: TYPE_FLOAT column で meta=2 (invalid: 4 or 8 が正常)
	tableId := uint64(0x500)
	p.TableMaps[tableId] = &BinlogEventTableMap{
		TableId:         tableId,
		SchemaName:      "test",
		TableName:       "floaterr",
		ColumnCount:     1,
		ColumnTypes:     []byte{TYPE_FLOAT},
		ColumnMetas:     []int{2}, // meta=2 -> 不正な float サイズ
		NullableColumns: []bool{false},
	}

	// WRITE_ROWS_v2 パケット: tableId=0x500, 1列(FLOAT, meta=2)
	// Header 19 bytes + Payload 15 bytes = 34 bytes total
	packet := []byte{
		0x00, 0x00, 0x00, 0x00, // timestamp
		0x1e,                   // event type = WRITE_ROWS_v2
		0x01, 0x00, 0x00, 0x00, // server_id
		0x22, 0x00, 0x00, 0x00, // event_size = 34
		0x00, 0x00, 0x00, 0x00, // log_pos
		0x00, 0x00,             // flags
		// Payload
		0x00, 0x05, 0x00, 0x00, 0x00, 0x00, // tableId = 0x0500 (little endian 6 bytes)
		0x00, 0x00, // flags
		0x02, 0x00, // extra_data_len = 2 (no extra data)
		0x01,       // column_count = 1 (LES)
		0x01,       // columns_present_bitmap: bit0=1
		0x00,       // null_bitmap: not null
		0x00, 0x01, // 2 bytes of float data (size=2 -> triggers error)
	}
	_, _, err := p.ParseBinlogEvent(packet)
	if err == nil {
		t.Error("expected error for float column with invalid meta in WRITE_ROWS_v2")
	}
}

func TestParseBinlogEventUnknownType(t *testing.T) {
	p := getParser(t)
	// イベントタイプを不明な値に設定したパケット (type=0x55)
	packet := []byte{
		0xbb, 0x11, 0xc5, 0x57, // timestamp
		0x55,                   // event type (unknown)
		0x01, 0x00, 0x00, 0x00, // server_id
		0x13, 0x00, 0x00, 0x00, // event_size
		0x01, 0x00, 0x00, 0x00, // log_pos
		0x00, 0x00,             // flags
		0xAA, 0xBB,             // dummy payload
	}
	ev, _, err := p.ParseBinlogEvent(packet)
	if err != nil {
		t.Errorf("unknown event type should not error: %v", err)
	}
	if ev == nil {
		t.Error("expected non-nil event for unknown type")
	}
}
