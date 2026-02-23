package binlog

import (
	"testing"
)

// ---- MY_PACKED_TIME_MAKE / MY_PACKED_TIME_MAKE_INT ----

func TestMyPackedTimeMake(t *testing.T) {
	result := MY_PACKED_TIME_MAKE(5, 3)
	expected := uint64(5<<24) + 3
	if result != expected {
		t.Errorf("MY_PACKED_TIME_MAKE(5, 3) = %d, want %d", result, expected)
	}
	// ゼロケース
	result0 := MY_PACKED_TIME_MAKE(0, 0)
	if result0 != 0 {
		t.Errorf("MY_PACKED_TIME_MAKE(0, 0) = %d, want 0", result0)
	}
}

func TestMyPackedTimeMakeInt(t *testing.T) {
	result := MY_PACKED_TIME_MAKE_INT(7)
	expected := uint64(7 << 24)
	if result != expected {
		t.Errorf("MY_PACKED_TIME_MAKE_INT(7) = %d, want %d", result, expected)
	}
}

// ---- readPackedTimestamp ----

func makeTimestampBytes(unixSec uint32) []byte {
	// Big endian 4 bytes
	return []byte{
		byte(unixSec >> 24),
		byte(unixSec >> 16),
		byte(unixSec >> 8),
		byte(unixSec),
	}
}

func TestReadPackedTimestampMeta0(t *testing.T) {
	// meta=0: フラクションなし、4バイトのみ
	data := makeTimestampBytes(1000000)
	ts, size := readPackedTimestamp(data, 0)
	if size != 4 {
		t.Errorf("meta=0 size = %d, want 4", size)
	}
	if ts.Unix() != 1000000 {
		t.Errorf("meta=0 Unix() = %d, want 1000000", ts.Unix())
	}
}

func TestReadPackedTimestampMeta1(t *testing.T) {
	// meta=1/2: 4バイト + 1バイトのフラクション
	data := append(makeTimestampBytes(1000000), 10)
	ts, size := readPackedTimestamp(data, 1)
	if size != 5 {
		t.Errorf("meta=1 size = %d, want 5", size)
	}
	_ = ts
}

func TestReadPackedTimestampMeta2(t *testing.T) {
	data := append(makeTimestampBytes(1000000), 20)
	_, size := readPackedTimestamp(data, 2)
	if size != 5 {
		t.Errorf("meta=2 size = %d, want 5", size)
	}
}

func TestReadPackedTimestampMeta3(t *testing.T) {
	// meta=3/4: 4バイト + 2バイトのフラクション
	data := append(makeTimestampBytes(1000000), 0x03, 0xE8)
	_, size := readPackedTimestamp(data, 3)
	if size != 6 {
		t.Errorf("meta=3 size = %d, want 6", size)
	}
}

func TestReadPackedTimestampMeta4(t *testing.T) {
	data := append(makeTimestampBytes(1000000), 0x00, 0x64)
	_, size := readPackedTimestamp(data, 4)
	if size != 6 {
		t.Errorf("meta=4 size = %d, want 6", size)
	}
}

func TestReadPackedTimestampMeta5(t *testing.T) {
	// meta=5/6: 4バイト + 3バイトのフラクション
	data := append(makeTimestampBytes(1000000), 0x00, 0x00, 0x64)
	_, size := readPackedTimestamp(data, 5)
	if size != 7 {
		t.Errorf("meta=5 size = %d, want 7", size)
	}
}

func TestReadPackedTimestampMeta6(t *testing.T) {
	data := append(makeTimestampBytes(1000000), 0x00, 0x03, 0xE8)
	_, size := readPackedTimestamp(data, 6)
	if size != 7 {
		t.Errorf("meta=6 size = %d, want 7", size)
	}
}

// ---- readPackedDatetime ----

// DATETIMEF_INT_OFS = 0x8000000000
// meta=0 には 0x8000000000 をデータに入れると ltime=0
func makeDatetimeBytes(ltime uint64) []byte {
	offset := uint64(DATETIMEF_INT_OFS)
	val := ltime + offset
	return []byte{
		byte(val >> 32),
		byte(val >> 24),
		byte(val >> 16),
		byte(val >> 8),
		byte(val),
	}
}

func TestReadPackedDatetimeMeta0(t *testing.T) {
	data := makeDatetimeBytes(0)
	result, size := readPackedDatetime(data, 0)
	if size != 5 {
		t.Errorf("meta=0 size = %d, want 5", size)
	}
	// MY_PACKED_TIME_MAKE_INT(0) = 0
	if result != 0 {
		t.Errorf("meta=0 result = %d, want 0", result)
	}
}

func TestReadPackedDatetimeMeta1(t *testing.T) {
	data := append(makeDatetimeBytes(0), 0x05)
	_, size := readPackedDatetime(data, 1)
	if size != 6 {
		t.Errorf("meta=1 size = %d, want 6", size)
	}
}

func TestReadPackedDatetimeMeta2(t *testing.T) {
	data := append(makeDatetimeBytes(0), 0x0A)
	_, size := readPackedDatetime(data, 2)
	if size != 6 {
		t.Errorf("meta=2 size = %d, want 6", size)
	}
}

func TestReadPackedDatetimeMeta3(t *testing.T) {
	data := append(makeDatetimeBytes(0), 0x00, 0x64)
	_, size := readPackedDatetime(data, 3)
	if size != 7 {
		t.Errorf("meta=3 size = %d, want 7", size)
	}
}

func TestReadPackedDatetimeMeta4(t *testing.T) {
	data := append(makeDatetimeBytes(0), 0x03, 0xE8)
	_, size := readPackedDatetime(data, 4)
	if size != 7 {
		t.Errorf("meta=4 size = %d, want 7", size)
	}
}

func TestReadPackedDatetimeMeta5(t *testing.T) {
	data := append(makeDatetimeBytes(0), 0x00, 0x00, 0x64)
	_, size := readPackedDatetime(data, 5)
	if size != 8 {
		t.Errorf("meta=5 size = %d, want 8", size)
	}
}

func TestReadPackedDatetimeMeta6(t *testing.T) {
	data := append(makeDatetimeBytes(0), 0x00, 0x03, 0xE8)
	_, size := readPackedDatetime(data, 6)
	if size != 8 {
		t.Errorf("meta=6 size = %d, want 8", size)
	}
}

// ---- readPackedTime ----

// TIMEF_INT_OFS = 0x800000
func makeTimeBytes3(ltime int64) []byte {
	offset := int64(TIMEF_INT_OFS)
	val := uint64(ltime + offset)
	return []byte{
		byte(val >> 16),
		byte(val >> 8),
		byte(val),
	}
}

func TestReadPackedTimeMeta0(t *testing.T) {
	// meta=0 (default): 3バイト
	data := makeTimeBytes3(0)
	_, size := readPackedTime(data, 0)
	if size != 3 {
		t.Errorf("meta=0 size = %d, want 3", size)
	}
}

func TestReadPackedTimeMeta1(t *testing.T) {
	// meta=1/2: 3+1=4バイト、ltime >= 0, frac=0
	data := append(makeTimeBytes3(100), 0x00)
	_, size := readPackedTime(data, 1)
	if size != 4 {
		t.Errorf("meta=1 size = %d, want 4", size)
	}
}

func TestReadPackedTimeMeta1NegativeLtime(t *testing.T) {
	// meta=1 with ltime < 0 and frac != 0 -> 補正処理が実行される
	// ltime < 0 にするには readBigEndianVarint64(data[:3]) < TIMEF_INT_OFS
	// {0,0,0} -> val=0 -> 0 - 0x800000 = -8388608 < 0
	data := []byte{0x00, 0x00, 0x00, 0x01} // ltime=-8388608, frac=1
	_, size := readPackedTime(data, 1)
	if size != 4 {
		t.Errorf("meta=1 negative ltime size = %d, want 4", size)
	}
}

func TestReadPackedTimeMeta2(t *testing.T) {
	data := append(makeTimeBytes3(0), 0x0A)
	_, size := readPackedTime(data, 2)
	if size != 4 {
		t.Errorf("meta=2 size = %d, want 4", size)
	}
}

func TestReadPackedTimeMeta3(t *testing.T) {
	// meta=3/4: 3+2=5バイト
	data := append(makeTimeBytes3(100), 0x00, 0x64)
	_, size := readPackedTime(data, 3)
	if size != 5 {
		t.Errorf("meta=3 size = %d, want 5", size)
	}
}

func TestReadPackedTimeMeta3NegativeLtime(t *testing.T) {
	// ltime < 0, frac != 0 -> 補正処理
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x01} // ltime < 0, frac=1
	_, size := readPackedTime(data, 3)
	if size != 5 {
		t.Errorf("meta=3 negative size = %d, want 5", size)
	}
}

func TestReadPackedTimeMeta4(t *testing.T) {
	data := append(makeTimeBytes3(0), 0x03, 0xE8)
	_, size := readPackedTime(data, 4)
	if size != 5 {
		t.Errorf("meta=4 size = %d, want 5", size)
	}
}

func TestReadPackedTimeMeta5(t *testing.T) {
	// meta=5/6: 6バイト
	data := []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00} // 6バイト
	_, size := readPackedTime(data, 5)
	if size != 6 {
		t.Errorf("meta=5 size = %d, want 6", size)
	}
}

func TestReadPackedTimeMeta6(t *testing.T) {
	data := []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00}
	_, size := readPackedTime(data, 6)
	if size != 6 {
		t.Errorf("meta=6 size = %d, want 6", size)
	}
}

// ---- readBigEndianUvarint64 ----

func TestReadBigEndianUvarint64(t *testing.T) {
	// {0x01, 0x02} -> big endian: 0x0102 = 258
	val, _ := readBigEndianUvarint64([]byte{0x01, 0x02})
	if val != 0x0102 {
		t.Errorf("readBigEndianUvarint64 = 0x%X, want 0x0102", val)
	}

	// 1バイト
	val2, _ := readBigEndianUvarint64([]byte{0xFF})
	if val2 != 0xFF {
		t.Errorf("readBigEndianUvarint64 single byte = 0x%X, want 0xFF", val2)
	}
}

// ---- convertMysqllonglongToDate ----

func TestConvertMysqllonglongToDate(t *testing.T) {
	// year=2020, month=1, day=15
	// ltime = (year << 9) | (month << 5) | day
	// = (2020 << 9) | (1 << 5) | 15
	ltime := uint64((2020 << 9) | (1 << 5) | 15)
	d := convertMysqllonglongToDate(ltime)
	if d.Year() != 2020 || d.Month() != 1 || d.Day() != 15 {
		t.Errorf("convertMysqllonglongToDate = %v, want 2020-01-15", d)
	}
}
