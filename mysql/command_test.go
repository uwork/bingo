package mysql

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"testing"

	"github.com/uwork/bingo/mysql/binlog"
)

// ---- ヘルパー ----

// Length-Encoded String を構築
func makeLES(s string) []byte {
	b := []byte{byte(len(s))}
	return append(b, []byte(s)...)
}

// カラム定義パケットのバイナリを構築
func makeColDefPayload(name string) []byte {
	var data []byte
	data = append(data, makeLES("def")...)    // catalog
	data = append(data, makeLES("")...)        // schema
	data = append(data, makeLES("test")...)    // table
	data = append(data, makeLES("test")...)    // orgTable
	data = append(data, makeLES(name)...)      // name
	data = append(data, 0x0C)                  // filter1
	data = append(data, 0x21, 0x00)            // charset utf8
	data = append(data, 0x0B, 0x00, 0x00, 0x00) // colLen = 11
	data = append(data, 0x03)                  // colType = LONG
	data = append(data, 0x00, 0x00)            // flags
	data = append(data, 0x00)                  // decimals
	data = append(data, 0x00, 0x00)            // filter2
	return data
}

// 複数のパケットを buf に連続して書き込む
func writePackets(c *Conn, payloads ...[]byte) *bytes.Buffer {
	buf := &bytes.Buffer{}
	for _, p := range payloads {
		n := len(p)
		buf.Write([]byte{byte(n), byte(n >> 8), byte(n >> 16), byte(c.sequence)})
		buf.Write(p)
		c.sequence++
	}
	return buf
}

// ---- BinlogEOFError ----

func TestBinlogEOFError(t *testing.T) {
	e := &BinlogEOFError{}
	if e.Error() != "End of binlog stream." {
		t.Errorf("Error() = %q, want 'End of binlog stream.'", e.Error())
	}
}

// ---- dumpNextBinlog ----

func TestDumpNextBinlogOK(t *testing.T) {
	// pOK (0x00) + FORMAT_DESCRIPTION_EVENT データ
	eventData := []byte{
		0xbb, 0x11, 0xc5, 0x57, 0x0f, 0x01, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00,
		0x7b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x35, 0x2e, 0x37, 0x2e, 0x31,
		0x34, 0x2d, 0x6c, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13, 0x38, 0x0d, 0x00, 0x08, 0x00,
		0x12, 0x00, 0x04, 0x04, 0x04, 0x04, 0x12, 0x00, 0x00, 0x5f, 0x00, 0x04, 0x1a,
		0x08, 0x00, 0x00, 0x00, 0x08, 0x08, 0x08, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x0a,
		0x0a, 0x2a, 0x2a, 0x00, 0x12, 0x34, 0x00, 0x00, 0x1c, 0x4e, 0x06, 0xf8,
	}
	payload := append([]byte{0x00}, eventData...)

	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, payload)
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	c.binlogParser = &binlog.BinlogParser{TableMaps: map[uint64]*binlog.BinlogEventTableMap{}}

	ev, err := c.dumpNextBinlog()
	if err != nil {
		t.Fatal(err)
	}
	if ev == nil {
		t.Error("expected non-nil event")
	}
}

func TestDumpNextBinlogERR(t *testing.T) {
	errPayload := append([]byte{pERR, 0x0A, 0x00}, []byte("binlog error")...)
	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, errPayload)
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	c.binlogParser = &binlog.BinlogParser{TableMaps: map[uint64]*binlog.BinlogEventTableMap{}}

	_, err := c.dumpNextBinlog()
	if err == nil {
		t.Error("expected error for ERR packet")
	}
}

func TestDumpNextBinlogEOF(t *testing.T) {
	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, []byte{pEOF})
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	c.binlogParser = &binlog.BinlogParser{TableMaps: map[uint64]*binlog.BinlogEventTableMap{}}

	_, err := c.dumpNextBinlog()
	if err == nil {
		t.Error("expected BinlogEOFError")
	}
	if _, ok := err.(*BinlogEOFError); !ok {
		t.Errorf("expected *BinlogEOFError, got %T: %v", err, err)
	}
}

// ---- readColumnDefinition ----

func TestReadColumnDefinition(t *testing.T) {
	data := makeColDefPayload("id")
	c := &Conn{}
	col, pos, err := c.readColumnDefinition(data)
	if err != nil {
		t.Fatal(err)
	}
	if col.catalog != "def" {
		t.Errorf("catalog = %q, want 'def'", col.catalog)
	}
	if col.name != "id" {
		t.Errorf("name = %q, want 'id'", col.name)
	}
	if pos == 0 {
		t.Error("pos should be > 0")
	}
}

// ---- readResultSetPacket ----

func TestReadResultSetPacketOK(t *testing.T) {
	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, []byte{pOK})
	c.sequence = 0
	c.r = bufio.NewReader(buf)

	rs, err := c.readResultSetPacket()
	if err != nil {
		t.Fatal(err)
	}
	if rs != nil {
		t.Error("expected nil rs for OK packet")
	}
}

func TestReadResultSetPacketERR(t *testing.T) {
	errPayload := append([]byte{pERR, 0x0A, 0x00}, []byte("query error")...)
	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, errPayload)
	c.sequence = 0
	c.r = bufio.NewReader(buf)

	_, err := c.readResultSetPacket()
	if err == nil {
		t.Error("expected error for ERR packet")
	}
}

func TestReadResultSetPacketData(t *testing.T) {
	// 1カラム、1行の結果セット
	// Packet 1: column count = 1
	// Packet 2: column definition
	// Packet 3: EOF (end of column definitions)
	// Packet 4: EOF (end of rows, no more results)
	colDef := makeColDefPayload("id")
	eofPacket := []byte{pEOF, 0x00, 0x00, 0x00, 0x00}

	// 行データ: value "42" (length=2, "42")
	rowPacket := append([]byte{0x02}, []byte("42")...)

	// 行の最後の EOF (i=0, columns=1 -> i != columns -> check status -> isMoreResults=false)
	rowEOF := []byte{pEOF, 0x00, 0x00, 0x00, 0x00}

	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, []byte{0x01}, colDef, eofPacket, rowPacket, rowEOF)
	c.sequence = 0
	c.r = bufio.NewReader(buf)

	rs, err := c.readResultSetPacket()
	if err != nil {
		t.Fatal(err)
	}
	if rs == nil {
		t.Fatal("expected non-nil result set")
	}
	if len(rs.Columns) != 1 {
		t.Errorf("expected 1 column, got %d", len(rs.Columns))
	}
	if len(rs.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rs.Rows))
	}
	if rs.Rows[0].Values[0].Value != "42" {
		t.Errorf("value = %q, want '42'", rs.Rows[0].Values[0].Value)
	}
}

func TestReadResultSetPacketNullValue(t *testing.T) {
	// NULL 値 (0xfb) を含む行
	colDef := makeColDefPayload("id")
	eofPacket := []byte{pEOF, 0x00, 0x00, 0x00, 0x00}
	rowPacket := []byte{0xfb} // NULL value
	rowEOF := []byte{pEOF, 0x00, 0x00, 0x00, 0x00}

	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, []byte{0x01}, colDef, eofPacket, rowPacket, rowEOF)
	c.sequence = 0
	c.r = bufio.NewReader(buf)

	rs, err := c.readResultSetPacket()
	if err != nil {
		t.Fatal(err)
	}
	if rs == nil {
		t.Fatal("expected non-nil result set")
	}
	if len(rs.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rs.Rows))
	}
	if !rs.Rows[0].Values[0].IsNull {
		t.Error("expected null value")
	}
}

func TestReadResultSetPacketColDefERR(t *testing.T) {
	// カラム定義読み込み中に ERR パケット
	errPayload := append([]byte{pERR, 0x0A, 0x00}, []byte("col def error")...)

	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, []byte{0x01}, errPayload)
	c.sequence = 0
	c.r = bufio.NewReader(buf)

	_, err := c.readResultSetPacket()
	if err == nil {
		t.Error("expected error for ERR in column definition")
	}
}

func TestReadResultSetPacketInvalidColCount(t *testing.T) {
	// カラム定義が途中でEOFになる (カラム数不一致)
	eofPacket := []byte{pEOF, 0x00, 0x00, 0x00, 0x00}

	c := &Conn{}
	c.sequence = 0
	// 2カラム宣言しているが、EOF だけ来る
	buf := writePackets(c, []byte{0x02}, eofPacket)
	c.sequence = 0
	c.r = bufio.NewReader(buf)

	_, err := c.readResultSetPacket()
	if err == nil {
		t.Error("expected error for column count mismatch")
	}
}

// ---- UpdateQuery ----

func TestUpdateQuery(t *testing.T) {
	// OK レスポンスを受信する
	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, []byte{pOK})
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	wbuf := &bytes.Buffer{}
	c.w = bufio.NewWriter(wbuf)

	err := c.UpdateQuery("DELETE FROM test WHERE id=1")
	if err != nil {
		t.Fatal(err)
	}
}

func TestUpdateQueryERR(t *testing.T) {
	errPayload := append([]byte{pERR, 0x0A, 0x00}, []byte("update error")...)
	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, errPayload)
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	wbuf := &bytes.Buffer{}
	c.w = bufio.NewWriter(wbuf)

	err := c.UpdateQuery("DELETE FROM test")
	if err == nil {
		t.Error("expected error")
	}
}

// ---- Quit ----

func TestQuit(t *testing.T) {
	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, []byte{pOK})
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	wbuf := &bytes.Buffer{}
	c.w = bufio.NewWriter(wbuf)

	err := c.Quit()
	if err != nil {
		t.Fatal(err)
	}
}

// ---- Query ----

func TestQueryOK(t *testing.T) {
	// Query に OK パケット -> nil ResultSet, nil error
	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, []byte{pOK})
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	wbuf := &bytes.Buffer{}
	c.w = bufio.NewWriter(wbuf)

	rs, err := c.Query("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if rs != nil {
		t.Error("expected nil rs for OK response")
	}
}

func TestReadResultSetPacketRowERR(t *testing.T) {
	// 行データが pERR で始まる場合
	colDef := makeColDefPayload("id")
	eofPacket := []byte{pEOF, 0x00, 0x00, 0x00, 0x00}
	// 行の代わりに ERR パケット
	errPayload := append([]byte{pERR, 0x0A, 0x00}, []byte("row error")...)

	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, []byte{0x01}, colDef, eofPacket, errPayload)
	c.sequence = 0
	c.r = bufio.NewReader(buf)

	_, err := c.readResultSetPacket()
	if err == nil {
		t.Error("expected error for ERR in row data")
	}
}

func TestQuitWriteError(t *testing.T) {
	// 書き込みエラーのパス: reader に何もない場合 commandSimple 後の readResultPacket が失敗
	c := &Conn{}
	c.sequence = 0
	// 空のバッファ → readPacket が io.EOF でエラー
	buf := bytes.NewBuffer([]byte{})
	c.r = bufio.NewReader(buf)
	wbuf := &bytes.Buffer{}
	c.w = bufio.NewWriter(wbuf)

	err := c.Quit()
	if err == nil {
		t.Error("expected error when no response data")
	}
}

func TestUpdateQueryWriteError(t *testing.T) {
	// 空バッファ → readResultPacket が失敗
	c := &Conn{}
	c.sequence = 0
	buf := bytes.NewBuffer([]byte{})
	c.r = bufio.NewReader(buf)
	wbuf := &bytes.Buffer{}
	c.w = bufio.NewWriter(wbuf)

	err := c.UpdateQuery("DELETE FROM test")
	if err == nil {
		t.Error("expected error when no response data")
	}
}

func TestQueryERR(t *testing.T) {
	errPayload := append([]byte{pERR, 0x0A, 0x00}, []byte("query error")...)
	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, errPayload)
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	wbuf := &bytes.Buffer{}
	c.w = bufio.NewWriter(wbuf)

	_, err := c.Query("SELECT 1")
	if err == nil {
		t.Error("expected error for ERR packet")
	}
}

// ---- コマンド書き込みエラーパス (errWriter) ----

type errIoWriter struct{}

func (e *errIoWriter) Write(p []byte) (n int, err error) {
	return 0, bufio.ErrBufferFull
}

func TestQueryCommandWriteError(t *testing.T) {
	// command() が失敗する場合 -> Query が最初の return err を返す
	c := &Conn{}
	c.r = bufio.NewReader(bytes.NewBuffer([]byte{}))
	c.w = bufio.NewWriter(&errIoWriter{})

	_, err := c.Query("SELECT 1")
	if err == nil {
		t.Error("expected write error from Query")
	}
}

func TestUpdateQueryCommandWriteError(t *testing.T) {
	// command() 失敗 -> UpdateQuery が最初の return err
	c := &Conn{}
	c.r = bufio.NewReader(bytes.NewBuffer([]byte{}))
	c.w = bufio.NewWriter(&errIoWriter{})

	err := c.UpdateQuery("DELETE FROM test")
	if err == nil {
		t.Error("expected write error from UpdateQuery")
	}
}

func TestQuitCommandWriteError(t *testing.T) {
	// commandSimple() 失敗 -> Quit が最初の return err
	c := &Conn{}
	c.r = bufio.NewReader(bytes.NewBuffer([]byte{}))
	c.w = bufio.NewWriter(&errIoWriter{})

	err := c.Quit()
	if err == nil {
		t.Error("expected write error from Quit")
	}
}

func TestDumpBinlogCommandWriteError(t *testing.T) {
	// commandBinary() 失敗 -> DumpBinlog が最初の return err
	c := &Conn{}
	c.r = bufio.NewReader(bytes.NewBuffer([]byte{}))
	c.w = bufio.NewWriter(&errIoWriter{})

	err := c.DumpBinlog("mysql-bin.000001", 4, func(ev *binlog.BinlogEvent) error {
		return nil
	})
	if err == nil {
		t.Error("expected write error from DumpBinlog")
	}
}

func TestDumpBinlogResultPacketError(t *testing.T) {
	// commandBinary は成功 (c.w は本物のバッファ)、readResultPacket に ERR パケット
	c := &Conn{}
	c.sequence = 0
	errPayload := append([]byte{pERR, 0x0A, 0x00}, []byte("result packet error")...)
	buf := writePackets(c, errPayload)
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	c.w = bufio.NewWriter(&bytes.Buffer{})
	c.binlogParser = &binlog.BinlogParser{TableMaps: map[uint64]*binlog.BinlogEventTableMap{}}

	err := c.DumpBinlog("mysql-bin.000001", 4, func(ev *binlog.BinlogEvent) error { return nil })
	if err == nil {
		t.Error("expected error from readResultPacket in DumpBinlog")
	}
}

func TestDumpBinlogFirstEventError(t *testing.T) {
	// readResultPacket は pOK で成功、first dumpNextBinlog は ERR でエラー
	c := &Conn{}
	c.sequence = 0
	okPayload := []byte{pOK}
	errPayload := append([]byte{pERR, 0x0A, 0x00}, []byte("first event error")...)
	buf := writePackets(c, okPayload, errPayload)
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	c.w = bufio.NewWriter(&bytes.Buffer{})
	c.binlogParser = &binlog.BinlogParser{TableMaps: map[uint64]*binlog.BinlogEventTableMap{}}

	err := c.DumpBinlog("mysql-bin.000001", 4, func(ev *binlog.BinlogEvent) error { return nil })
	if err == nil {
		t.Error("expected error from first dumpNextBinlog in DumpBinlog")
	}
}

func TestDumpBinlogMainLoop(t *testing.T) {
	// readResultPacket → pOK
	// first dumpNextBinlog → FORMAT_DESCRIPTION event (成功)
	// main loop dumpNextBinlog → pEOF → BinlogEOFError (ループ終了)
	eventData := []byte{
		0xbb, 0x11, 0xc5, 0x57, 0x0f, 0x01, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00,
		0x7b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x35, 0x2e, 0x37, 0x2e, 0x31,
		0x34, 0x2d, 0x6c, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13, 0x38, 0x0d, 0x00, 0x08, 0x00,
		0x12, 0x00, 0x04, 0x04, 0x04, 0x04, 0x12, 0x00, 0x00, 0x5f, 0x00, 0x04, 0x1a,
		0x08, 0x00, 0x00, 0x00, 0x08, 0x08, 0x08, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x0a,
		0x0a, 0x2a, 0x2a, 0x00, 0x12, 0x34, 0x00, 0x00, 0x1c, 0x4e, 0x06, 0xf8,
	}

	c := &Conn{}
	c.sequence = 0
	payload1 := []byte{pOK}                           // readResultPacket → 成功
	payload2 := append([]byte{0x00}, eventData...)    // first dumpNextBinlog → FORMAT_DESCRIPTION
	payload3 := []byte{pEOF}                          // main loop → BinlogEOFError
	buf := writePackets(c, payload1, payload2, payload3)
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	c.w = bufio.NewWriter(&bytes.Buffer{})
	c.binlogParser = &binlog.BinlogParser{TableMaps: map[uint64]*binlog.BinlogEventTableMap{}}

	// c.nc が必要 (SetDeadline 用)
	client, server := net.Pipe()
	c.nc = client
	defer client.Close()
	defer server.Close()

	err := c.DumpBinlog("mysql-bin.000001", 4, func(ev *binlog.BinlogEvent) error { return nil })
	if err == nil {
		t.Error("expected BinlogEOFError from main loop")
	}
	if _, ok := err.(*BinlogEOFError); !ok {
		t.Errorf("expected *BinlogEOFError, got %T: %v", err, err)
	}
}

func TestDumpNextBinlogReadError(t *testing.T) {
	// 空のリーダー → readPacket が io.EOF → dumpNextBinlog がエラー
	c := &Conn{}
	c.r = bufio.NewReader(bytes.NewBuffer([]byte{}))
	c.binlogParser = &binlog.BinlogParser{TableMaps: map[uint64]*binlog.BinlogEventTableMap{}}

	_, err := c.dumpNextBinlog()
	if err == nil {
		t.Error("expected read error for empty reader")
	}
}

func TestDumpNextBinlogParseError(t *testing.T) {
	// pOK + 5 バイト → data[1:] が 5 バイト < 19 → ParseBinlogEvent がエラー
	c := &Conn{}
	c.sequence = 0
	payload := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05} // pOK + 5 data bytes
	buf := writePackets(c, payload)
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	c.binlogParser = &binlog.BinlogParser{TableMaps: map[uint64]*binlog.BinlogEventTableMap{}}

	_, err := c.dumpNextBinlog()
	if err == nil {
		t.Error("expected parse error for short event data")
	}
}

func TestDumpBinlogCallbackError(t *testing.T) {
	// main loop で dumpNextBinlog がイベントを返し、callback がエラーを返す
	eventData := []byte{
		0xbb, 0x11, 0xc5, 0x57, 0x0f, 0x01, 0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00,
		0x7b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x35, 0x2e, 0x37, 0x2e, 0x31,
		0x34, 0x2d, 0x6c, 0x6f, 0x67, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x13, 0x38, 0x0d, 0x00, 0x08, 0x00,
		0x12, 0x00, 0x04, 0x04, 0x04, 0x04, 0x12, 0x00, 0x00, 0x5f, 0x00, 0x04, 0x1a,
		0x08, 0x00, 0x00, 0x00, 0x08, 0x08, 0x08, 0x02, 0x00, 0x00, 0x00, 0x0a, 0x0a,
		0x0a, 0x2a, 0x2a, 0x00, 0x12, 0x34, 0x00, 0x00, 0x1c, 0x4e, 0x06, 0xf8,
	}
	// ROTATE_EVENT (type=0x04): 最小ヘッダー 19 バイト、unknown type → ParseBinlogEvent は nil を返す
	rotateEvent := []byte{
		0x00, 0x00, 0x00, 0x00, // timestamp
		0x04,                   // event_type = ROTATE_EVENT
		0x01, 0x00, 0x00, 0x00, // server_id
		0x13, 0x00, 0x00, 0x00, // event_length = 19
		0x00, 0x00, 0x00, 0x00, // log_pos
		0x00, 0x00,             // flags
	}

	c := &Conn{}
	c.sequence = 0
	payload1 := []byte{pOK}
	payload2 := append([]byte{0x00}, eventData...)   // FORMAT_DESCRIPTION
	payload3 := append([]byte{0x00}, rotateEvent...) // ROTATE (main loop event)
	buf := writePackets(c, payload1, payload2, payload3)
	c.sequence = 0
	c.r = bufio.NewReader(buf)
	c.w = bufio.NewWriter(&bytes.Buffer{})
	c.binlogParser = &binlog.BinlogParser{TableMaps: map[uint64]*binlog.BinlogEventTableMap{}}

	client, server := net.Pipe()
	c.nc = client
	defer client.Close()
	defer server.Close()

	callbackErr := fmt.Errorf("callback failed")
	err := c.DumpBinlog("mysql-bin.000001", 4, func(ev *binlog.BinlogEvent) error {
		return callbackErr
	})
	if err != callbackErr {
		t.Errorf("expected callbackErr, got %v", err)
	}
}

func TestQueryReadError(t *testing.T) {
	// command() が成功するが readResultSetPacket の最初の readPacket が空でエラー
	c := &Conn{}
	c.r = bufio.NewReader(bytes.NewBuffer([]byte{}))
	c.w = bufio.NewWriter(&bytes.Buffer{})

	_, err := c.Query("SELECT 1")
	if err == nil {
		t.Error("expected read error from Query")
	}
}

func TestReadResultSetPacketColDefReadError(t *testing.T) {
	// colCount=1 を受信後、カラム定義の readPacket が失敗する
	c := &Conn{}
	c.sequence = 0
	buf := writePackets(c, []byte{0x01}) // colCount=1 のみ送信
	c.sequence = 0
	c.r = bufio.NewReader(buf)

	_, err := c.readResultSetPacket()
	if err == nil {
		t.Error("expected read error in column definition loop")
	}
}

func TestReadResultSetPacketRowReadError(t *testing.T) {
	// カラム定義後、行データの readPacket が失敗する
	colDef := makeColDefPayload("id")
	eofPacket := []byte{pEOF, 0x00, 0x00, 0x00, 0x00}

	c := &Conn{}
	c.sequence = 0
	// colCount=1, colDef (i=0), EOF (i=1 == columns → break), その後データなし → 行の readPacket 失敗
	buf := writePackets(c, []byte{0x01}, colDef, eofPacket)
	c.sequence = 0
	c.r = bufio.NewReader(buf)

	_, err := c.readResultSetPacket()
	if err == nil {
		t.Error("expected read error in row data loop")
	}
}
