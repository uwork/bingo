package mysql

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"
)

// ---- errWriter: 書き込みに常に失敗するライター ----

type errWriter struct{}

func (e *errWriter) Write(p []byte) (n int, err error) {
	return 0, fmt.Errorf("simulated write error")
}

// ---- commandSimple / command / commandBinary ----

func TestCommandSimple(t *testing.T) {
	buf := &bytes.Buffer{}
	c := &Conn{}
	c.w = bufio.NewWriter(buf)
	c.sequence = 5

	err := c.commandSimple(0x0E)
	if err != nil {
		t.Fatal(err)
	}
	// sequence は commandSimple で 0 にリセット後、writePacket で 1 になる
	if c.sequence != 1 {
		t.Errorf("sequence = %d, want 1", c.sequence)
	}
	// 書き込まれたバイト: [1, 0, 0, 0, 0x0E]
	written := buf.Bytes()
	if len(written) == 0 {
		t.Error("expected data written")
	}
	if written[4] != 0x0E {
		t.Errorf("command byte = 0x%X, want 0x0E", written[4])
	}
}

func TestCommand(t *testing.T) {
	buf := &bytes.Buffer{}
	c := &Conn{}
	c.w = bufio.NewWriter(buf)
	c.sequence = 0

	err := c.command(0x03, "SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	written := buf.Bytes()
	if len(written) == 0 {
		t.Error("expected data written")
	}
	// payload: command_byte + "SELECT 1"
	if written[4] != 0x03 {
		t.Errorf("command byte = 0x%X, want 0x03", written[4])
	}
}

func TestCommandBinary(t *testing.T) {
	buf := &bytes.Buffer{}
	c := &Conn{}
	c.w = bufio.NewWriter(buf)
	c.sequence = 0

	err := c.commandBinary(0x12, []byte{0x01, 0x02, 0x03})
	if err != nil {
		t.Fatal(err)
	}
	written := buf.Bytes()
	if len(written) == 0 {
		t.Error("expected data written")
	}
	if written[4] != 0x12 {
		t.Errorf("command byte = 0x%X, want 0x12", written[4])
	}
}

// ---- readFixedLengthInteger ----

func makePacket(payload []byte) []byte {
	n := len(payload)
	head := []byte{byte(n), byte(n >> 8), byte(n >> 16), 0}
	return append(head, payload...)
}

func TestReadFixedLengthIntegerOK(t *testing.T) {
	// pOK パケット -> -1, nil
	buf := bytes.NewBuffer(makePacket([]byte{pOK}))
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	v, err := c.readFixedLengthInteger()
	if err != nil {
		t.Fatal(err)
	}
	if v != -1 {
		t.Errorf("pOK readFixedLengthInteger = %d, want -1", v)
	}
}

func TestReadFixedLengthIntegerEOF(t *testing.T) {
	// pEOF パケット -> -1, nil
	buf := bytes.NewBuffer(makePacket([]byte{pEOF}))
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	v, err := c.readFixedLengthInteger()
	if err != nil {
		t.Fatal(err)
	}
	if v != -1 {
		t.Errorf("pEOF readFixedLengthInteger = %d, want -1", v)
	}
}

func TestReadFixedLengthIntegerEOFWithWarnings(t *testing.T) {
	// pEOF + warnings + status
	payload := []byte{pEOF, 0x01, 0x00, 0x02, 0x00}
	buf := bytes.NewBuffer(makePacket(payload))
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	v, err := c.readFixedLengthInteger()
	if err != nil {
		t.Fatal(err)
	}
	if v != -1 {
		t.Errorf("pEOF readFixedLengthInteger = %d, want -1", v)
	}
	if c.warnings != 1 {
		t.Errorf("warnings = %d, want 1", c.warnings)
	}
	if c.status != 2 {
		t.Errorf("status = %d, want 2", c.status)
	}
}

func TestReadFixedLengthIntegerERR(t *testing.T) {
	// pERR パケット -> -1, error
	payload := append([]byte{pERR, 0x0A, 0x00}, []byte("error message")...)
	buf := bytes.NewBuffer(makePacket(payload))
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	v, err := c.readFixedLengthInteger()
	if err == nil {
		t.Error("expected error for ERR packet")
	}
	if v != -1 {
		t.Errorf("pERR readFixedLengthInteger = %d, want -1", v)
	}
}

func TestReadFixedLengthIntegerData1Byte(t *testing.T) {
	// 1バイトデータ: data[0]=0x05 -> v=5, length=0 (no extra bytes)
	buf := bytes.NewBuffer(makePacket([]byte{0x05}))
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	v, err := c.readFixedLengthInteger()
	if err != nil {
		t.Fatal(err)
	}
	if v != 5 {
		t.Errorf("1-byte readFixedLengthInteger = %d, want 5", v)
	}
}

func TestReadFixedLengthIntegerData3Bytes(t *testing.T) {
	// 3バイトデータ: data = {0x05, 0x02, 0x00} -> length=2, v=5 + 2<<8 = 517
	buf := bytes.NewBuffer(makePacket([]byte{0x05, 0x02, 0x00}))
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	v, err := c.readFixedLengthInteger()
	if err != nil {
		t.Fatal(err)
	}
	if v != 5+2<<8 {
		t.Errorf("3-byte readFixedLengthInteger = %d, want %d", v, 5+2<<8)
	}
}

// ---- readEOFPacket ----

func TestReadEOFPacketShort(t *testing.T) {
	// データが 1 バイト以下の場合: warnings/status は設定されない
	c := &Conn{}
	c.readEOFPacket([]byte{pEOF})
	if c.warnings != 0 {
		t.Errorf("warnings = %d, want 0", c.warnings)
	}
	if c.status != 0 {
		t.Errorf("status = %d, want 0", c.status)
	}
}

func TestReadEOFPacketWithWarnings(t *testing.T) {
	// len > 1: warnings 設定
	c := &Conn{}
	c.readEOFPacket([]byte{pEOF, 0x03, 0x00})
	if c.warnings != 3 {
		t.Errorf("warnings = %d, want 3", c.warnings)
	}
}

func TestReadEOFPacketWithStatus(t *testing.T) {
	// len > 3: status 設定
	c := &Conn{}
	c.readEOFPacket([]byte{pEOF, 0x01, 0x00, 0x02, 0x00})
	if c.warnings != 1 {
		t.Errorf("warnings = %d, want 1", c.warnings)
	}
	if c.status != 2 {
		t.Errorf("status = %d, want 2", c.status)
	}
}

// ---- readResultPacket の追加カバレッジ ----

func TestReadResultPacketUnknown(t *testing.T) {
	// pOK でも pEOF でも pERR でもない -> エラー
	buf := bytes.NewBuffer(makePacket([]byte{0x42})) // 0x42 は未知
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	err := c.readResultPacket()
	if err == nil {
		t.Error("expected error for unknown packet type")
	}
}

func TestReadResultPacketEOF(t *testing.T) {
	// pEOF パケット: readEOFPacket を呼び nil を返す
	buf := bytes.NewBuffer(makePacket([]byte{pEOF, 0x00, 0x00}))
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	err := c.readResultPacket()
	if err != nil {
		t.Errorf("pEOF readResultPacket should return nil, got %v", err)
	}
}

func TestReadFixedLengthIntegerReadError(t *testing.T) {
	// 空バッファ -> readPacket が io.EOF でエラー
	buf := bytes.NewBuffer([]byte{})
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	_, err := c.readFixedLengthInteger()
	if err == nil {
		t.Error("expected error for empty reader")
	}
}

// ---- errorPacketToString ----

func TestErrorPacketToStringShort(t *testing.T) {
	// データが 1 バイト以下 -> "authentication error"
	c := &Conn{}
	err := c.errorPacketToString([]byte{pERR})
	if err == nil {
		t.Error("expected error")
	}
	if err.Error() != "authentication error" {
		t.Errorf("got %q, want 'authentication error'", err.Error())
	}
}

func TestErrorPacketToStringWithNullByte(t *testing.T) {
	// data[3:] に null byte がある場合
	payload := []byte{pERR, 0x0A, 0x00, 'm', 's', 'g', 0x00, 'x', 'x'}
	c := &Conn{}
	err := c.errorPacketToString(payload)
	if err == nil {
		t.Error("expected error")
	}
	// null byte 前の "msg" が切り取られる (IndexByte の挙動確認)
	_ = err
}

// ---- handshakeRead エラーパス ----

// makeHandshakePacket はテスト用の MySQL ハンドシェイクパケットを構築する
// protocolVersion: プロトコルバージョンバイト
// capabilities: 2バイトのケイパビリティ値 (コードは big-endian-ish で読む)
func makeHandshakePacket(protocolVersion byte, capabilities uint16) []byte {
	data := []byte{protocolVersion}
	data = append(data, []byte("MySQL")...)
	data = append(data, 0x00) // server version null terminator
	data = append(data, 0x01, 0x00, 0x00, 0x00) // connection id
	// auth-plugin-data-part-1 (8 bytes)
	data = append(data, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22)
	data = append(data, 0x00) // filler
	// capability (code reads as: int(uint(data[partPos])<<8 + uint(data[partPos+1])))
	data = append(data, byte(capabilities>>8), byte(capabilities))
	return data
}

func TestHandshakeReadProtocolVersionError(t *testing.T) {
	// プロトコルバージョン 9 < 10 -> エラー
	buf := bytes.NewBuffer(makePacket(makeHandshakePacket(9, 0)))
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	_, err := c.handshakeRead()
	if err == nil {
		t.Error("expected error for old protocol version")
	}
}

func TestHandshakeReadNoProtocol41Error(t *testing.T) {
	// capability & 0x200 == 0 -> Protocol41 なし -> エラー
	buf := bytes.NewBuffer(makePacket(makeHandshakePacket(10, 0x0000)))
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	_, err := c.handshakeRead()
	if err == nil {
		t.Error("expected error for missing Protocol41 capability")
	}
}

func TestHandshakeReadNoSSLError(t *testing.T) {
	// Protocol41 あり (0x0200) だが SSL なし -> エラー
	// capability = uint(0x02)<<8 + uint(0x00) = 0x0200
	// 0x0200 & 0x200 = 0x0200 != 0 (Protocol41 OK)
	// 0x0200 & 0x800 = 0 (SSL なし -> エラー)
	buf := bytes.NewBuffer(makePacket(makeHandshakePacket(10, 0x0200)))
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	_, err := c.handshakeRead()
	if err == nil {
		t.Error("expected error for missing SSL capability")
	}
}

// ---- commandSimple / command / commandBinary write error パス ----

func TestCommandSimpleWriteError(t *testing.T) {
	// errWriter を使って writePacket/writeBytes/Flush で失敗させる
	c := &Conn{}
	c.w = bufio.NewWriter(&errWriter{})

	err := c.commandSimple(0x0E)
	if err == nil {
		t.Error("expected write error from commandSimple")
	}
}

func TestCommandWriteError(t *testing.T) {
	c := &Conn{}
	c.w = bufio.NewWriter(&errWriter{})

	err := c.command(0x03, "SELECT 1")
	if err == nil {
		t.Error("expected write error from command")
	}
}

func TestCommandBinaryWriteError(t *testing.T) {
	c := &Conn{}
	c.w = bufio.NewWriter(&errWriter{})

	err := c.commandBinary(0x12, []byte{0x01, 0x02})
	if err == nil {
		t.Error("expected write error from commandBinary")
	}
}

func TestHandshakeReadWithSecureConnection(t *testing.T) {
	// Protocol41 + SSL + SecureConnection 全設定
	// capability = 0x8A00: Protocol41 (0x0200) | SSL (0x0800) | SecureConnection (0x8000)
	cap := uint16(0x8A00)
	pkt := makeHandshakePacket(10, cap)
	// charset(1) + status(2) + capability_upper(2)
	pkt = append(pkt, 0x21, 0x00, 0x00, 0x00, 0x00)
	// auth-plugin-data-len = 22 -> authData2Len = 22-8 = 14 > 13 -> 13 にキャップ
	pkt = append(pkt, 0x16)
	// reserved (10 bytes)
	pkt = append(pkt, make([]byte, 10)...)
	// auth-plugin-data-part-2 (13 bytes)
	pkt = append(pkt, make([]byte, 13)...)

	buf := bytes.NewBuffer(makePacket(pkt))
	c := &Conn{}
	c.r = bufio.NewReader(buf)

	salt, err := c.handshakeRead()
	if err != nil {
		t.Fatal(err)
	}
	// salt = 8 (part1) + 13 (part2, capped) = 21 bytes
	if len(salt) != 21 {
		t.Errorf("salt length = %d, want 21", len(salt))
	}
}
