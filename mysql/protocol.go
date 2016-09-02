package mysql

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"github.com/uwork/bingo/util"
)

const (
	protocolVersion         = 10
	maxPacketSize           = 1<<24 - 1
	clientLongPassword      = 0x1
	clientProtocolVersion41 = 0x200
	clientSSL               = 0x800
	clientSecureConnection  = 0x8000
	clientPluginAuth        = 0x80000
)

const (
	// packet code: http://dev.mysql.com/doc/internals/en/generic-response-packets.html
	pOK  = 0x00
	pEOF = 0xfe
	pERR = 0xff
	// other: http://dev.mysql.com/doc/internals/en/describing-packets.html
)

func (c *Conn) commandSimple(command byte) error {
	c.sequence = 0

	err := c.writePacket([]byte{command})
	if err != nil {
		return err
	}

	return nil
}

func (c *Conn) command(command byte, args string) error {
	c.sequence = 0

	payload := append([]byte{command}, []byte(args)...)
	err := c.writePacket(payload)
	if err != nil {
		return err
	}

	return nil
}

func (c *Conn) commandBinary(command byte, args []byte) error {
	c.sequence = 0

	payload := append([]byte{command}, args...)
	err := c.writePacket(payload)
	if err != nil {
		return err
	}

	return nil
}

// http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
func (c *Conn) handshake(user string, pass string) error {
	// read handshake packet
	authSalt, err := c.handshakeRead()
	if err != nil {
		return err
	}

	// write handshake packet
	err = c.handshakeWrite(user, pass, authSalt)
	if err != nil {
		return err
	}

	err = c.readResultPacket()
	if err != nil {
		return err
	}

	return nil
}

func (c *Conn) handshakeRead() ([]byte, error) {
	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	// protocl version check
	if protocolVersion > data[0] {
		return nil, fmt.Errorf("server protocol version: %d < %d", data[0], protocolVersion)
	}

	// auth-plugin-data-part-1
	partPos := 1 + bytes.IndexByte(data[1:], 0x00) + 1 + 4
	authSalt := data[partPos : partPos+8]

	// auth-plugindata-part-1(8) + filler(1)
	partPos += 8 + 1

	// capability flags
	capability := int(uint(data[partPos])<<8 + uint(data[partPos+1]))
	c.capabilities = capability
	partPos += 2
	if capability&clientProtocolVersion41 == 0 {
		return nil, fmt.Errorf("client protocol version: %d & %d != 0", capability, clientProtocolVersion41)
	}
	if capability&clientSSL == 0 {
		return nil, fmt.Errorf("ssl not supported.")
	}

	// more data...
	if len(data) > partPos && capability&clientSecureConnection != 0 {
		// charaset(1) + status(2) + capabilityflag(2) + 1 + reserved(10)
		partPos += 1 + 2 + 2

		// length of auth-plugin-data
		authPartLen := uint(data[partPos])
		partPos += 1 + 10

		// max 13 bytes
		authData2Len := authPartLen - 8
		if authData2Len > 13 {
			authData2Len = 13
		}

		// auth-plugin-data-part2
		authSalt = append(authSalt, data[partPos:partPos+int(authData2Len)]...)
	}

	return authSalt, nil
}

func (c *Conn) handshakeWrite(user string, pass string, authSalt []byte) error {

	// capability flags
	capability := clientProtocolVersion41 | clientSecureConnection | clientPluginAuth | clientLongPassword | c.capabilities&0x4 // longflag
	maxPacketSize := 0x0
	characterSet := byte(33) // utf8 http://dev.mysql.com/doc/internals/en/character-set.html

	passwordBytes := []byte{}
	if len(pass) > 0 {
		passwordBytes = CreateNativePassword([]byte(pass), authSalt)
	}
	authPluginName := []byte("mysql_native_password")

	// make payload
	payload := []byte{}
	payload = append(payload, util.IntToBytes(capability)...)
	payload = append(payload, util.IntToBytes(maxPacketSize)...)
	payload = append(payload, characterSet)
	payload = append(payload, make([]byte, 23)...) // reserved(23)
	payload = append(payload, []byte(user)...)
	payload = append(payload, 0)
	payload = append(payload, byte(len(passwordBytes)))
	payload = append(payload, passwordBytes...)
	payload = append(payload, authPluginName...)
	payload = append(payload, 0)

	err := c.writePacket(payload)
	if err != nil {
		return err
	}

	return nil
}

// http://dev.mysql.com/doc/internals/en/generic-response-packets.html
// 暫定的に一部のパケット対応のみ実装
func (c *Conn) readResultPacket() error {
	data, err := c.readPacket()
	if err != nil {
		return err
	}

	if data[0] == pOK { // OK packet
		return nil
	}

	if data[0] == pEOF {
		c.readEOFPacket(data)
		return nil
	}

	if data[0] == pERR { // ERR packet
		return c.errorPacketToString(data)
	}

	return fmt.Errorf("unknown error in result packet: %#v", data)
}

func (c *Conn) readFixedLengthInteger() (int64, error) {
	data, err := c.readPacket()
	if err != nil {
		return -1, err
	}

	if data[0] == pOK { // OK packet
		return -1, nil
	}

	if data[0] == pEOF {
		c.readEOFPacket(data)
		return -1, nil
	}

	if data[0] == pERR { // ERR packet
		return -1, c.errorPacketToString(data)
	}

	length := len(data) - 1
	v := int64(data[0])
	if length >= 2 {
		v += int64(data[1]) << 8
	} else if length >= 3 {
		v += int64(data[2]) << 16
	} else if length >= 4 {
		v += int64(data[3]) << 24
	} else if length >= 6 {
		v += int64(data[4]) << 32
		v += int64(data[5]) << 40
	} else if length >= 8 {
		v += int64(data[6]) << 48
		v += int64(data[7]) << 56
	}

	return v, nil
}

func (c *Conn) errorPacketToString(data []byte) error {
	if len(data) > 1 {
		errorCode := int(uint(data[1]) + uint(data[2])<<8)
		var errorMessage string
		idx := bytes.IndexByte(data[3:], 0x00)
		if idx < 0 {
			errorMessage = string(data[3:])
		} else {
			errorMessage = string(data[3:idx])
		}
		return fmt.Errorf("authentication error: %s (%d)", errorMessage, errorCode)
	} else {
		return fmt.Errorf("authentication error")
	}
}

func (c *Conn) readEOFPacket(data []byte) {
	if len(data) > 1 {
		warnings := uint(uint(data[1]) + uint(data[2])<<8)
		c.warnings = warnings
	}
	if len(data) > 3 {
		statusFlags := uint(uint(data[3]) + uint(data[4])<<8)
		c.status = statusFlags
	}
}

// http://dev.mysql.com/doc/internals/en/mysql-packet.html
func (c *Conn) readPacket() ([]byte, error) {

	head, err := c.readBytes(4)
	if err != nil {
		return nil, err
	}

	length := int(uint(head[0]) + uint(head[1])<<8 + uint(head[2])<<16)
	seqId := uint(head[3])
	c.sequence = seqId + 1

	payload, err := c.readBytes(length)
	if err != nil {
		return nil, err
	}

	return payload, nil
}

func (c *Conn) writePacket(payload []byte) error {
	packet := make([]byte, 4)

	payloadSize := len(payload)
	packet[0] = byte(payloadSize)
	packet[1] = byte(payloadSize >> 8)
	packet[2] = byte(payloadSize >> 16)
	packet[3] = byte(c.sequence) // sequence id
	packet = append(packet, payload...)

	err := c.writeBytes(packet)
	if err != nil {
		return err
	}

	c.sequence += 1

	return nil
}

func (c *Conn) readBytes(expectSize int) ([]byte, error) {
	data := make([]byte, expectSize)

	tryCount := 0
	readedSize := 0
	for {
		tryCount += 1

		size, err := c.r.Read(data[readedSize:])
		if err != nil {
			return nil, err
		}

		readedSize += size
		if readedSize >= expectSize {
			break
		}

		// try count check.
		if tryCount > 10 {
			return nil, fmt.Errorf("packet read count was over.")
		}
	}

	return data, nil
}

func (c *Conn) writeBytes(data []byte) error {
	dataLen := len(data)

	tryCount := 0
	writedSize := 0
	for {
		tryCount += 1

		size, err := c.w.Write(data[writedSize:])
		if err != nil {
			return err
		}

		writedSize += size
		if writedSize >= dataLen {
			break
		}

		// try count check.
		if tryCount > 10 {
			return fmt.Errorf("packet write count was over.")
		}
	}

	err := c.w.Flush()
	if err != nil {
		return err
	}

	return nil
}

// http://dev.mysql.com/doc/internals/en/secure-password-authentication.html#packet-Authentication::Native41
func CreateNativePassword(pass []byte, authSalt []byte) []byte {
	crypto := sha1.New()

	crypto.Write(pass)
	value1 := crypto.Sum(nil)

	crypto.Reset()
	crypto.Write(value1)
	value2 := crypto.Sum(nil)

	crypto.Reset()
	crypto.Write(append(authSalt[:20], value2...))
	value3 := crypto.Sum(nil)

	for i := range value1 {
		value1[i] = value1[i] ^ value3[i]
	}

	return value1
}
