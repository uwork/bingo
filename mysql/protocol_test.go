package mysql

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
)

func TestMain(m *testing.M) {
	log.SetOutput(&bytes.Buffer{})
	os.Exit(m.Run())
}

func TestHandshakeRead(t *testing.T) {
	expecteds := []struct {
		packet []byte
		isOk   bool
		salt   []byte
	}{
		{
			[]byte{
				10, 53, 46, 55, 46, 49, 48, 0, 116, 56, 0, 0, 30,
				119, 6, 126, 70, 28, 122, 22, 0, 255, 255, 8, 2, 0, 255,
				193, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 85, 68, 74, 110,
				105, 60, 2, 97, 10, 84, 80, 28, 0, 109, 121, 115, 113,
				108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115,
				115, 119, 111, 114, 100, 0,
			},
			true,
			[]byte{30, 119, 6, 126, 70, 28, 122, 22, 85, 68, 74, 110, 105, 60, 2, 97, 10, 84, 80, 28, 0},
		},
	}

	for _, s := range expecteds {
		packetSize := len(s.packet)
		data := []byte{
			byte(packetSize),
			byte(packetSize >> 8),
			byte(packetSize >> 16),
			1,
		}
		data = append(data, s.packet...)

		buf := bytes.NewBuffer(data)
		c := &Conn{}
		c.r = bufio.NewReader(buf)

		salt, err := c.handshakeRead()
		if err != nil {
			t.Errorf("invalid data: %v", err)
			break
		}

		if !reflect.DeepEqual(s.salt, salt) {
			t.Errorf("invalid read data.  expected:%v salt:%v", s.salt, salt)
		}
	}

}

func TestHandshakeWrite(t *testing.T) {
	salt := []byte{119, 10, 41, 56, 96, 16, 76, 53, 22, 7, 86, 111, 65, 40, 103, 93, 55, 1, 84, 61, 0}
	expecteds := []struct {
		user   string
		pass   string
		isOk   bool
		result []byte
	}{
		{"user", "password!", true,
			[]byte{
				80, 0, 0, 0, 1, 130, 8, 0, 0, 0, 0, 0, 33,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 117, 115, 101, 114, 0, 20, 239,
				52, 119, 210, 122, 35, 185, 51, 3, 234, 122, 217,
				255, 83, 173, 158, 85, 83, 183, 184, 109, 121, 115,
				113, 108, 95, 110, 97, 116, 105, 118, 101, 95,
				112, 97, 115, 115, 119, 111, 114, 100, 0,
			},
		},
	}

	for _, s := range expecteds {
		buf := &bytes.Buffer{}
		c := &Conn{}
		c.w = bufio.NewWriter(buf)
		c.capabilities = clientLongPassword | clientProtocolVersion41 | clientSecureConnection | clientPluginAuth

		err := c.handshakeWrite(s.user, s.pass, salt)

		if s.isOk {
			if err != nil {
				t.Errorf("write error: %v", err)
			}
			writed := buf.Bytes()
			if !reflect.DeepEqual(s.result, writed) {
				t.Errorf("invalid write data.  expected:%v data:%v", s.result, writed)
			}
		} else {
			if err == nil {
				t.Errorf("invalid pattern. input: %v", s)
			}
		}
	}
}

func TestReadResultPacket(t *testing.T) {
	expecteds := []struct {
		data    []byte
		isOk    bool
		errCode int
		errMsg  string
	}{
		{[]byte{1, 0, 0, 1, pOK}, true, 0, ""},
		{[]byte{1, 0, 0, 1, pERR}, false, 0, ""},
		{append([]byte{3 + 7, 0, 0, 1, pERR, 10, 0}, []byte("message")...), false, 10, "message"},
		{[]byte{1, 0, 0, 1, pEOF}, false, 0, ""},
	}

	for _, s := range expecteds {
		c := &Conn{}
		c.r = bufio.NewReader(bytes.NewBuffer(s.data))

		err := c.readResultPacket()

		if s.isOk {
			if err != nil {
				t.Errorf("read error: %v", err)
			}
		} else {
			var expectedMessage string
			if s.data[4] == pERR {
				if len(s.errMsg) != 0 {
					expectedMessage = fmt.Sprintf("authentication error: %s (%d)", s.errMsg, s.errCode)
				} else {
					expectedMessage = "authentication error"
				}
			} else {
				expectedMessage = fmt.Sprintf("unknown error in authentication sequence: %#v", s.data[4:])
			}
			if err != nil {
				if expectedMessage != err.Error() {
					t.Errorf("invalid read data.  expected:%v error:%v", expectedMessage, err.Error())
				}
			} else if s.data[4] != pEOF {
				t.Errorf("invalid read data.  err is null")
			}
		}
	}
}

func TestReadPacket(t *testing.T) {
	expecteds := []struct {
		head   []byte
		data   []byte
		result []byte
	}{
		{[]byte{13, 0, 0, 1}, []byte("connect mysql"), []byte("connect mysql")},
		{[]byte{6, 0, 0, 2}, []byte{255, 0, 128, 127, 1, 254}, []byte{255, 0, 128, 127, 1, 254}},
	}

	buf := &bytes.Buffer{}
	for _, s := range expecteds {
		data := append(s.head, s.result...)
		buf.Write(data)
	}

	c := &Conn{}
	c.r = bufio.NewReader(buf)

	for _, s := range expecteds {
		result, err := c.readPacket()
		if err != nil {
			t.Errorf("read error: %v", err)
		}

		if !reflect.DeepEqual(s.result, result) {
			t.Errorf("invalid read data.  expected:%v data:%v", s.result, result)
		}
		if uint(s.head[3]+1) != c.sequence {
			t.Errorf("invalid sequence.  expected:%v sequence:%v", s.head[3]+1, c.sequence)
		}
	}
}

func TestWritePacket(t *testing.T) {
	expecteds := []struct {
		input []byte
		head  []byte
	}{
		{[]byte("connect mysql"), []byte{13, 0, 0, 1}},
		{[]byte{255, 0, 128, 127, 1, 254}, []byte{6, 0, 0, 2}},
	}

	buf := &bytes.Buffer{}
	c := &Conn{}
	c.sequence = 1
	c.w = bufio.NewWriter(buf)

	for _, s := range expecteds {
		buf.Reset()
		err := c.writePacket(s.input)
		if err != nil {
			t.Errorf("write packet error: %v", err)
		}

		writed := buf.Bytes()
		expect := append(s.head, s.input...)
		if !reflect.DeepEqual(expect, writed) {
			t.Errorf("invalid write data.  expected:%v data:%v", expect, writed)
		}
	}
}

func TestReadBytes(t *testing.T) {
	expecteds := []struct {
		data   []byte
		size   int
		result []byte
	}{
		{[]byte("connect mysql"), 13, []byte("connect mysql")},
		{[]byte("connect mysql"), 3, []byte("con")},
		{[]byte{255, 0, 128, 127, 1, 254}, 5, []byte{255, 0, 128, 127, 1}},
	}
	for _, s := range expecteds {
		buf := bytes.NewBuffer(s.data)

		c := &Conn{}
		c.r = bufio.NewReader(buf)

		data, err := c.readBytes(s.size)
		if err != nil {
			t.Errorf("write error: %v", err)
		}

		if !reflect.DeepEqual(s.result, data) {
			t.Errorf("invalid read data.  expected:%v data:%v", s.result, data)
		}
	}

}

func TestWriteBytes(t *testing.T) {
	expecteds := []struct {
		input []byte
	}{
		{[]byte("connect mysql")},
		{[]byte{255, 0, 128, 127, 1, 254}},
	}
	for _, s := range expecteds {
		buf := &bytes.Buffer{}

		c := &Conn{}
		c.w = bufio.NewWriter(buf)

		err := c.writeBytes(s.input)
		if err != nil {
			t.Errorf("write error: %v", err)
		}

		writed := buf.Bytes()
		if !reflect.DeepEqual(s.input, writed) {
			t.Errorf("invalid write data.  expected:%v data:%v", s.input, writed)
		}
	}
}

func TestCreateNativePassword(t *testing.T) {
	expecteds := []struct {
		pass   string
		salt   []byte
		result []byte
	}{
		{
			"password!",
			[]byte{119, 10, 41, 56, 96, 16, 76, 53, 22, 7, 86, 111, 65, 40, 103, 93, 55, 1, 84, 61, 0},
			[]byte{239, 52, 119, 210, 122, 35, 185, 51, 3, 234, 122, 217, 255, 83, 173, 158, 85, 83, 183, 184},
		},
	}

	for _, s := range expecteds {
		hash := CreateNativePassword([]byte(s.pass), s.salt)

		if !reflect.DeepEqual(s.result, hash) {
			t.Errorf("invalid hash.  expected:%v hash:%v", s.result, hash)
		}
	}
}
