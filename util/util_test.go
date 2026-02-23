package util

import (
	"reflect"
	"testing"
)

func TestReadLengthEncodedInteger(t *testing.T) {
	tests := []struct {
		name  string
		data  []byte
		wantV uint64
		wantN int
	}{
		{"zero", []byte{0}, 0, 1},
		{"small", []byte{100}, 100, 1},
		{"max_single", []byte{250}, 250, 1},
		{"null_0xfb", []byte{0xfb, 0x00}, 0, 1},
		{"2byte_0xfc", []byte{0xfc, 0x01, 0x02}, uint64(0x01) + uint64(0x02)<<8, 2},
		{"3byte_0xfd", []byte{0xfd, 0x01, 0x02, 0x03}, uint64(0x01) + uint64(0x02)<<8 + uint64(0x03)<<16, 3},
		{"8byte_0xfe", []byte{0xfe, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			uint64(0x01) + uint64(0x02)<<8 + uint64(0x03)<<16 + uint64(0x04)<<24 +
				uint64(0x05)<<32 + uint64(0x06)<<40 + uint64(0x07)<<48 + uint64(0x08)<<56, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, n := ReadLengthEncodedInteger(tt.data)
			if v != tt.wantV || n != tt.wantN {
				t.Errorf("ReadLengthEncodedInteger(%v) = (%d, %d), want (%d, %d)",
					tt.data, v, n, tt.wantV, tt.wantN)
			}
		})
	}
}

func TestReadLengthEncodedString(t *testing.T) {
	// 非空文字列
	data := append([]byte{5}, []byte("hello")...)
	s, n := ReadLengthEncodedString(data)
	if s != "hello" || n != 6 {
		t.Errorf("ReadLengthEncodedString: got (%q, %d), want (hello, 6)", s, n)
	}

	// 空文字列 (size=0)
	data = []byte{0}
	s, n = ReadLengthEncodedString(data)
	if s != "" || n != 1 {
		t.Errorf("ReadLengthEncodedString empty: got (%q, %d), want ('', 1)", s, n)
	}
}

func TestBytesToUint(t *testing.T) {
	got := BytesToUint([]byte{0x01, 0x02, 0x03, 0x04})
	want := uint32(0x04030201)
	if got != want {
		t.Errorf("BytesToUint = 0x%X, want 0x%X", got, want)
	}

	// ゼロ
	got = BytesToUint([]byte{0, 0, 0, 0})
	if got != 0 {
		t.Errorf("BytesToUint(zeros) = %d, want 0", got)
	}
}

func TestBytesToInt(t *testing.T) {
	got := BytesToInt([]byte{0x01, 0x02, 0x03, 0x04})
	want := int(0x04030201)
	if got != want {
		t.Errorf("BytesToInt = 0x%X, want 0x%X", got, want)
	}

	got = BytesToInt([]byte{0, 0, 0, 0})
	if got != 0 {
		t.Errorf("BytesToInt(zeros) = %d, want 0", got)
	}
}

func TestIntToBytes(t *testing.T) {
	got := IntToBytes(0x04030201)
	want := []byte{0x01, 0x02, 0x03, 0x04}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("IntToBytes(0x04030201) = %v, want %v", got, want)
	}

	// ゼロ
	got = IntToBytes(0)
	want = []byte{0, 0, 0, 0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("IntToBytes(0) = %v, want %v", got, want)
	}
}

func TestRoundTrip(t *testing.T) {
	// IntToBytes -> BytesToInt の往復テスト
	values := []int{0, 1, 255, 256, 65535, 0x01020304}
	for _, v := range values {
		b := IntToBytes(v)
		got := BytesToInt(b)
		if got != v {
			t.Errorf("roundtrip %d: BytesToInt(IntToBytes(%d)) = %d", v, v, got)
		}
	}
}
