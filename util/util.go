package util

func ReadLengthEncodedInteger(data []byte) (uint64, int) {
	first := uint64(data[0])

	if first < 251 {
		return first, 1
	}

	var v uint64
	size := 1
	if first == 0xfb {
		v = 0 // null
	} else if first == 0xfc {
		v = uint64(data[1]) + uint64(data[2])<<8
		size = 2
	} else if first == 0xfd {
		v = uint64(data[1]) + uint64(data[2])<<8 + uint64(data[3])<<16
		size = 3
	} else if first == 0xfe {
		v = uint64(data[1]) + uint64(data[2])<<8 + uint64(data[3])<<16 + uint64(data[4])<<24 +
			uint64(data[5])<<32 + uint64(data[6])<<40 + uint64(data[7])<<48 + uint64(data[8])<<56
		size = 8
	}

	return v, size
}

func ReadLengthEncodedString(data []byte) (string, int) {
	strSize, pos := ReadLengthEncodedInteger(data)
	if 0 < strSize {
		str := string(data[pos : pos+int(strSize)])
		return str, pos + int(strSize)
	}

	return "", pos
}

func BytesToUint(data []byte) uint32 {
	return uint32(uint(data[0]) |
		uint(data[1])<<8 |
		uint(data[2])<<16 |
		uint(data[3])<<24)
}

func BytesToInt(data []byte) int {
	return int(uint(data[0]) |
		uint(data[1])<<8 |
		uint(data[2])<<16 |
		uint(data[3])<<24)
}

func IntToBytes(v int) []byte {
	b := make([]byte, 4)
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	return b
}
