package binlog

import (
	"time"
)

const (
	DATETIMEF_INT_OFS = 0x8000000000
	TIMEF_OFS         = 0x800000000000
	TIMEF_INT_OFS     = 0x800000
)

func parseBitmaskBytes(data []byte, count int) []bool {
	flags := make([]bool, count)

	for i := 0; i < count; i++ {
		bitIndex := uint(i % 8)
		byteIndex := i / 8
		flag := (data[byteIndex])&(1<<bitIndex) != 0

		flags[i] = flag
	}

	return flags
}

func readLittleEndianUvarint(data []byte) (uint64, int) {
	readed := 0
	val := uint64(0)

	for i := 0; i < len(data); i++ {
		val = val + uint64(data[i])<<(uint(i)*8)
		readed++
	}

	return val, readed
}

func readLittleEndianVarint(data []byte) (int64, int) {
	v, n := readLittleEndianUvarint(data)
	return int64(v), n
}

func readBigEndianVarint(data []byte) (int, int) {
	v, n := readBigEndianVarint64(data)
	return int(v), n
}

func readBigEndianVarint64(data []byte) (int64, int) {
	readed := 0
	val := int64(0)

	for i := 0; i < len(data); i++ {
		if i != 0 {
			val <<= 8
			readed++
		}
		val |= int64(data[i])
	}

	return val, readed
}

func readBigEndianUvarint(data []byte) (uint, int) {
	v, n := readBigEndianUvarint(data)
	return uint(v), n
}

func readBigEndianUvarint64(data []byte) (uint64, int) {
	readed := 0
	val := uint64(0)

	for i := 0; i < len(data); i++ {
		if i != 0 {
			val <<= 8
			readed++
		}
		val |= uint64(data[i])
	}

	return val, readed
}

// mysql-source: sql/log_event.cc
func convertMysqllonglongToDate(ltime uint64) time.Time {
	day := int(ltime & 31)
	month := int(ltime>>5) & 15
	year := int(ltime >> 9)
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
}

// mysql-source: sql-common/my_time.c:
func convertMysqllonglongToTime(ltime uint64) time.Time {
	ymdhms := ltime >> 24

	ymd := int(ymdhms >> 17)
	ym := ymd >> 5
	hms := int(ymdhms % (1 << 17))

	day := ymd % (1 << 5)
	month := ym % 13
	year := int(uint(ym / 13))

	second := hms % (1 << 6)
	minute := (hms >> 6) % (1 << 6)
	hour := int(uint(hms >> 12))

	return time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
}

// mysql-source: sql-common/my_time.c:
func readPackedTimestamp(data []byte, meta int) (time.Time, int) {
	size := 4
	ltime, _ := readBigEndianUvarint64(data[:size])
	nsec := int64(0)

	switch meta {
	case 1, 2:
		nsec = int64(data[size]) * 10
		size += 1
	case 3, 4:
		nsec, _ = readBigEndianVarint64(data[size : size+2])
		nsec /= 10
		size += 2
	case 5, 6:
		nsec, _ = readBigEndianVarint64(data[size : size+3])
		nsec /= 10000
		size += 3
	}

	dtime := time.Unix(int64(ltime), nsec)
	dtime = dtime.In(time.UTC)

	return dtime, size
}

// mysql-source: sql-common/my_time.c:
func readPackedDatetime(data []byte, meta int) (uint64, int) {
	size := 5
	frac := 0
	ltime, _ := readBigEndianUvarint64(data[:size])
	ltime = ltime - DATETIMEF_INT_OFS

	switch meta {
	case 0:
		return MY_PACKED_TIME_MAKE_INT(ltime), size
	case 1, 2:
		frac = int(data[size]) * 10000
		size += 1
	case 3, 4:
		frac, _ = readBigEndianVarint(data[size : size+2])
		frac *= 100
		size += 2
	case 5, 6:
		frac, _ = readBigEndianVarint(data[size : size+3])
		size += 3
	}

	return MY_PACKED_TIME_MAKE(ltime, uint64(frac)), size
}

func readPackedTime(data []byte, meta int) (uint64, int) {
	size := 3

	switch meta {
	case 1, 2:
		ltime, _ := readBigEndianVarint64(data[:size])
		ltime = ltime - TIMEF_INT_OFS
		frac := int(data[size])
		size += 1
		if ltime < 0 && 0 != frac {
			ltime += 1
			frac -= 0x100
		}
		return MY_PACKED_TIME_MAKE(uint64(ltime), uint64(frac*10000)), size
	case 3, 4:
		ltime, _ := readBigEndianVarint64(data[:size])
		ltime = ltime - TIMEF_INT_OFS
		frac, _ := readBigEndianVarint(data[size : size+2])
		size += 2
		if ltime < 0 && 0 != frac {
			ltime += 1
			frac -= 0x10000
		}
		return MY_PACKED_TIME_MAKE(uint64(ltime), uint64(frac*100)), size
	case 5, 6:
		// other
		size = 6
		ltime, _ := readBigEndianUvarint64(data[:size])
		ltime = ltime - TIMEF_INT_OFS
		return ltime, size
	}

	ltime, _ := readBigEndianVarint64(data[:size])
	ltime = ltime - TIMEF_INT_OFS
	return MY_PACKED_TIME_MAKE_INT(uint64(ltime)), size
}

func MY_PACKED_TIME_MAKE_INT(u uint64) uint64 {
	return u << 24
}

func MY_PACKED_TIME_MAKE(u uint64, frac uint64) uint64 {
	return (u << 24) + frac
}
