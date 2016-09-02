package binlog

import (
	"encoding/binary"
	"fmt"
	"github.com/uwork/bingo/util"
	"math"
	"strconv"
)

const (
	TYPE_DECIMAL = iota
	TYPE_TINY
	TYPE_SHORT
	TYPE_LONG
	TYPE_FLOAT
	TYPE_DOUBLE
	TYPE_NULL
	TYPE_TIMESTAMP
	TYPE_LONGLONG
	TYPE_INT24
	TYPE_DATE // 0x0a
	TYPE_TIME
	TYPE_DATETIME
	TYPE_YEAR
	TYPE_NEWDATE
	TYPE_VARCHAR
	TYPE_BIT // 0x10
	TYPE_TIMESTAMP2
	TYPE_DATETIME2
	TYPE_TIME2

	TYPE_UNKNOWN = 0xf0

	TYPE_JSON        = 0xf5
	TYPE_NEWDECIMAL  = 0xf6
	TYPE_ENUM        = 0xf7
	TYPE_SET         = 0xf8
	TYPE_TINY_BLOB   = 0xf9
	TYPE_MEDIUM_BLOB = 0xfa
	TYPE_LONG_BLOB   = 0xfb
	TYPE_BLOB        = 0xfc
	TYPE_VAR_STRING  = 0xfd
	TYPE_STRING      = 0xfe
	TYPE_GEOMETRY    = 0xff
)

var DECIMAL_SIZES = [...]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

// http://dev.mysql.com/doc/internals/en/table-map-event.html
func (p *BinlogParser) parseBinlogTableMap(ev *BinlogEvent, data []byte) error {

	tableIdSize := 6
	if p.Description.EventTypeHeadersLength[BINLOG_EVENT_TABLE_MAP] == 6 {
		tableIdSize = 4
	}

	tm := &BinlogEventTableMap{}

	// post-header
	tm.TableId, _ = readLittleEndianUvarint(data[:tableIdSize])
	pos := tableIdSize

	tm.Flags = uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2

	// payload
	schemaNameLen := int(data[pos])
	pos += 1

	tm.SchemaName = string(data[pos : pos+schemaNameLen])
	pos += schemaNameLen
	pos += 1 // null data

	tableNameLen := int(data[pos])
	pos += 1

	tm.TableName = string(data[pos : pos+tableNameLen])
	pos += tableNameLen
	pos += 1 // null data

	columnCount, n := util.ReadLengthEncodedInteger(data[pos:])
	tm.ColumnCount = int(columnCount)
	pos += n

	tm.ColumnTypes = data[pos : pos+tm.ColumnCount]
	pos += tm.ColumnCount

	// metadata
	metadata_, n := util.ReadLengthEncodedString(data[pos:])
	metadata := []byte(metadata_)
	metaIndex := 0

	// mysql source: sql/rpl_utility.cc
	for _, colType := range tm.ColumnTypes {
		switch colType {
		case TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB, TYPE_TINY_BLOB,
			TYPE_GEOMETRY, TYPE_JSON, TYPE_DOUBLE, TYPE_FLOAT:
			tm.ColumnMetas = append(tm.ColumnMetas, int(uint(metadata[metaIndex])))
			metaIndex += 1

		case TYPE_SET, TYPE_ENUM, TYPE_STRING:
			meta := int(uint(metadata[metaIndex])<<8 | uint(metadata[metaIndex+1]))
			tm.ColumnMetas = append(tm.ColumnMetas, meta)
			metaIndex += 2

		case TYPE_BIT:
			meta := int(uint(metadata[metaIndex]) | uint(metadata[metaIndex+1])<<8)
			tm.ColumnMetas = append(tm.ColumnMetas, meta)
			metaIndex += 2

		case TYPE_VAR_STRING, TYPE_VARCHAR:
			meta := int(uint(metadata[metaIndex]) | uint(metadata[metaIndex+1])<<8)
			tm.ColumnMetas = append(tm.ColumnMetas, meta)
			metaIndex += 2

		case TYPE_DECIMAL, TYPE_NEWDECIMAL:
			meta := int(uint(metadata[metaIndex])<<8 | uint(metadata[metaIndex+1]))
			tm.ColumnMetas = append(tm.ColumnMetas, meta)
			metaIndex += 2

		case TYPE_TIME2, TYPE_DATETIME2, TYPE_TIMESTAMP2:
			tm.ColumnMetas = append(tm.ColumnMetas, int(uint(metadata[metaIndex])))
			metaIndex += 1

		default:
			tm.ColumnMetas = append(tm.ColumnMetas, 0)
		}
	}
	pos += n

	// null bitmask flags
	nullFlagsSize := (tm.ColumnCount + 7) / 8
	tm.NullableColumns = parseBitmaskBytes(data[pos:pos+nullFlagsSize], tm.ColumnCount)
	ev.TableMap = tm

	return nil
}

// http://dev.mysql.com/doc/internals/en/rows-event.html
func (p *BinlogParser) parseBinlogRows(ev *BinlogEvent, data []byte) error {

	tableIdSize := 6
	if p.Description.EventTypeHeadersLength[BINLOG_EVENT_TABLE_MAP] == 6 {
		tableIdSize = 4
	}

	r := &BinlogEventRows{}

	// post-header
	r.TableId, _ = readLittleEndianUvarint(data[:tableIdSize])
	pos := tableIdSize

	tmap := p.TableMaps[r.TableId]
	r.Schema = tmap.SchemaName
	r.Table = tmap.TableName

	r.Flags = uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2

	rowEventVersion := 2
	evType := ev.Header.EventType
	if evType == BINLOG_EVENT_WRITE_ROWSv1 || evType == BINLOG_EVENT_UPDATE_ROWSv1 || evType == BINLOG_EVENT_DELETE_ROWSv1 {
		rowEventVersion = 1
	}

	if rowEventVersion >= 2 {
		// extra-data
		extraLen := int(uint(data[pos])|uint(data[pos+1])<<8) - 2
		pos += 2

		r.ExtraData = data[pos : pos+extraLen]
		pos += extraLen
	}

	columns, n := util.ReadLengthEncodedInteger(data[pos:])
	pos += n

	// columns-present-bitmap1
	presentFlagsSize := (int(columns) + 7) / 8
	presentedColumns := parseBitmaskBytes(data[pos:pos+presentFlagsSize], int(columns))
	pos += presentFlagsSize

	// columns-present-bitmap2
	var presentedUpdateColumns []bool
	if ev.Header.IsRowsUpdateEvent() {
		presentedUpdateColumns = parseBitmaskBytes(data[pos:pos+presentFlagsSize], int(columns))
		pos += presentFlagsSize
	}

	// rows
	for pos < len(data) {
		row, n, err := p.parseRowBinary(ev, r, presentedColumns, data[pos:])
		if err != nil {
			return err
		}
		pos += n

		if ev.Header.IsRowsUpdateEvent() {
			rowBefore, n, err := p.parseRowBinary(ev, r, presentedUpdateColumns, data[pos:])
			if err != nil {
				return err
			}
			pos += n
			row.BeforeRow = &rowBefore
		}
		r.Rows = append(r.Rows, row)
	}

	ev.Rows = r
	return nil
}

func (p *BinlogParser) parseRowBinary(ev *BinlogEvent, r *BinlogEventRows, presentedColumns []bool, data []byte) (Row, int, error) {
	tmap := p.TableMaps[r.TableId]
	row := Row{}
	row.IsEnableColumns = presentedColumns

	// null-bitmap
	nullBitmapsSize := (tmap.ColumnCount + 7) / 8
	row.IsNullColumns = parseBitmaskBytes(data[0:nullBitmapsSize], tmap.ColumnCount)

	pos := nullBitmapsSize

	// value of each field.
	for i := 0; i < len(presentedColumns); i++ {
		col := Column{}
		if !presentedColumns[i] {
			col.IsPresent = false
			row.Columns = append(row.Columns, col)
		} else if row.IsNullColumns[i] {
			col.IsNull = true
			col.IsPresent = true
			row.Columns = append(row.Columns, col)
		} else {
			col.Type = tmap.ColumnTypes[i]
			col.Meta = tmap.ColumnMetas[i]

			// mysql-source: sql/log_event.cc:
			// mysql source: include/libbinlogevents/src/binary_log_funcs.cpp
			var size int
			switch col.Type {
			case TYPE_NEWDECIMAL:
				// mysql source: strings/decimal.c
				prec := col.Meta >> 8
				dec := col.Meta & 0xff
				intg := prec - dec

				intg0 := int(float64(intg) / 9.0)
				frac0 := int(float64(dec) / 9.0)
				intg0x := intg - intg0*9
				frac0x := dec - frac0*9

				intsize := intg0*4 + DECIMAL_SIZES[intg0x]
				decsize := frac0*4 + DECIMAL_SIZES[frac0x]

				size = int(intsize + decsize)

				col.bin = data[pos : pos+size]
				pos += size

				// sign
				sign := ""
				if col.bin[0]&0x80 == 0 {
					sign = "-"
				}
				col.bin[0] ^= 0x80 // remove sign flag

				ints := "0"
				decs := "0"

				offset := 0
				if 0 < intsize {
					buff, _ := readBigEndianVarint(col.bin[offset : offset+DECIMAL_SIZES[intg0x]])
					ints = strconv.Itoa(int(buff))
					offset += DECIMAL_SIZES[intg0x]
					for i := 0; i < intg0; i++ {
						buff, _ = readBigEndianVarint(col.bin[offset : offset+4])
						ints += strconv.Itoa(int(buff))
						offset += 4
					}
				}

				if 0 < decsize {
					buff, _ := readBigEndianVarint(col.bin[offset : offset+DECIMAL_SIZES[frac0x]])
					decs = strconv.Itoa(int(buff))
					offset += DECIMAL_SIZES[frac0x]
					for i := 0; i < frac0; i++ {
						buff, _ = readBigEndianVarint(col.bin[offset : offset+4])
						decs += strconv.Itoa(int(buff))
						offset += 4
					}
				}

				col.str = fmt.Sprintf("%s%s.%s", sign, ints, decs)

			case TYPE_FLOAT, TYPE_DOUBLE:
				size = int(col.Meta)
				buf := data[pos : pos+size]

				if size == 4 {
					bits := binary.LittleEndian.Uint32(buf)
					col.double = float64(math.Float32frombits(bits))
				} else if size == 8 {
					bits := binary.LittleEndian.Uint64(buf)
					col.double = math.Float64frombits(bits)
				} else {
					return row, pos, fmt.Errorf("unknown float data: %v", buf)
				}

				pos += size

			case TYPE_SET, TYPE_ENUM, TYPE_STRING:
				typ := int(col.Meta >> 8)
				size = int(col.Meta & 0xff)

				if typ == TYPE_SET || typ == TYPE_ENUM {
					col.bin = data[pos : pos+size]
					col.num, _ = readBigEndianVarint(col.bin)
					pos += size
				} else {
					size = 1
					if (((col.Meta >> 4) & 0x300) ^ 0x300 + size) >= 0xff {
						size = 2
					}

					ssize, _ := readLittleEndianVarint(data[pos : pos+size])
					pos += size

					col.str = string(data[pos : pos+int(ssize)])
					pos += int(ssize)
				}

			case TYPE_YEAR:
				size = 1
				col.num = int(data[pos]) + 1900
				pos += size

			case TYPE_TINY:
				size = 1
				col.num = int(int8(data[pos]))
				pos += size

			case TYPE_SHORT:
				size = 2
				num, _ := readLittleEndianUvarint(data[pos : pos+size])
				col.num = int(int16(num))
				pos += size

			case TYPE_INT24:
				size = 3
				num, _ := readLittleEndianUvarint(data[pos : pos+size])

				sign := num&0x800000 != 0
				if sign {
					// 一度整数値に戻して32bitの負数に変換する
					num = (num - 1) ^ 0xffffff
					num = (num ^ 0xffffffff) + 1
				}
				col.num = int(int32(num))
				pos += size

			case TYPE_LONG:
				size = 4
				num, _ := readLittleEndianUvarint(data[pos : pos+size])
				col.num = int(int32(num))
				pos += size

			case TYPE_LONGLONG:
				size = 8
				num, _ := readLittleEndianUvarint(data[pos : pos+size])
				col.num = int(int64(num))
				pos += size

			case TYPE_NULL:
				size = 0
				col.IsNull = true

			case TYPE_TIME:
				// TODO
				size = 3
				num, _ := readLittleEndianVarint(data[pos : pos+size])
				col.num = int(num)
				pos += size

			case TYPE_DATE, TYPE_NEWDATE:
				size = 3
				ltime, _ := readLittleEndianUvarint(data[pos : pos+size])
				col.time = convertMysqllonglongToDate(ltime)
				pos += size

			case TYPE_TIME2:
				// mysql-source: sql-common/my_time.c:
				ltime, size := readPackedTime(data[pos:], col.Meta)
				col.time = convertMysqllonglongToTime(ltime)
				pos += size

			case TYPE_TIMESTAMP:
				// TODO
				size = 4
				num, _ := readLittleEndianVarint(data[pos : pos+size])
				col.num = int(num)
				pos += size

			case TYPE_TIMESTAMP2:
				time, size := readPackedTimestamp(data[pos:], col.Meta)
				col.time = time
				pos += size

			case TYPE_DATETIME:
				// TODO
				size = 8
				num, _ := readLittleEndianVarint(data[pos : pos+size])
				col.num = int(num)
				pos += size

			case TYPE_DATETIME2:
				// mysql-source: sql-common/my_time.c:
				ltime, size := readPackedDatetime(data[pos:], col.Meta)
				col.time = convertMysqllonglongToTime(ltime)
				pos += size

			case TYPE_BIT:
				nbits := ((col.Meta >> 8) * 8) + (col.Meta & 0xff)
				size := (nbits + 7) / 8
				col.bin = data[pos : pos+size]
				pos += size

			case TYPE_VARCHAR, TYPE_VAR_STRING:
				size = 1
				if col.Meta >= 256 {
					size = 2
				}

				strlen, _ := readLittleEndianUvarint(data[pos : pos+size])
				pos += size

				col.bin = data[pos : pos+int(strlen)]
				col.str = string(col.bin)
				pos += int(strlen)

			case TYPE_TINY_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB, TYPE_BLOB,
				TYPE_GEOMETRY, TYPE_JSON:
				size = int(col.Meta)

				strlen, _ := readLittleEndianUvarint(data[pos : pos+size])
				pos += size

				col.bin = data[pos : pos+int(strlen)]
				pos += int(strlen)

			default:
				// unknown
				col.bin = []byte{}
				col.Type = TYPE_UNKNOWN
				col.IsNull = true
				col.IsPresent = false
			}

			row.Columns = append(row.Columns, col)
		}
	}

	return row, pos, nil
}
