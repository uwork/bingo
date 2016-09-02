package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/uwork/bingo/util"
	"log"
	"reflect"
	"strconv"
	"time"
)

const (
	BINLOG_EVENT_QUERY              = 0x02
	BINLOG_EVENT_TABLE_MAP          = 0x13
	BINLOG_EVENT_FORMAT_DESCRIPTION = 0x0f

	BINLOG_EVENT_WRITE_ROWSv1  = 0x17
	BINLOG_EVENT_UPDATE_ROWSv1 = 0x18
	BINLOG_EVENT_DELETE_ROWSv1 = 0x19

	BINLOG_EVENT_WRITE_ROWSv2  = 0x1e
	BINLOG_EVENT_UPDATE_ROWSv2 = 0x1f
	BINLOG_EVENT_DELETE_ROWSv2 = 0x20
)

// FORMAT_DESCRIPTION_EVENT payload
type BinlogEventFormatDescription struct {
	BinlogVersion          uint16
	ServerVersion          string
	CreateTimestamp        uint32
	EventHeaderLength      uint8
	EventTypeHeadersLength []uint8
}

// QUERY_EVENT post-header + payload
type BinlogEventQuery struct {
	SlaveProxyId  uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    string
	Schema        string
	Query         string
}

// TABLE_MAP_EVENT payload
type BinlogEventTableMap struct {
	TableId         uint64
	Flags           uint16
	SchemaName      string
	TableName       string
	ColumnCount     int
	ColumnTypes     []byte
	ColumnMetas     []int
	NullableColumns []bool
}

type Column struct {
	bin       []byte
	num       int
	double    float64
	str       string
	time      time.Time
	Type      byte
	IsPresent bool
	IsNull    bool
	Meta      int
}

func NewColumn(_type byte, val interface{}) Column {
	c := Column{}
	c.Type = _type
	switch c.Type {
	case TYPE_LONG, TYPE_LONGLONG,
		TYPE_INT24, TYPE_TINY, TYPE_SHORT, TYPE_YEAR:
		c.num, _ = val.(int)
	case TYPE_FLOAT, TYPE_DOUBLE:
		c.double, _ = val.(float64)
	case TYPE_NEWDECIMAL, TYPE_DATETIME, TYPE_DATETIME2,
		TYPE_TIME2, TYPE_DATE, TYPE_TIMESTAMP2:
		c.time, _ = val.(time.Time)
	case TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB, TYPE_TINY_BLOB:
		c.bin, _ = val.([]byte)
	case TYPE_STRING, TYPE_VAR_STRING, TYPE_VARCHAR:
		c.str, _ = val.(string)
	}
	return c
}

func (c Column) Int() int {
	switch c.Type {
	case TYPE_FLOAT, TYPE_DOUBLE:
		return int(c.double)
	case TYPE_NEWDECIMAL, TYPE_DATETIME, TYPE_DATETIME2,
		TYPE_TIME2, TYPE_DATE, TYPE_TIMESTAMP2:
		return int(c.time.Unix())
	case TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB, TYPE_TINY_BLOB:
		num, _ := readLittleEndianVarint(c.bin)
		return int(num)
	case TYPE_STRING, TYPE_VAR_STRING, TYPE_VARCHAR:
		num, err := strconv.Atoi(c.str)
		if err != nil {
			log.Println(err)
			return 0
		}
		return num
	case TYPE_NULL:
		if c.IsNull {
			return 1
		} else {
			return 0
		}
	}
	return c.num
}

func (c Column) Double() float64 {
	switch c.Type {
	case TYPE_LONG, TYPE_LONGLONG,
		TYPE_INT24, TYPE_TINY, TYPE_SHORT, TYPE_YEAR:
		return float64(c.num)
	case TYPE_FLOAT, TYPE_DOUBLE:
		return (c.double)
	case TYPE_NEWDECIMAL, TYPE_DATETIME, TYPE_DATETIME2,
		TYPE_TIME2, TYPE_DATE, TYPE_TIMESTAMP2:
		return float64(c.time.Unix())
	case TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB, TYPE_TINY_BLOB:
		num, _ := readLittleEndianVarint(c.bin)
		return float64(num)
	case TYPE_STRING, TYPE_VAR_STRING, TYPE_VARCHAR:
		num, err := strconv.ParseFloat(c.str, 64)
		if err != nil {
			log.Println(err)
			return 0
		}
		return num
	case TYPE_NULL:
		if c.IsNull {
			return 1
		} else {
			return 0
		}
	}
	return c.double
}

func (c Column) Bytes() []byte {
	switch c.Type {
	case TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB, TYPE_TINY_BLOB, TYPE_BIT:
		return c.bin
	case TYPE_FLOAT, TYPE_DOUBLE:
		buf := new(bytes.Buffer)
		err := binary.Write(buf, binary.LittleEndian, c.Double())
		if err != nil {
			log.Println("double to bytes error.", err)
			return nil
		}
		return buf.Bytes()
	case TYPE_NULL:
		if c.IsNull {
			return []byte{1}
		} else {
			return []byte{0}
		}
	default:
		return c.bin
	}
	return c.bin
}

func (c Column) Time() time.Time {
	switch c.Type {
	case TYPE_NEWDECIMAL, TYPE_DATETIME, TYPE_DATETIME2,
		TYPE_TIME2, TYPE_DATE, TYPE_TIMESTAMP2:
		return c.time
	default:
		num := c.Int()
		return time.Unix(int64(num), 0)
	}
	return c.time
}

func (c Column) String() string {
	switch c.Type {
	case TYPE_LONG, TYPE_LONGLONG,
		TYPE_INT24, TYPE_TINY, TYPE_SHORT, TYPE_YEAR:
		return strconv.Itoa(c.num)
	case TYPE_FLOAT, TYPE_DOUBLE:
		return fmt.Sprintf("%f", c.double)
	case TYPE_NEWDECIMAL:
		return c.str
	case TYPE_DATETIME, TYPE_DATETIME2:
		return c.time.Format("2006-01-02 15:04:05")
	case TYPE_TIME2:
		return c.time.Format("15:04:05")
	case TYPE_DATE:
		return c.time.Format("2006-01-02")
	case TYPE_TIMESTAMP2:
		return c.time.Format("2006-01-02 15:04:05")
	case TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB, TYPE_TINY_BLOB:
		return string(c.bin)
	case TYPE_STRING, TYPE_VAR_STRING, TYPE_VARCHAR:
		return c.str
	case TYPE_NULL:
		return "[NULL]"
	}

	return c.str
}

func (c Column) Equals(c2 Column) bool {
	if c.IsNull || c2.IsNull {
		return c.IsNull == c2.IsNull
	}

	switch c.Type {
	case TYPE_LONG, TYPE_LONGLONG,
		TYPE_INT24, TYPE_TINY, TYPE_SHORT, TYPE_YEAR:
		return c.num == c2.Int()
	case TYPE_FLOAT, TYPE_DOUBLE:
		return c.double == c2.Double()
	case TYPE_NEWDECIMAL, TYPE_DATETIME, TYPE_DATETIME2,
		TYPE_TIME2, TYPE_DATE, TYPE_TIMESTAMP2:
		return c.time == c2.Time()
	case TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB, TYPE_TINY_BLOB:
		return reflect.DeepEqual(c.bin, c2.Bytes())
	case TYPE_STRING, TYPE_VAR_STRING, TYPE_VARCHAR:
		return c.str == c2.String()
	case TYPE_NULL:
		return c.IsNull == c2.IsNull
	}
	return false
}

func (c Column) GreaterThan(c2 Column) bool {
	if c.IsNull || c2.IsNull {
		return false
	}

	switch c.Type {
	case TYPE_LONG, TYPE_LONGLONG,
		TYPE_INT24, TYPE_TINY, TYPE_SHORT, TYPE_YEAR:
		return c.num > c2.Int()
	case TYPE_FLOAT, TYPE_DOUBLE:
		return c.double > c2.Double()
	case TYPE_NEWDECIMAL, TYPE_DATETIME, TYPE_DATETIME2,
		TYPE_TIME2, TYPE_DATE, TYPE_TIMESTAMP2:
		return c.time.Unix() > c2.Time().Unix()
	case TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB, TYPE_TINY_BLOB:
		return string(c.bin) > string(c2.Bytes())
	case TYPE_STRING, TYPE_VAR_STRING, TYPE_VARCHAR:
		return c.str > c2.String()
	case TYPE_NULL:
		return c.IsNull && !c2.IsNull
	}
	return false
}
func (c Column) GreaterEquals(c2 Column) bool {
	if c.IsNull || c2.IsNull {
		return false
	}

	switch c.Type {
	case TYPE_LONG, TYPE_LONGLONG,
		TYPE_INT24, TYPE_TINY, TYPE_SHORT, TYPE_YEAR:
		return c.num >= c2.Int()
	case TYPE_FLOAT, TYPE_DOUBLE:
		return c.double >= c2.Double()
	case TYPE_NEWDECIMAL, TYPE_DATETIME, TYPE_DATETIME2,
		TYPE_TIME2, TYPE_DATE, TYPE_TIMESTAMP2:
		return c.time.Unix() >= c2.Time().Unix()
	case TYPE_BLOB, TYPE_MEDIUM_BLOB, TYPE_LONG_BLOB, TYPE_TINY_BLOB:
		return string(c.bin) >= string(c2.Bytes())
	case TYPE_STRING, TYPE_VAR_STRING, TYPE_VARCHAR:
		return c.str >= c2.String()
	case TYPE_NULL:
		return c.IsNull || c2.IsNull == false
	}

	return false
}

type Row struct {
	IsNullColumns   []bool
	IsEnableColumns []bool
	Columns         []Column
	BeforeRow       *Row
}

// ROW_EVENT payload
type BinlogEventRows struct {
	TableId   uint64
	Schema    string
	Table     string
	Flags     uint16
	ExtraData []byte
	Rows      []Row
}

type BinlogEventHeader struct {
	// Binlog Header
	Timestamp uint32
	EventType uint8
	ServerId  uint32
	EventSize uint32
	LogPos    uint32
	Flags     uint16
}

func (h *BinlogEventHeader) IsRowsUpdateEvent() bool {
	return h.EventType == BINLOG_EVENT_UPDATE_ROWSv1 || h.EventType == BINLOG_EVENT_UPDATE_ROWSv2
}

type BinlogEvent struct {
	Header            *BinlogEventHeader
	Query             *BinlogEventQuery
	FormatDescription *BinlogEventFormatDescription
	TableMap          *BinlogEventTableMap
	Rows              *BinlogEventRows
}

type BinlogParser struct {
	Description *BinlogEventFormatDescription
	TableMaps   map[uint64]*BinlogEventTableMap
}

func (p *BinlogParser) ParseBinlogEvent(data []byte) (*BinlogEvent, int, error) {
	ev, pos, err := p.parseBinlogHeader(data)
	if err != nil {
		return nil, 0, err
	}

	switch ev.Header.EventType {
	case BINLOG_EVENT_FORMAT_DESCRIPTION:
		if err = p.parseBinlogFormatDescription(ev, data[pos:]); err != nil {
			return nil, 0, err
		}
		p.Description = ev.FormatDescription

	case BINLOG_EVENT_QUERY:
		if err = p.parseBinlogQuery(ev, data[pos:]); err != nil {
			return nil, 0, err
		}

	case BINLOG_EVENT_TABLE_MAP:
		if err = p.parseBinlogTableMap(ev, data[pos:]); err != nil {
			return nil, 0, err
		}
		p.TableMaps[ev.TableMap.TableId] = ev.TableMap

	case BINLOG_EVENT_WRITE_ROWSv1,
		BINLOG_EVENT_UPDATE_ROWSv1,
		BINLOG_EVENT_DELETE_ROWSv1,
		BINLOG_EVENT_WRITE_ROWSv2,
		BINLOG_EVENT_UPDATE_ROWSv2,
		BINLOG_EVENT_DELETE_ROWSv2:
		if err = p.parseBinlogRows(ev, data[pos:]); err != nil {
			return nil, 0, err
		}
	}

	return ev, pos, nil
}

func (p *BinlogParser) parseBinlogHeader(data []byte) (*BinlogEvent, int, error) {
	if len(data) < 19 {
		return nil, 0, fmt.Errorf("binlog event data size %d < 19. (support binlog v4 only)", len(data))
	}

	pos := 0

	head := &BinlogEventHeader{}
	head.Timestamp = util.BytesToUint(data[pos : pos+4])
	pos += 4

	head.EventType = uint8(data[pos])
	pos += 1

	head.ServerId = util.BytesToUint(data[pos : pos+4])
	pos += 4

	head.EventSize = util.BytesToUint(data[pos : pos+4])
	pos += 4

	head.LogPos = util.BytesToUint(data[pos : pos+4])
	pos += 4

	head.Flags = uint16(data[pos]) + uint16(data[pos+1])<<8
	pos += 2

	ev := &BinlogEvent{}
	ev.Header = head
	return ev, pos, nil
}

func (p *BinlogParser) parseBinlogFormatDescription(ev *BinlogEvent, data []byte) error {
	pos := 0

	fd := &BinlogEventFormatDescription{}
	fd.BinlogVersion = uint16(data[pos]) + uint16(data[pos+1])<<8
	pos += 2

	fd.ServerVersion = string(data[pos : pos+50])
	pos += 50

	fd.CreateTimestamp = util.BytesToUint(data[pos : pos+4])
	pos += 4

	fd.EventHeaderLength = uint8(data[pos])
	pos += 1

	eventsLen := []uint8{}
	for b := range data[pos:] {
		eventsLen = append(eventsLen, uint8(b))
	}
	fd.EventTypeHeadersLength = eventsLen

	ev.FormatDescription = fd

	return nil
}

func (p *BinlogParser) parseBinlogQuery(ev *BinlogEvent, data []byte) error {
	pos := 0

	q := &BinlogEventQuery{}
	q.SlaveProxyId = util.BytesToUint(data[pos : pos+4])
	pos += 4

	q.ExecutionTime = util.BytesToUint(data[pos : pos+4])
	pos += 4

	schemaLen := int(uint8(data[pos]))
	pos += 1

	q.ErrorCode = uint16(data[pos]) + uint16(data[pos+1])<<8
	pos += 2

	statusVarsLen := int(uint8(data[pos]) + uint8(data[pos+1])<<8)
	pos += 2

	q.StatusVars = string(data[pos : pos+statusVarsLen])
	pos += statusVarsLen

	q.Schema = string(data[pos : pos+schemaLen])
	pos += schemaLen

	q.Query = string(data[pos+1:])

	ev.Query = q
	return nil
}
