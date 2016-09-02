package mysql

import (
	"fmt"
	"github.com/uwork/bingo/mysql/binlog"
	"github.com/uwork/bingo/util"
	"log"
	"time"
)

const (
	// mysql-devel: include/mysql/my_command.h
	COM_SLEEP = iota
	COM_QUIT
	COM_INIT_DB
	COM_QUERY
	COM_FIELD_LIST
	COM_CREATE_DB
	COM_DROP_DB
	COM_REFRESH
	COM_SHUTDOWN
	COM_STATISTICS
	COM_PROCESS_INFO
	COM_CONNECT
	COM_PROCESS_KILL
	COM_DEBUG
	COM_PING
	COM_TIME
	COM_DELAYED_INSERT
	COM_CHANGE_USER
	COM_BINLOG_DUMP
	COM_TABLE_DUMP
	COM_CONNECT_OUT
	COM_REGISTER_SLAVE
	COM_STMT_PREPARE
	COM_STMT_EXECUTE
	COM_STMT_SEND_LONG_DATA
	COM_STMT_CLOSE
	COM_STMT_RESET
	COM_SET_OPTION
	COM_STMT_FETCH
	COM_DAEMON
	COM_BINLOG_DUMP_GTID
	COM_RESET_CONNECTION
)

const (
	serverMoreResultsExists = 0x0008
)

type Value struct {
	Value  string
	IsNull bool
}

type Row struct {
	Values []Value
}

type Column struct {
	catalog  string
	schema   string
	table    string
	orgTable string
	name     string
	orgName  string
	filter1  byte
	charaset uint16
	colLen   int
	colType  byte
	flags    uint16
	decimals byte
	filter2  uint16
}

type ResultSet struct {
	Columns []Column
	Rows    []Row
}

type OnEvent func(*binlog.BinlogEvent) error

func (c *Conn) DumpBinlog(binlogFile string, binlogPos int, callback OnEvent) error {

	if c.binlogParser == nil {
		c.binlogParser = &binlog.BinlogParser{}
		c.binlogParser.TableMaps = map[uint64]*binlog.BinlogEventTableMap{}
	}

	args := []byte{}
	args = append(args, util.IntToBytes(binlogPos)...)
	args = append(args, []byte{0x00, 0x00}...)    // BLOCKING_IO(0) NON_BLOCKING_IO(1)
	args = append(args, util.IntToBytes(0x20)...) // FIXME: serverId
	args = append(args, []byte(binlogFile)...)

	err := c.commandBinary(COM_BINLOG_DUMP, args)
	if err != nil {
		return err
	}

	err = c.readResultPacket()
	if err != nil {
		return err
	}

	// first event is format description
	_, err = c.dumpNextBinlog()
	if err != nil {
		return err
	}

	log.Println("start reading binlog")
	log.Println("    Binlog Version: ", c.binlogParser.Description.BinlogVersion)
	log.Println("    Server Version: ", c.binlogParser.Description.ServerVersion)

	for {
		c.nc.SetDeadline(time.Now().Add(24 * 365 * time.Hour))

		// read next binlog
		ev, err := c.dumpNextBinlog()
		if err != nil {
			return err
		}

		err = callback(ev)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Conn) Query(sql string) (*ResultSet, error) {
	err := c.command(COM_QUERY, sql)
	if err != nil {
		return nil, err
	}

	rs, err := c.readResultSetPacket()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (c *Conn) readResultSetPacket() (*ResultSet, error) {

	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == pOK { // OK packet
		return nil, nil
	}

	if data[0] == pERR { // ERR packet
		return nil, c.errorPacketToString(data)
	}

	columns, _ := util.ReadLengthEncodedInteger(data)

	rs := &ResultSet{}
	rs.Columns = make([]Column, columns)

	// read column definitions.
	for i := uint64(0); ; i++ {
		data, err = c.readPacket()
		if err != nil {
			return nil, err
		}

		if data[0] == pEOF {
			c.readEOFPacket(data)
			if i == columns {
				break
			}
			return nil, fmt.Errorf("invalid columns definition packet")
		}
		if data[0] == pERR {
			return nil, c.errorPacketToString(data)
		}

		col, _, err := c.readColumnDefinition(data)
		if err != nil {
			return nil, err
		}

		rs.Columns[i] = col
	}

	// read column values.
	isMoreResults := true
	for rows := 0; isMoreResults; rows++ {
		row := Row{[]Value{}}

		data, err = c.readPacket()
		if err != nil {
			return nil, err
		}

		pos := 0
		for i := uint64(0); i < columns; i++ {
			if data[pos] == pEOF {
				c.readEOFPacket(data[pos:])
				if i == columns {
					break
				}
				if c.status&serverMoreResultsExists == 0 {
					isMoreResults = false
					break
				}
				return nil, fmt.Errorf("invalid values row packet")
			}
			if data[pos] == pERR {
				return nil, c.errorPacketToString(data[pos:])
			}

			if data[pos] != 0xfb {
				// string
				str, n := util.ReadLengthEncodedString(data[pos:])
				row.Values = append(row.Values, Value{str, false})
				pos += n
			} else {
				// null value
				row.Values = append(row.Values, Value{"", true})
				pos += 1
			}
		}

		if 0 < len(row.Values) {
			rs.Rows = append(rs.Rows, row)
		}
	}

	return rs, err
}

func (c *Conn) readColumnDefinition(data []byte) (Column, int, error) {
	col := Column{}

	catalog, pos := util.ReadLengthEncodedString(data)
	col.catalog = catalog

	db, n := util.ReadLengthEncodedString(data[pos:])
	col.schema = db
	pos += n

	table, n := util.ReadLengthEncodedString(data[pos:])
	col.table = table
	pos += n

	orgTable, n := util.ReadLengthEncodedString(data[pos:])
	col.orgTable = orgTable
	pos += n

	name, n := util.ReadLengthEncodedString(data[pos:])
	col.name = name
	pos += n

	col.filter1 = data[pos]
	pos += 1

	col.charaset = uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2

	col.colLen = util.BytesToInt(data[pos : pos+4])
	pos += 4

	col.colType = data[pos]
	pos += 1

	col.flags = uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2

	col.decimals = data[pos]
	pos += 1

	col.filter2 = uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2

	return col, pos, nil
}

// no result query
func (c *Conn) UpdateQuery(sql string) error {
	err := c.command(COM_QUERY, sql)
	if err != nil {
		return err
	}

	err = c.readResultPacket()
	if err != nil {
		return err
	}

	return nil
}

// exit mysql
func (c *Conn) Quit() error {
	err := c.commandSimple(COM_QUIT)
	if err != nil {
		return err
	}

	err = c.readResultPacket()
	if err != nil {
		return err
	}

	return nil
}
