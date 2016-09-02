package mysql

import (
	"github.com/uwork/bingo/mysql/binlog"
)

type BinlogEOFError struct {
}

func (e *BinlogEOFError) Error() string {
	return "End of binlog stream."
}

func (c *Conn) dumpNextBinlog() (*binlog.BinlogEvent, error) {
	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == pERR {
		return nil, c.errorPacketToString(data)
	}

	if data[0] == pEOF {
		return nil, &BinlogEOFError{}
	}

	ev, _, err := c.binlogParser.ParseBinlogEvent(data[1:])
	if err != nil {
		return nil, err
	}

	return ev, err
}
