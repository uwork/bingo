package mysql

import (
	"bufio"
	"fmt"
	"github.com/uwork/bingo/mysql/binlog"
	"net"
	"strconv"
)

type Conn struct {
	nc           net.Conn
	r            *bufio.Reader
	w            *bufio.Writer
	capabilities int
	sequence     uint
	warnings     uint
	status       uint

	binlogParser *binlog.BinlogParser
}

func Open(user string, pass string, host string, port int) (*Conn, error) {

	// connect to mysql server.
	myconn, err := net.Dial("tcp", host+":"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}

	// set KeepAlive
	if tc, ok := myconn.(*net.TCPConn); ok {
		if err := tc.SetKeepAlive(true); err != nil {
			myconn.Close()
			return nil, err
		}
	}

	conn := &Conn{}
	conn.nc = myconn
	conn.r = bufio.NewReader(myconn)
	conn.w = bufio.NewWriter(myconn)

	err = conn.handshake(user, pass)
	if err != nil {
		return nil, fmt.Errorf("handshake error: %s", err)
	}

	return conn, nil
}
