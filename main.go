package main

import (
	"flag"
	"fmt"
	"github.com/uwork/bingo/mysql"
	"github.com/uwork/bingo/mysql/binlog"
	"log"
	"os"
	"strconv"
	"time"
)

var version = "1.0.0"

type CliOptions struct {
	user    *string
	pass    *string
	host    *string
	port    *int
	dest    *string
	conf    *string
	genconf *bool
	version *bool
}

func main() {
	opts := &CliOptions{
		flag.String("u", "root", "mysql user"),
		flag.String("p", "", "mysql password"),
		flag.String("h", "127.0.0.1", "mysql server ip address"),
		flag.Int("P", 3306, "mysql server port"),
		flag.String("d", "http://localhost:8888/bingo.data", "destinate for binlog data."),
		flag.String("c", "", "config file path"),
		flag.Bool("genconf", false, "generate config."),
		flag.Bool("v", false, "show version"),
	}
	flag.Parse()

	var result int
	if *opts.version {
		result = doVersion()
	} else if *opts.genconf {
		result = doDumpConfig(opts)
	} else {
		result = doStartBinlogRead(opts)
	}
	os.Exit(result)
}

func doStartBinlogRead(opts *CliOptions) int {
	conf, err := LoadConfig(opts)
	conn, err := mysql.Open(conf.Mysql.User, conf.Mysql.Pass, conf.Mysql.Host, conf.Mysql.Port)
	if err != nil {
		log.Fatal("error: ", err)
	} else {
		log.Printf("connected to mysql(%s@%s:%d)\n", conf.Mysql.User, conf.Mysql.Host, conf.Mysql.Port)
	}

	rs, err := conn.Query("show master logs")
	if err != nil {
		log.Fatal("error:", err)
	}

	lastRow := len(rs.Rows) - 1
	binlogFile := rs.Rows[lastRow].Values[0].Value
	binlogPos, err := strconv.Atoi(rs.Rows[lastRow].Values[1].Value)
	if err != nil {
		log.Fatal("error: ", err)
	}

	err = conn.DumpBinlog(binlogFile, binlogPos, func(ev *binlog.BinlogEvent) error {
		if nil != ev.Rows && 0 < len(ev.Rows.Rows) {
			data, err := conf.Filter.FilterEvent(ev)
			if err != nil {
				log.Println("data filter failure: ", err)
			}

			if data != nil {
				err = PostBinary(conf.Dest, data)
				if err != nil {
					log.Println("data trans failure: ", err)
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal("error: ", err)
	}

	err = conn.Quit()
	if err != nil {
		log.Fatal("error:", err)
	}

	time.Sleep(3 * time.Second)

	return 0
}

// 設定を出力する
func doDumpConfig(opts *CliOptions) int {
	json, err := DumpConfig(opts)
	if err != nil {
		fmt.Println(err)
		return 1
	}

	fmt.Println(json)
	return 0
}

// バージョンを表示
func doVersion() int {
	fmt.Println("bingo v", version)
	return 0
}
