package main

import (
	"bytes"
	"encoding/json"
	"github.com/uwork/bingo/filter"
	"io/ioutil"
)

type MysqlConfig struct {
	User string `json:"user"`
	Pass string `json:"pass"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

type Config struct {
	Mysql  MysqlConfig         `json:"mysql"`
	Dest   string              `json:"dest"`
	Filter filter.FilterConfig `json:"filter"`
}

func LoadConfig(opts *CliOptions) (Config, error) {
	config := Config{}
	config.Mysql = MysqlConfig{
		*opts.user,
		*opts.pass,
		*opts.host,
		*opts.port,
	}
	config.Dest = *opts.dest
	config.Filter = filter.FilterConfig{
		[]filter.Filter{},
	}

	if 0 < len(*opts.conf) {
		confBytes, err := ioutil.ReadFile(*opts.conf)
		if err != nil {
			return config, err
		}

		err = json.Unmarshal(confBytes, &config)
		if err != nil {
			return config, err
		}
	}
	return config, nil
}

func DumpConfig(opts *CliOptions) (string, error) {
	config, err := LoadConfig(opts)
	if err != nil {
		return "", err
	}

	// filter sample
	if 0 == len(config.Filter.Filters) {
		filter := filter.Filter{
			"dbname",
			"tablename",
			[]int{0, 1, 2},
			filter.NewExpression("$$0", "=", "1"),
		}
		config.Filter.Filters = append(config.Filter.Filters, filter)
	}

	jsonb, err := json.Marshal(config)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	json.Indent(buf, jsonb, "", "  ")
	return buf.String(), nil
}
