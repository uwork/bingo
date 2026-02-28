package main

import (
	"bytes"
	"encoding/json"
	"github.com/uwork/bingo/destination"
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
	Mysql  MysqlConfig            `json:"mysql"`
	Dest   string                 `json:"dest,omitempty"`   // 後方互換: 旧設定の単一 HTTP 転送先
	Dests  []destination.Config   `json:"destinations,omitempty"` // 新: 複数転送先
	Filter filter.FilterConfig    `json:"filter"`
}

// BuildDestination は設定から転送先を構築して返す
// destinations が指定されていればそちらを優先し、なければ dest を HTTP 転送先として使う
func (c *Config) BuildDestination() (destination.Destination, error) {
	if len(c.Dests) > 0 {
		dests := make([]destination.Destination, 0, len(c.Dests))
		for _, dc := range c.Dests {
			d, err := destination.New(dc)
			if err != nil {
				return nil, err
			}
			dests = append(dests, d)
		}
		if len(dests) == 1 {
			return dests[0], nil
		}
		return destination.NewMulti(dests), nil
	}

	// 後方互換: dest が指定されていれば HTTP 転送先として使う
	url := c.Dest
	if url == "" {
		url = "http://localhost:8888/bingo.data"
	}
	return destination.NewHTTP(url), nil
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

	// destinations サンプル（未設定の場合のみ）
	if len(config.Dests) == 0 && config.Dest == "" {
		config.Dests = []destination.Config{
			{Type: "http", URL: "http://localhost:8888/bingo.data"},
			{Type: "stdout"},
			{Type: "loki", URL: "http://localhost:3100", Labels: map[string]string{"app": "bingo"}},
			{Type: "elasticsearch", URL: "http://localhost:9200", Index: "binlog"},
			{Type: "file", Path: "/var/log/bingo.jsonl"},
		}
	}

	// filter サンプル
	if 0 == len(config.Filter.Filters) {
		f := filter.Filter{
			"dbname",
			"tablename",
			[]int{0, 1, 2},
			filter.NewExpression("$$0", "=", "1"),
		}
		config.Filter.Filters = append(config.Filter.Filters, f)
	}

	jsonb, err := json.Marshal(config)
	if err != nil {
		return "", err
	}

	buf := new(bytes.Buffer)
	json.Indent(buf, jsonb, "", "  ")
	return buf.String(), nil
}
