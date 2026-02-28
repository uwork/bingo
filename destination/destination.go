package destination

import "fmt"

// Destination はログ転送先インターフェース
type Destination interface {
	Send(data []byte) error
	Close() error
}

// Config は転送先1件の設定
type Config struct {
	Type string `json:"type"`

	// HTTP / Fluentd / Loki / Elasticsearch
	URL string `json:"url,omitempty"`

	// Loki 追加ラベル
	Labels map[string]string `json:"labels,omitempty"`

	// Elasticsearch / OpenSearch インデックス名
	Index string `json:"index,omitempty"`

	// ファイル出力先パス
	Path string `json:"path,omitempty"`
}

// New は設定から Destination を生成する
func New(conf Config) (Destination, error) {
	switch conf.Type {
	case "http", "fluentd":
		if conf.URL == "" {
			return nil, fmt.Errorf("destination %q: url は必須です", conf.Type)
		}
		return NewHTTP(conf.URL), nil

	case "stdout":
		return NewStdout(), nil

	case "loki":
		if conf.URL == "" {
			return nil, fmt.Errorf("destination loki: url は必須です")
		}
		return NewLoki(conf.URL, conf.Labels), nil

	case "elasticsearch", "opensearch":
		if conf.URL == "" {
			return nil, fmt.Errorf("destination %q: url は必須です", conf.Type)
		}
		index := conf.Index
		if index == "" {
			index = "binlog"
		}
		return NewElasticsearch(conf.URL, index), nil

	case "file":
		if conf.Path == "" {
			return nil, fmt.Errorf("destination file: path は必須です")
		}
		return NewFile(conf.Path)

	default:
		return nil, fmt.Errorf("未知の転送先タイプ: %q (http/fluentd/stdout/loki/elasticsearch/opensearch/file)", conf.Type)
	}
}

// Multi は複数の Destination へ同時にデータを送信する
type Multi struct {
	dests []Destination
}

// NewMulti は Multi を生成する
func NewMulti(dests []Destination) *Multi {
	return &Multi{dests: dests}
}

// Send は全転送先へ送信し、最後のエラーを返す
func (m *Multi) Send(data []byte) error {
	var lastErr error
	for _, d := range m.dests {
		if err := d.Send(data); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Close は全転送先をクローズする
func (m *Multi) Close() error {
	for _, d := range m.dests {
		d.Close()
	}
	return nil
}
