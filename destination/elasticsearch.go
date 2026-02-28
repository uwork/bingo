package destination

import (
	"bytes"
	"fmt"
	"net/http"
)

// Elasticsearch は Elasticsearch / OpenSearch の Document API へ JSON を送信する転送先
//
// エンドポイント: POST /<index>/_doc
// Elasticsearch 7.x 以降および OpenSearch に対応
type Elasticsearch struct {
	url      string
	index    string
	endpoint string
}

// NewElasticsearch は Elasticsearch / OpenSearch 転送先を生成する
func NewElasticsearch(url, index string) *Elasticsearch {
	return &Elasticsearch{
		url:      url,
		index:    index,
		endpoint: fmt.Sprintf("%s/%s/_doc", url, index),
	}
}

// Send は JSON データを Elasticsearch /_doc へ POST する
func (e *Elasticsearch) Send(data []byte) error {
	resp, err := http.Post(e.endpoint, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 新規作成は 201、既存ドキュメント更新は 200
	if resp.StatusCode != 201 && resp.StatusCode != 200 {
		return fmt.Errorf("elasticsearch: invalid response status: %d", resp.StatusCode)
	}
	return nil
}

// Close は何もしない
func (e *Elasticsearch) Close() error { return nil }
