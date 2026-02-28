package destination

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Loki は Grafana Loki の Push API へログを送信する転送先
//
// Loki Push API: POST /loki/api/v1/push
// ペイロード形式:
//
//	{
//	  "streams": [{
//	    "stream": {"app": "bingo", ...},
//	    "values": [["<unix_nano>", "<log_line>"]]
//	  }]
//	}
type Loki struct {
	url    string
	labels map[string]string
}

// NewLoki は Loki 転送先を生成する
// labels が nil の場合はデフォルトラベル {"app": "bingo"} を使用する
func NewLoki(url string, labels map[string]string) *Loki {
	if labels == nil {
		labels = map[string]string{"app": "bingo"}
	}
	return &Loki{url: url, labels: labels}
}

type lokiPushRequest struct {
	Streams []lokiStream `json:"streams"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

// Send は JSON データを Loki Push API へ送信する
func (l *Loki) Send(data []byte) error {
	ts := strconv.FormatInt(time.Now().UnixNano(), 10)

	req := lokiPushRequest{
		Streams: []lokiStream{
			{
				Stream: l.labels,
				Values: [][]string{{ts, string(data)}},
			},
		},
	}

	body, err := json.Marshal(req)
	if err != nil {
		return err
	}

	resp, err := http.Post(l.url+"/loki/api/v1/push", "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Loki は成功時に 204 No Content を返す
	if resp.StatusCode != 204 {
		return fmt.Errorf("loki: invalid response status: %d", resp.StatusCode)
	}
	return nil
}

// Close は何もしない
func (l *Loki) Close() error { return nil }
