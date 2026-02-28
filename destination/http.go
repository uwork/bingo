package destination

import (
	"bytes"
	"fmt"
	"net/http"
)

// HTTP は Fluentd など HTTP エンドポイントへ JSON を POST する転送先
type HTTP struct {
	url string
}

// NewHTTP は HTTP 転送先を生成する
func NewHTTP(url string) *HTTP {
	return &HTTP{url: url}
}

// Send は JSON データを HTTP POST する
func (h *HTTP) Send(data []byte) error {
	resp, err := http.Post(h.url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("http: invalid response status: %d", resp.StatusCode)
	}
	return nil
}

// Close は何もしない（HTTP はコネクションレス）
func (h *HTTP) Close() error { return nil }
