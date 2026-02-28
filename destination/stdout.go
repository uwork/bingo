package destination

import (
	"fmt"
	"os"
)

// Stdout は標準出力へ JSON を1行ずつ書き出す転送先
// コンテナ / Kubernetes 環境でのログ収集に適している
type Stdout struct{}

// NewStdout は Stdout 転送先を生成する
func NewStdout() *Stdout {
	return &Stdout{}
}

// Send は JSON データを標準出力へ書き出す
func (s *Stdout) Send(data []byte) error {
	_, err := fmt.Fprintf(os.Stdout, "%s\n", data)
	return err
}

// Close は何もしない
func (s *Stdout) Close() error { return nil }
