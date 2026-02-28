package destination

import (
	"os"
	"sync"
)

// File はローカルファイルへ JSON を1行ずつ追記する転送先
type File struct {
	path string
	f    *os.File
	mu   sync.Mutex
}

// NewFile は File 転送先を生成する
// ファイルは追記モードで開かれ、存在しない場合は作成される
func NewFile(path string) (*File, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &File{path: path, f: f}, nil
}

// Send は JSON データをファイルへ1行追記する
func (f *File) Send(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err := f.f.Write(append(data, '\n'))
	return err
}

// Close はファイルをクローズする
func (f *File) Close() error {
	return f.f.Close()
}
