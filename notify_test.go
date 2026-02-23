package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPostBinarySuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		body, _ := io.ReadAll(r.Body)
		if !bytes.Equal(body, []byte(`{"test":"data"}`)) {
			t.Errorf("unexpected body: %s", body)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	err := PostBinary(ts.URL, []byte(`{"test":"data"}`))
	if err != nil {
		t.Fatal(err)
	}
}

func TestPostBinaryNon200(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	// 非200でもエラーを返さない（既知のバグ: ログのみ）
	err := PostBinary(ts.URL, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
}

func TestPostBinaryError(t *testing.T) {
	// 接続できないURLを使う
	err := PostBinary("http://127.0.0.1:1", []byte(`{}`))
	if err == nil {
		t.Error("expected error for unreachable URL")
	}
}

func TestPostDataSuccess(t *testing.T) {
	var received []byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	err := PostData(ts.URL, map[string]interface{}{"key": "value", "num": 42})
	if err != nil {
		t.Fatal(err)
	}
	if len(received) == 0 {
		t.Error("expected non-empty body")
	}
}

func TestPostDataNon200(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer ts.Close()

	err := PostData(ts.URL, map[string]interface{}{"key": "value"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPostDataError(t *testing.T) {
	err := PostData("http://127.0.0.1:1", map[string]interface{}{"key": "value"})
	if err == nil {
		t.Error("expected error for unreachable URL")
	}
}

func TestPostDataMarshalError(t *testing.T) {
	// json.Marshal できない値 (func) を含むマップ -> エラー
	err := PostData("http://127.0.0.1:1", map[string]interface{}{"fn": func() {}})
	if err == nil {
		t.Error("expected marshal error for non-serializable value")
	}
}
