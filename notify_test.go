package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/uwork/bingo/destination"
)

func TestHTTPDestinationSuccess(t *testing.T) {
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

	d := destination.NewHTTP(ts.URL)
	if err := d.Send([]byte(`{"test":"data"}`)); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPDestinationNon200(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	d := destination.NewHTTP(ts.URL)
	// 旧 PostBinary は非200でもエラーを返さなかった（既知バグ）が、
	// 新実装では正しくエラーを返す
	if err := d.Send([]byte(`{}`)); err == nil {
		t.Error("expected error for non-200 response")
	}
}

func TestHTTPDestinationError(t *testing.T) {
	d := destination.NewHTTP("http://127.0.0.1:1")
	if err := d.Send([]byte(`{}`)); err == nil {
		t.Error("expected error for unreachable URL")
	}
}

func TestStdoutDestination(t *testing.T) {
	d := destination.NewStdout()
	if err := d.Send([]byte(`{"key":"value"}`)); err != nil {
		t.Fatal(err)
	}
}

func TestMultiDestination(t *testing.T) {
	var received [][]byte
	ts1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received = append(received, body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts1.Close()

	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received = append(received, body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts2.Close()

	multi := destination.NewMulti([]destination.Destination{
		destination.NewHTTP(ts1.URL),
		destination.NewHTTP(ts2.URL),
	})

	data := []byte(`{"multi":"test"}`)
	if err := multi.Send(data); err != nil {
		t.Fatal(err)
	}

	if len(received) != 2 {
		t.Errorf("expected 2 sends, got %d", len(received))
	}
}
