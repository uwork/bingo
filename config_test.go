package main

import (
	"os"
	"strings"
	"testing"
)

func makeTestOpts(conf string) *CliOptions {
	user := "root"
	pass := ""
	host := "127.0.0.1"
	port := 3306
	dest := "http://localhost:8888/"
	genconf := false
	ver := false
	return &CliOptions{&user, &pass, &host, &port, &dest, &conf, &genconf, &ver}
}

func TestLoadConfigDefaults(t *testing.T) {
	opts := makeTestOpts("")
	config, err := LoadConfig(opts)
	if err != nil {
		t.Fatal(err)
	}
	if config.Mysql.User != "root" {
		t.Errorf("expected user root, got %s", config.Mysql.User)
	}
	if config.Mysql.Host != "127.0.0.1" {
		t.Errorf("expected host 127.0.0.1, got %s", config.Mysql.Host)
	}
	if config.Mysql.Port != 3306 {
		t.Errorf("expected port 3306, got %d", config.Mysql.Port)
	}
	if config.Dest != "http://localhost:8888/" {
		t.Errorf("expected dest http://localhost:8888/, got %s", config.Dest)
	}
	if config.Filter.Filters == nil {
		t.Error("expected non-nil Filters slice")
	}
}

func TestLoadConfigWithFile(t *testing.T) {
	content := `{"mysql":{"user":"fileuser","pass":"filepass","host":"db.local","port":3307},"dest":"http://example.com/","filter":{"filters":[]}}`

	tmpFile, err := os.CreateTemp("", "bingo_config_*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString(content)
	tmpFile.Close()

	opts := makeTestOpts(tmpFile.Name())
	config, err := LoadConfig(opts)
	if err != nil {
		t.Fatal(err)
	}
	if config.Mysql.User != "fileuser" {
		t.Errorf("expected fileuser, got %s", config.Mysql.User)
	}
	if config.Mysql.Port != 3307 {
		t.Errorf("expected port 3307, got %d", config.Mysql.Port)
	}
	if config.Mysql.Host != "db.local" {
		t.Errorf("expected host db.local, got %s", config.Mysql.Host)
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	opts := makeTestOpts("/nonexistent/path/config.json")
	_, err := LoadConfig(opts)
	if err == nil {
		t.Error("expected error for non-existent config file")
	}
}

func TestLoadConfigInvalidJSON(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "bingo_config_*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString("this is not valid json {{{")
	tmpFile.Close()

	opts := makeTestOpts(tmpFile.Name())
	_, err = LoadConfig(opts)
	if err == nil {
		t.Error("expected error for invalid JSON config")
	}
}

func TestDumpConfigNoFilter(t *testing.T) {
	opts := makeTestOpts("")
	result, err := DumpConfig(opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) == 0 {
		t.Error("expected non-empty result from DumpConfig")
	}
	// サンプルフィルタが含まれることを確認
	if !strings.Contains(result, "dbname") {
		t.Errorf("expected sample filter in DumpConfig output, got: %s", result)
	}
}

func TestDumpConfigWithExistingFilter(t *testing.T) {
	content := `{"mysql":{"user":"root","pass":"","host":"127.0.0.1","port":3306},"dest":"http://localhost/","filter":{"filters":[{"database":"mydb","table":"mytable","columns":[0,1],"where":{"left":"$$0","op":"=","right":"1"}}]}}`

	tmpFile, err := os.CreateTemp("", "bingo_config_*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.WriteString(content)
	tmpFile.Close()

	opts := makeTestOpts(tmpFile.Name())
	result, err := DumpConfig(opts)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result, "mydb") {
		t.Errorf("expected mydb in DumpConfig output, got: %s", result)
	}
}
