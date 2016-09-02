package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"log"
	"net/http"
)

func PostBinary(url string, data []byte) error {
	buf := bufio.NewReader(bytes.NewBuffer(data))
	resp, err := http.Post(url, "application/json", buf)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		log.Println("invalid http response: ", 200)
	}

	return nil
}

func PostData(url string, data map[string]interface{}) error {
	json, err := json.Marshal(data)
	if err != nil {
		return err
	}

	buf := bufio.NewReader(bytes.NewBuffer(json))
	resp, err := http.Post(url, "application/json", buf)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		log.Println("invalid http response: ", 200)
	}

	return nil
}
