package hdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

const HOBBIT_STORAGE = "/var/tmp/hdb/"

func Get(table string, key string) (string, error) {
	path := fmt.Sprintf("%s%s", HOBBIT_STORAGE, table)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var js map[string]interface{}
	err = json.Unmarshal([]byte(string(data)), &js)
	if err != nil {
		return "", err
	}
	if value, ok := js[key]; ok {
		s, ok := value.(string)
		if ok {
			return s, nil
		}
	}
	return "", errors.New("Key not found")
}

func Set(table string, key string, value string) error {
	// Just create the table if it doesn't already exist.
	makeTable(table)
	path := fmt.Sprintf("%s%s", HOBBIT_STORAGE, table)
	// Read existing table if it exists.
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var js map[string]interface{}
	err = json.Unmarshal([]byte(string(data)), &js)
	if err != nil {
		return err
	}
	// Set the key and write back as json.
	js[key] = value
	save, _ := json.MarshalIndent(js, "", "  ")
	// First write to .tmp file then rename so operation is atomic.
	tmp := fmt.Sprintf("%s.tmp", path)
	err = os.WriteFile(tmp, []byte(save), 0644)
	if err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func Del(table string, key string) error {
	path := fmt.Sprintf("%s%s", HOBBIT_STORAGE, table)
	// Read existing table if it exists.
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var js map[string]interface{}
	err = json.Unmarshal([]byte(string(data)), &js)
	if err != nil {
		return err
	}
	// Set the key and write back as json.
	delete(js, key)
	if len(js) == 0 {
		// If the table is now empty, just clean it up.
		err = os.Remove(path)
		// Also cleanup empty directories.
		os.Remove(trimSlash(path))
	} else {
		save, _ := json.MarshalIndent(js, "", "  ")
		tmp := fmt.Sprintf("%s.tmp", path)
		err = os.WriteFile(tmp, []byte(save), 0644)
		if err != nil {
			return err
		}
		err = os.Rename(tmp, path)
	}
	return err
}

func Map(table string) map[string]interface{} {
	path := fmt.Sprintf("%s%s", HOBBIT_STORAGE, table)
	// Read existing table if it exists.
	data, err := os.ReadFile(path)
	if err != nil {
		return make(map[string]interface{})
	}
	var js map[string]interface{}
	err = json.Unmarshal([]byte(string(data)), &js)
	if err != nil {
		return make(map[string]interface{})
	}
	return js
}

func makeTable(table string) error {
	if strings.Contains(table, "..") || len(table) > 256 {
		return errors.New("Invalid table name")
	}
	os.MkdirAll(HOBBIT_STORAGE, os.ModePerm)

	path := fmt.Sprintf("%s%s", HOBBIT_STORAGE, table)
	if _, err := os.Stat(path); err == nil {
		// Table already exists; just exit.
		return nil
	} else if errors.Is(err, os.ErrNotExist) {
		// Create subdirectory if table contains slash.
		if strings.Contains(table, "/") {
			os.MkdirAll(trimSlash(path), os.ModePerm)
		}
		// Create an empty table.
		data := []byte("{}")
		tmp := fmt.Sprintf("%s.tmp", path)
		err = os.WriteFile(tmp, data, 0644)
		if err != nil {
			return err
		}
		err = os.Rename(tmp, path)
		if err != nil {
			return err
		}
		return nil
	}
	return errors.New("Error creating table")
}

func trimSlash(s string) string {
	if idx := strings.LastIndex(s, "/"); idx != -1 {
		return s[:idx]
	}
	return s
}
