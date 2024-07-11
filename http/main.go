/*
 * Copyright (c) 2024. Shuojiang Liu.
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/LiuShuoJiang/betadb"
	"log"
	"net/http"
	"os"
)

var db *betadb.Database

func init() {
	// initialize the Database instance
	var err error
	options := betadb.DefaultOptions
	directory, _ := os.MkdirTemp("", "betadb-http")
	options.DirectoryPath = directory

	db, err = betadb.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open database: %v", err))
	}
}

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "Method is not allowed", http.StatusMethodNotAllowed)
		return
	}

	var keyValue map[string]string

	if err := json.NewDecoder(request.Body).Decode(&keyValue); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range keyValue {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put key value to database: %v\n", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method is not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")

	value, err := db.Get([]byte(key))

	if err != nil && !errors.Is(err, betadb.ErrKeyNotFound) {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get key value from database: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "Method is not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")

	err := db.Delete([]byte(key))

	if err != nil && !errors.Is(err, betadb.ErrKeyIsEmpty) {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get key value in database: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("Delete OK")
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method is not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")

	var result []string
	for _, k := range keys {
		result = append(result, string(k))
	}

	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method is not allowed", http.StatusMethodNotAllowed)
		return
	}

	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	// register the handle methods
	// example command: curl -X POST localhost:8989/betadb/put -d '{"name1": "value1", "name2": "value2"}'
	http.HandleFunc("/betadb/put", handlePut)
	// example command: curl "localhost:8989/betadb/get?key=name1"
	http.HandleFunc("/betadb/get", handleGet)
	// example command: curl -X DELETE localhost:8989/betadb/delete?key=name1
	http.HandleFunc("/betadb/delete", handleDelete)
	// example command: curl "localhost:8989/betadb/listkeys"
	http.HandleFunc("/betadb/listkeys", handleListKeys)
	// example command: curl "localhost:8989/betadb/stat"
	http.HandleFunc("/betadb/stat", handleStat)

	_ = http.ListenAndServe("localhost:8989", nil)
}
