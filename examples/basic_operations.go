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
	"fmt"
	"github.com/LiuShuoJiang/betadb"
)

func main() {
	options := betadb.DefaultOptions
	options.DirectoryPath = "/tmp/betadb"

	db, err := betadb.Open(options)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("betadb"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val =", string(val))

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}
}
