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
	"github.com/LiuShuoJiang/betadb"
	"github.com/LiuShuoJiang/betadb/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

// address can be modified to a custom value
const addr = "127.0.0.1:6380"

type BetaDBServer struct {
	dbs    map[int]*redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	// open redis data structure service
	redisDataStructure, err := redis.NewRedisDataStructure(betadb.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// initialize BetaDBServer
	betadbServer := &BetaDBServer{
		dbs: make(map[int]*redis.RedisDataStructure),
	}
	betadbServer.dbs[0] = redisDataStructure

	// initialize a Redis server
	betadbServer.server = redcon.NewServer(addr, execClientCommand, betadbServer.accept, betadbServer.close)
	betadbServer.listen()
}

func (bs *BetaDBServer) listen() {
	log.Println("BetaDB server is running, ready for accepting connections...")
	_ = bs.server.ListenAndServe()
}

func (bs *BetaDBServer) accept(conn redcon.Conn) bool {
	cli := new(BetaDBClient)
	bs.mu.Lock()
	defer bs.mu.Unlock()

	cli.server = bs
	// here we set the database for client to dbs[0] for simplicity
	// the real scenario may be more complicated
	cli.db = bs.dbs[0]

	conn.SetContext(cli)

	return true
}

func (bs *BetaDBServer) close(conn redcon.Conn, err error) {
	for _, db := range bs.dbs {
		_ = db.Close()
	}
}
