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
	"errors"
	"fmt"
	"github.com/LiuShuoJiang/betadb"
	"github.com/LiuShuoJiang/betadb/redis"
	"github.com/LiuShuoJiang/betadb/utils"
	"github.com/tidwall/redcon"
	"strings"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BetaDBClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"set":   set,
	"get":   get,
	"hset":  hset,
	"sadd":  sadd,
	"lpush": lpush,
	"zadd":  zadd,
}

type BetaDBClient struct {
	server *BetaDBServer
	db     *redis.RedisDataStructure
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))

	client, _ := conn.Context().(*BetaDBClient)

	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		cmdFunc, ok := supportedCommands[command]
		if !ok {
			conn.WriteError("Err unsupported command: '" + command + "'")
			return
		}

		result, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if errors.Is(err, betadb.ErrKeyNotFound) {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}

		conn.WriteAny(result)
	}
}

func set(cli *BetaDBClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func get(cli *BetaDBClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}

	value, err := cli.db.Get(args[0])
	if err != nil {
		return nil, err
	}

	return value, nil
}

func hset(cli *BetaDBClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}

	var ok = 0
	key, field, value := args[0], args[1], args[2]

	result, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if result {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func sadd(cli *BetaDBClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}

	var ok = 0
	key, member := args[0], args[1]

	result, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if result {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BetaDBClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}

	key, value := args[0], args[1]

	res, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(res), nil
}

func zadd(cli *BetaDBClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]

	result, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if result {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}
