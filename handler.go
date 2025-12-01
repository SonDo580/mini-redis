package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	CmdPing = "PING"
	CmdEcho = "ECHO"
	CmdSet  = "SET"
	CmdGet  = "GET"
	CmdHSet = "HSET"
	CmdHGet = "HGET"
)

// Map commands to handlers
var Handlers = map[string]func([]Value) Value{
	CmdPing: ping,
	CmdEcho: echo,
	CmdSet:  set,
	CmdGet:  get,
	CmdHSet: hset,
	CmdHGet: hget,
}

// ==== Helpers =====

func okVal() Value {
	return Value{typ: RespTypeString, str: "OK"}
}

func nullVal() Value {
	return Value{typ: RespTypeNull}
}

func errVal(str string) Value {
	return Value{typ: RespTypeError, str: str}
}

func argsCountErrVal(command string) Value {
	return errVal(
		fmt.Sprintf("ERR wrong number of arguments for '%s' command", command),
	)
}

func syntaxErrVal() Value {
	return errVal("ERR syntax error")
}

func intErrVal() Value {
	return errVal("ERR value is not an integer or out of range")
}

// ===== PING =====

func ping(args []Value) Value {
	reply := "PONG"
	if len(args) > 0 {
		reply = args[0].bulk
	}
	return Value{typ: RespTypeString, str: reply}
}

// ===== ECHO =====

func echo(args []Value) Value {
	if len(args) != 1 {
		return argsCountErrVal(CmdEcho)
	}
	return Value{typ: RespTypeBulk, bulk: args[0].bulk}
}

// ===== SET & GET =====

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

// Side note:
// - Write lock: Allow 1 writer, block readers and other writers
// - Read lock: Allow multiple readers, block writers

// Stores expiration time (Unix ms) for keys in SETs
var SETsExpirations = map[string]int64{}
var SETsExpirationsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) < 2 {
		return argsCountErrVal(CmdSet)
	}

	key := args[0].bulk
	value := args[1].bulk

	var expiresAtMs int64 = 0 // default: no expiration

	// Parse optional arguments
	for i := 2; i < len(args); i++ {
		option := strings.ToUpper(args[i].bulk)

		switch option {
		case "PX":
			if i+1 > len(args) {
				return syntaxErrVal()
			}
			ms, err := strconv.ParseInt(args[i+1].bulk, 10, 64)
			if err != nil || ms <= 0 {
				return intErrVal()
			}
			expiresAtMs = time.Now().UnixMilli() + ms
			i++

		case "EX":
			if i+1 > len(args) {
				return syntaxErrVal()
			}
			seconds, err := strconv.ParseInt(args[i+1].bulk, 10, 64)
			if err != nil || seconds <= 0 {
				return intErrVal()
			}
			expiresAtMs = time.Now().UnixMilli() + seconds*1000
			i++

		default:
			return syntaxErrVal()
		}
	}

	// Set value
	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	// Set expiration if specified
	if expiresAtMs > 0 {
		SETsExpirationsMu.Lock()
		SETsExpirations[key] = expiresAtMs
		SETsExpirationsMu.Unlock()
	}

	return okVal()
}

func get(args []Value) Value {
	if len(args) != 1 {
		return argsCountErrVal(CmdGet)
	}

	key := args[0].bulk

	SETsExpirationsMu.RLock()
	expiresAtMs, ok := SETsExpirations[key]
	SETsExpirationsMu.RUnlock()

	// Handle expired key: delete from both maps and return null
	if ok && time.Now().UnixMilli() > expiresAtMs {
		SETsMu.Lock()
		delete(SETs, key)
		SETsMu.Unlock()

		SETsExpirationsMu.Lock()
		delete(SETsExpirations, key)
		SETsExpirationsMu.Unlock()

		return nullVal()
	}

	// Read value
	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return nullVal()
	}

	return Value{typ: RespTypeBulk, bulk: value}
}

// ===== HSET & HGET =====

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
	if len(args) != 3 {
		return argsCountErrVal(CmdHSet)
	}

	hash := args[0].bulk
	key := args[1].bulk
	value := args[2].bulk

	HSETsMu.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMu.Unlock()

	return okVal()
}

func hget(args []Value) Value {
	if len(args) != 2 {
		return argsCountErrVal(CmdHGet)
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return nullVal()
	}

	return Value{typ: RespTypeBulk, bulk: value}
}
