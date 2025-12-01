package main

import (
	"fmt"
	"sync"
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

func checkArgsCount(command string, args []Value, expected int) *Value {
	if len(args) == expected {
		return nil
	}

	return &Value{
		typ: RespTypeError,
		str: fmt.Sprintf("ERR wrong number of arguments for '%s' command", command),
	}
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
	err_val := checkArgsCount(CmdEcho, args, 1)
	if err_val != nil {
		return *err_val
	}
	return Value{typ: RespTypeBulk, bulk: args[0].bulk}
}

// ===== SET & GET =====

var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	err_val := checkArgsCount(CmdSet, args, 2)
	if err_val != nil {
		return *err_val
	}

	key := args[0].bulk
	value := args[1].bulk

	// Write lock: Allow 1 writer, block readers and other writers
	SETsMu.Lock()
	SETs[key] = value
	SETsMu.Unlock()

	return Value{typ: RespTypeString, str: "OK"}
}

func get(args []Value) Value {
	err_val := checkArgsCount(CmdGet, args, 1)
	if err_val != nil {
		return *err_val
	}

	key := args[0].bulk

	// Read lock: Allow multiple readers, block writers
	SETsMu.RLock()
	value, ok := SETs[key]
	SETsMu.RUnlock()

	if !ok {
		return Value{typ: RespTypeNull}
	}

	return Value{typ: RespTypeBulk, bulk: value}
}

// ===== HSET & HGET =====

var HSETs = map[string]map[string]string{}
var HSETsMu = sync.RWMutex{}

func hset(args []Value) Value {
	err_val := checkArgsCount(CmdHSet, args, 3)
	if err_val != nil {
		return *err_val
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

	return Value{typ: RespTypeString, str: "OK"}
}

func hget(args []Value) Value {
	err_val := checkArgsCount(CmdHGet, args, 2)
	if err_val != nil {
		return *err_val
	}

	hash := args[0].bulk
	key := args[1].bulk

	HSETsMu.RLock()
	value, ok := HSETs[hash][key]
	HSETsMu.RUnlock()

	if !ok {
		return Value{typ: RespTypeNull}
	}

	return Value{typ: RespTypeBulk, bulk: value}
}
