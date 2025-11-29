package main

import "sync"

// Map commands to handlers
var Handlers = map[string]func([]Value) Value{
	"PING": ping,
	"SET":  set,
	"GET":  get,
}

// ===== PING =====
func ping(args []Value) Value {
	reply := "PONG"
	if len(args) > 0 {
		reply = args[0].bulk
	}
	return Value{typ: RespTypeString, str: reply}
}

// ===== SET & GET =====
var SETs = map[string]string{}
var SETsMu = sync.RWMutex{}

func set(args []Value) Value {
	if len(args) != 2 {
		return Value{typ: RespTypeError, str: "ERR wrong number of arguments for 'set' command"}
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
	if len(args) != 1 {
		return Value{typ: RespTypeError, str: "ERR wrong number of arguments for 'get' command"}
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
