package main

// Map commands to handlers
var Handlers = map[string]func([]Value) Value{
	"PING": ping,
}

func ping(args []Value) Value {
	reply := "PONG"
	if len(args) > 0 {
		reply = args[0].bulk
	}
	return Value{typ: RespTypeString, str: reply}
}
