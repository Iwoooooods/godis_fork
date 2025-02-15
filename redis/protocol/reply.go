package protocol

import (
	"bytes"
	"strconv"
)

var (
	// CRLF is the line separator of redis serialization protocol
	CRLF                = "\r\n"
	pongBytes           = []byte("+PONG\r\n")
	okBytes             = []byte("+OK\r\n")
	theOkReply          = new(OkReply)
	nullBulkBytes       = []byte("$-1\r\n")
	emptyMultiBulkBytes = []byte("*0\r\n")
)

type Reply interface {
	ToBytes() []byte
}

// PongReply is +PONG
type PongReply struct{}

// ToBytes marshal redis.Reply
func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

// OkReply is +OK
type OkReply struct{}

// ToBytes marshal redis.Reply
func (r *OkReply) ToBytes() []byte {
	return okBytes
}

// MakeOkReply returns a ok protocol
func MakeOkReply() *OkReply {
	return theOkReply
}

/* ---- Null Bulk Reply ---- */
// NullBulkReply is empty string
type NullBulkReply struct{}

// ToBytes marshal redis.Reply
func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

// MakeNullBulkReply creates a new NullBulkReply
func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// EmptyMultiBulkReply is a empty list
type EmptyMultiBulkReply struct{}

/* ---- Null Bulk List ---- */
// ToBytes marshal redis.Reply
func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

// MakeEmptyMultiBulkReply creates EmptyMultiBulkReply
func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

/* -- Status Reply  -- */
// StatusReply stores a simple status string
type StatusReply struct {
	Status string
}

// Creates a new status reply
func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

// ToBytes implements redis.Reply
func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

// IsOKReply returns true if the given protocol is +OK
func IsOKReply(reply Reply) bool {
	return string(reply.ToBytes()) == "+OK\r\n"
}

/* ---- Bulk Reply ---- */

// BulkReply stores a binary-safe string
type BulkReply struct {
	Arg []byte
}

// MakeBulkReply creates  BulkReply
func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

// ToBytes marshal redis.Reply
func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return nullBulkBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

/* ---- Error Reply ---- */

// ErrorReply is an error and redis.Reply
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// StandardErrReply represents server error
type StandardErrReply struct {
	Status string
}

// MakeErrReply creates StandardErrReply
func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// IsErrorReply returns true if the given protocol is error
func IsErrorReply(reply Reply) bool {
	return reply.ToBytes()[0] == '-'
}

// ToBytes marshal redis.Reply
func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

/* ---- Int Reply ---- */

// IntReply stores an int64 number
type IntReply struct {
	Code int64
}

// MakeIntReply creates int protocol
func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// ToBytes marshal redis.Reply
func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/* ---- Multi Raw Reply ---- */

// MultiRawReply store complex list structure, for example GeoPos command
type MultiRawReply struct {
	Replies []Reply
}

// MakeMultiRawReply creates MultiRawReply
func MakeMultiRawReply(replies []Reply) *MultiRawReply {
	return &MultiRawReply{
		Replies: replies,
	}
}

// ToBytes marshal redis.Reply
func (r *MultiRawReply) ToBytes() []byte {
	argLen := len(r.Replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Replies {
		buf.Write(arg.ToBytes())
	}
	return buf.Bytes()
}

/* ---- Multi Bulk Reply ---- */

// MultiBulkReply stores a list of string
type MultiBulkReply struct {
	Args [][]byte
}

// MakeMultiBulkReply creates MultiBulkReply
func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

// ToBytes marshal redis.Reply
func (r *MultiBulkReply) ToBytes() []byte {
	var buf bytes.Buffer
	//Calculate the length of buffer
	argLen := len(r.Args)
	bufLen := 1 + len(strconv.Itoa(argLen)) + 2
	for _, arg := range r.Args {
		if arg == nil {
			bufLen += 3 + 2
		} else {
			bufLen += 1 + len(strconv.Itoa(len(arg))) + 2 + len(arg) + 2
		}
	}
	//Allocate memory
	buf.Grow(bufLen)
	//Write string step by step,avoid concat strings
	buf.WriteString("*")
	buf.WriteString(strconv.Itoa(argLen))
	buf.WriteString(CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1")
			buf.WriteString(CRLF)
		} else {
			buf.WriteString("$")
			buf.WriteString(strconv.Itoa(len(arg)))
			buf.WriteString(CRLF)
			//Write bytes,avoid slice of byte to string(slicebytetostring)
			buf.Write(arg)
			buf.WriteString(CRLF)
		}
	}
	return buf.Bytes()
}
