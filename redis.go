package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

var crlf = []byte("\r\n")

// Error is a type used to distinguish between i/o errors and errors from Redis itself.
// See https://redis.io/topics/protocol#resp-errors for more info
type Error struct {
	msg string
}

func (e Error) Error() string {
	return e.msg
}

// A Client represents a single connection to Redis. It should be constructed with New. It is not safe for concurrent access.
type Client struct {
	dialer  net.Dialer
	conn    net.Conn
	address string
}

// New creates a new Redis Client at the given address. It does not handle authentication at this time.
func New(ctx context.Context, address string) (*Client, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	return &Client{
		address: address,
		conn:    conn,
		dialer:  dialer,
	}, nil
}

// Close closes the underlying net.Conn
func (c *Client) Close() error {
	return c.conn.Close()
}

// Get the value of the given key. If you wish to distinguish between a nil or empty string, check the exists bool.
// If you solely wish to check for existence, you should use Exists instead.
func (c *Client) Get(ctx context.Context, key string) (value string, exists bool, err error) {
	// Using named return values for documentation clarity, but I don't want to deal with it
	// in the code because it's a messy feature. The real Get is implemented in get
	return c.get(ctx, key)
}

func (c *Client) Set(ctx context.Context, key string, value string) error {
	if deadline, set := ctx.Deadline(); set {
		if err := c.conn.SetDeadline(deadline); err != nil {
			return err
		}
	}

	_, err := c.conn.Write(command(fmt.Sprintf("SET %s %s", key, value)))
	if err != nil {
		return err
	}
	reader := bufio.NewReader(c.conn)
	msgType, err := reader.ReadByte()
	if err != nil {
		return err
	}

	switch msgType {
	case '-':
		return readErrorMessage(reader)
	case '+':
		ok, err := readSimpleString(reader)
		if ok != "OK" {
			return fmt.Errorf("redis: expected OK from Redis but got: %v", ok)
		}
		return err
	case '$':
		_, _, err := readBulkString(reader)
		return err
	default:
		return fmt.Errorf("redis: unexpected message type %v", msgType)
	}
}

func (c *Client) get(ctx context.Context, key string) (string, bool, error) {
	if deadline, set := ctx.Deadline(); set {
		if err := c.conn.SetDeadline(deadline); err != nil {
			return "", false, err
		}
	}

	_, err := c.conn.Write(command("GET " + key))
	if err != nil {
		return "", false, err
	}

	reader := bufio.NewReader(c.conn)
	msgType, err := reader.ReadByte()
	if err != nil {
		return "", false, err
	}

	switch msgType {
	case '-':
		return "", false, readErrorMessage(reader)
	case '$':
		return readBulkString(reader)
	default:
		return "", false, fmt.Errorf("redis: unexpected message type %v", msgType)
	}
}

// either successfully reads the error message, returning an Error, or returns the i/o error
func readErrorMessage(reader *bufio.Reader) error {
	errMsg, err := reader.ReadString('\n')
	if err != nil {
		return err
	}
	return errors.New(errMsg[0 : len(errMsg)-2])
}

func readSimpleString(reader *bufio.Reader) (string, error) {
	simpleString, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return simpleString[0 : len(simpleString)-2], nil
}

func readBulkString(reader *bufio.Reader) (string, bool, error) {
	sizeS, err := reader.ReadString('\n')
	if err != nil {
		return "", false, err
	}
	sizeS = sizeS[0 : len(sizeS)-2] // drop crlf
	size, err := strconv.Atoi(sizeS)
	if err != nil {
		return "", false, err
	}
	switch size {
	case 0:
		_, err := reader.Discard(2)
		if err != nil {
			return "", false, err
		}
		return "", true, nil
	case -1:
		// no need to Discard, ReadString ate the CRLF
		return "", false, err
	default:
		msg := make([]byte, size+2) // for crlf. Alternatively reader.Discard(2) but that introduces another err check
		_, err = io.ReadFull(reader, msg)
		if err != nil {
			return "", false, err
		}
		// discard crlf
		return string(msg[0 : len(msg)-2]), true, nil
	}
}

func command(s string) []byte {
	var builder []byte
	ss := strings.Split(s, " ")
	builder = appendArrayToken(builder, len(ss))
	for _, s := range ss {
		builder = appendBulkString(builder, s)
	}
	return builder
}

func appendArrayToken(builder []byte, count int) []byte {
	builder = append(builder, '*')
	builder = append(builder, []byte(strconv.Itoa(count))...)
	builder = append(builder, crlf...)
	return builder
}

func appendBulkString(builder []byte, s string) []byte {
	builder = append(builder, '$')
	builder = append(builder, []byte(strconv.Itoa(len(s)))...)
	builder = append(builder, crlf...)
	builder = append(builder, s...)
	builder = append(builder, crlf...)
	return builder
}
