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

const DefaultPoolSize = 10

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
	pool    chan net.Conn
	address string
}

// New creates a new Redis Client at the given address. It does not handle authentication at this time.
func New(ctx context.Context, address string) (*Client, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return &Client{
		address: address,
		pool:    make(chan net.Conn, DefaultPoolSize),
	}, nil
}

// Close closes all outstanding connections and prevents future operations on Client from succeeding
func (c *Client) Close() error {
	// for conn := range c.pool {
	// 	conn.Close()
	// }
	// TODO figure out how to close channel safely
	return nil
}

func (c *Client) getConn(ctx context.Context) (net.Conn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case conn := <-c.pool:
		deadline, _ := ctx.Deadline()
		if err := conn.SetDeadline(deadline); err != nil {
			_ = conn.Close()
			// Not sure why SetDeadline can fail, but if it does discard the Conn
			// and try again below
		} else {
			return conn, nil
		}
	default:
	}
	return c.dialer.DialContext(ctx, "tcp", c.address)
}

// Set key to hold the string value.
// If key already holds a value, it is overwritten, regardless of its type.
// Any previous time to live associated with the key is discarded on successful SET operation.
func (c *Client) Set(ctx context.Context, key string, value string) error {
	conn, err := c.getConn(ctx)
	if err != nil {
		return err
	}
	defer func() {
		c.pool <- conn
	}()
	_, err = conn.Write(command(fmt.Sprintf("SET %s %s", key, value)))
	if err != nil {
		return err
	}
	reader := bufio.NewReader(conn)
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

// Get the value of the given key. If you wish to distinguish between a nil or empty string, check the exists bool.
// If you solely wish to check for existence, you should use Exists instead.
// An error is returned if the value stored at key is not a string, because GET only handles string values.
func (c *Client) Get(ctx context.Context, key string) (value string, exists bool, err error) {
	// Using named return values for documentation clarity, but I don't want to deal with it
	// in the code because it's a messy feature. The real Get is implemented in get
	return c.get(ctx, key)
}

func (c *Client) get(ctx context.Context, key string) (string, bool, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return "", false, err
	}
	defer func() {
		c.pool <- conn
	}()

	_, err = conn.Write(command("GET " + key))
	if err != nil {
		return "", false, err
	}

	reader := bufio.NewReader(conn)
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
