package redis

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

var crlf = []byte("\r\n")

type Client struct {
	dialer  net.Dialer
	conn    net.Conn
	address string
}

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

func (c *Client) Get(ctx context.Context, key string) (string, error) {

	deadline, _ := ctx.Deadline()
	err := c.conn.SetDeadline(deadline)
	if err != nil {
		return "", err
	}
	_, err = c.conn.Write(command("GET " + key))
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(c.conn)

	if !scanner.Scan() || scanner.Err() != nil {
		return "", scanner.Err()
	}
	token := scanner.Text()
	switch token[0] {
	case '-':
		return "", fmt.Errorf("redis: %v", token[1:])
	case '$':
		bulk, err := getBulkString(token, scanner)
		if bulk == nil {
			return "", err
		}
		return *bulk, err
	default:
		c.conn.Close()
		return "", fmt.Errorf("redis: unexpected response %v", token[1:])
	}
}

func getArray(token string, scanner *bufio.Scanner) ([]interface{}, error) {
	items, err := strconv.Atoi(token[1:])
	if err != nil {
		return nil, err
	}
	if items == -1 {
		return nil, nil
	}
	if items == 0 {
		return []interface{}{}, nil
	}
	var arr []interface{}
	for i := 0; i < items; i++ {
		scanner.Scan()
		if scanner.Err() != nil {
			log.Fatalln(scanner.Err())
		}
		token := scanner.Text()
		switch token[0] {
		case '+':
			arr = append(arr, token[1:])
		case ':':
			ii, _ := strconv.Atoi(token)
			arr = append(arr, ii)
		case '$':
			bulk, _ := getBulkString(token, scanner)
			arr = append(arr, *bulk)
		default:
			log.Fatalln("unknown RESP response ", token)
		}
	}
	return arr, nil
}

func getBulkString(s string, scanner *bufio.Scanner) (*string, error) {
	i, err := strconv.Atoi(s[1:])
	if err != nil {
		return nil, err
	}
	if i == -1 {
		return nil, nil
	}
	if i == 0 {
		return new(string), nil
	} else {
		scanner.Scan()
		if scanner.Err() != nil {
			return nil, scanner.Err()
		}
		s := scanner.Text()
		return &s, nil
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
