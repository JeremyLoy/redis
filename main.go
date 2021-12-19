package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

var crlf = []byte("\r\n")

type Client struct {
	dialer net.Dialer
	conn   net.Conn
}

func main() {
	dialer := net.Dialer{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", ":6379")

	if err != nil {
		log.Fatalln(err)
	}
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		log.Fatalln(err)
	}
	n, err := conn.Write(command("LRANGE a 0 -1"))
	fmt.Println(n)
	fmt.Println(err)

	buf := make([]byte, 1024)
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(buf, -1)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println(scanner.Scan())
	if scanner.Err() != nil {
		log.Fatalln(scanner.Err())
	}
	token := scanner.Text()
	switch token[0] {
	case '+':
		fmt.Println("Was Simple string type: ", token[1:])
	case '-':
		fmt.Println("Was Error type: ", token[1:])
	case ':':
		fmt.Println("Was Integer type: ", token[1:])
	case '$':
		bulk, err := getBulkString(token, scanner)
		fmt.Println("Was Bulk String type: ", bulk)
		fmt.Println("err was", err)
	case '*':
		arr, err := getArray(token, scanner)
		fmt.Println("Was Array type: ", arr)
		fmt.Println("err was", err)
	default:
		log.Fatalln("unknown RESP response ", token)
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
