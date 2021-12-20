package redis_test

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"

	"github.com/JeremyLoy/redis"
)

var crlf = []byte("\r\n")

type TestRedisServer struct {
	listener *net.TCPListener
	data     chan []byte
}

func NewTestRedisServer() *TestRedisServer {
	return &TestRedisServer{
		data: make(chan []byte, 1),
	}
}

func (c *TestRedisServer) Start(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to start TestRedisServer: %v", err)
	}
	c.listener = listener.(*net.TCPListener)
	go func() {
		conn, err := c.listener.Accept()
		if err != nil {
			t.Fatalf("failed to accept TCP conection: %v", err)
		}
		_, err = conn.Write(<-c.data)
		if err != nil {
			t.Logf("error in writing from server: %v", err)
		}
		// TODO need to close conn? or dose closing listener clean it up?
	}()

}

func (c *TestRedisServer) Stop() error {
	return c.listener.Close()
}

func (ts *TestRedisServer) Address() string {
	return ":" + strconv.Itoa(ts.listener.Addr().(*net.TCPAddr).Port)
}

func serverClientPair(t *testing.T) (*TestRedisServer, *redis.Client) {
	t.Helper()
	ts := NewTestRedisServer()
	ts.Start(t)
	t.Cleanup(func() {
		err := ts.Stop()
		if err != nil {
			t.Logf("Failed to stop TestRedisServer: %v", err)
		}
	})
	c, err := redis.New(context.Background(), ts.Address())
	if err != nil {
		t.Fatalf("Failed to connect redis.Client: %v", err)
	}
	t.Cleanup(func() {
		err := c.Close()
		if err != nil {
			t.Logf("Failed to stop redis.Client: %v", err)
		}
	})
	return ts, c
}

func asBulkString(s string) []byte {
	builder := append([]byte(nil), '$')
	builder = append(builder, []byte(strconv.Itoa(len(s)))...)
	builder = append(builder, crlf...)
	builder = append(builder, s...)
	builder = append(builder, crlf...)
	return builder
}

func integrationClient(t *testing.T) *redis.Client {
	t.Helper()
	if os.Getenv("INTEGRATION") == "" {
		t.Skip()
	}
	c, err := redis.New(context.Background(), ":6379")
	if err != nil {
		t.Fatalf("Failed to connect redis.Client: %v", err)
	}
	t.Cleanup(func() {
		err := c.Close()
		if err != nil {
			t.Logf("Failed to stop redis.Client: %v", err)
		}
	})
	return c
}

func TestClient_Get(t *testing.T) {
	ts, c := serverClientPair(t)
	want := "bar"

	ts.data <- asBulkString(want)
	got, err := c.Get(context.Background(), "X")

	if err != nil {
		t.Errorf("Get() error = %v, wantErr %v", err, nil)
		return
	}
	if got != want {
		t.Errorf("Get() got = %v, want %v", got, want)
	}
}

func Test_Integration(t *testing.T) {
	c := integrationClient(t)
	want := "bar"

	got, err := c.Get(context.Background(), "X")

	if err != nil {
		t.Errorf("Get() error = %v, wantErr %v", err, nil)
		return
	}
	if got != want {
		t.Errorf("Get() got = %v, want %v", got, want)
	}
}
