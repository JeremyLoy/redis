package redis

import (
	"context"
	"errors"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
)

var nullString = []byte("$-1\r\n")
var okString = []byte("+OK\r\n")

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
		for d := range c.data {
			_, err = conn.Write(d)
			if err != nil {
				t.Logf("error in writing from server: %v", err)
			}
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

func serverClientPair(t *testing.T) (*TestRedisServer, *Client) {
	t.Helper()
	ts := NewTestRedisServer()
	ts.Start(t)
	t.Cleanup(func() {
		err := ts.Stop()
		if err != nil {
			t.Logf("Failed to stop TestRedisServer: %v", err)
		}
		close(ts.data)
	})
	c, err := New(context.Background(), ts.Address())
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

func asSimpleErrorString(s string) []byte {
	builder := append([]byte(nil), '-')
	builder = append(builder, s...)
	builder = append(builder, crlf...)
	return builder
}

func asSimpleString(s string) []byte {
	builder := append([]byte(nil), '+')
	builder = append(builder, s...)
	builder = append(builder, crlf...)
	return builder
}

func integrationClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("INTEGRATION") != "" {
		t.Skip()
	}
	c, err := New(context.Background(), ":6379")
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
	tests := []struct {
		name      string
		response  []byte
		want      string
		wantExist bool
		wantErr   error
	}{
		{
			"Basic",
			asBulkString("bar"),
			"bar",
			true,
			nil,
		},
		{
			"Error messages are converted to errors",
			asSimpleErrorString("ERR wrong number of arguments for 'get' command"),
			"",
			false,
			errors.New("ERR wrong number of arguments for 'get' command"),
		},
		{
			"Bulk Strings containing CRLF are read in full",
			asBulkString("bar\nbaz"),
			"bar\nbaz",
			true,
			nil,
		},
		{
			"Unset keys return notExist",
			nullString,
			"",
			false,
			nil,
		},
		{
			"Empty values do exist",
			asBulkString(""),
			"",
			true,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ts.data <- tt.response
			got, gotExist, err := c.Get(context.Background(), "Foo")

			if (err != nil) != (tt.wantErr != nil) {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil && tt.wantErr.Error() != err.Error() {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Get() got = %v, want %v", got, tt.want)
			}
			if gotExist != tt.wantExist {
				t.Errorf("Get() gotExist = %v, want %v", gotExist, tt.wantExist)
			}
		})
	}
}

func TestClient_Set(t *testing.T) {
	ts, c := serverClientPair(t)
	tests := []struct {
		name     string
		response []byte
		wantErr  error
	}{
		{
			"OK",
			okString,
			nil,
		},
		{
			"Bulk response is not an error",
			asBulkString("A long string"),
			nil,
		},
		{
			"Error messages are converted to errors",
			asSimpleErrorString("WRONGTYPE Operation against a key holding the wrong kind of value"),
			errors.New("WRONGTYPE Operation against a key holding the wrong kind of value"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ts.data <- tt.response
			err := c.Set(context.Background(), "Foo", "bar")

			if (err != nil) != (tt.wantErr != nil) {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr != nil && tt.wantErr.Error() != err.Error() {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConcurrency(t *testing.T) {
	t.Run("Should use two independent connections and put them back", func(t *testing.T) {
		client, err := New(context.Background(), "-1")
		if err != nil {
			t.Fatal(err)
		}
		conn1, serv1 := net.Pipe()
		conn2, serv2 := net.Pipe()
		// Add two pipes to the client's connection pool
		client.pool <- conn1
		client.pool <- conn2
		var wg sync.WaitGroup
		wg.Add(2)
		f := func() {
			defer wg.Done()
			_, _, err = client.Get(context.Background(), "Foo")
			if err != nil {
				t.Errorf("Got an error back from Get %v", err)
			}
		}
		// Launch two concurrent Gets
		go f()
		go f()

		// arbitrary size larger than all the messages
		buf := make([]byte, 1024)

		// Reads and writes are synchronous in a net.Pipe
		// Because we are reading each request, but not writing a response, this proves
		// there are two independent connections at play in the client
		_, err = serv1.Read(buf)
		if err != nil {
			t.Error(err)
		}
		_, err = serv2.Read(buf)
		if err != nil {
			t.Error(err)
		}
		_, err = serv1.Write(asBulkString("Bar"))
		if err != nil {
			t.Error(err)
		}
		_, err = serv2.Write(asBulkString("Baz"))
		if err != nil {
			t.Error(err)
		}

		// all Gets are done
		wg.Wait()
		if len(client.pool) != 2 {
			t.Errorf("Should have put both conns back, instead got %v", len(client.pool))
		}
	})
}

func Test_Integration(t *testing.T) {
	c := integrationClient(t)
	key := "X"
	want := "baz"

	err := c.Set(context.Background(), key, want)
	if err != nil {
		t.Errorf("Set() error = %v", err)
	}

	got, _, err := c.Get(context.Background(), key)
	if err != nil {
		t.Errorf("Get() error = %v, wantErr %v", err, nil)
		return
	}
	if got != want {
		t.Errorf("Get() got = %v, want %v", got, want)
	}
}
