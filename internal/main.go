package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/JeremyLoy/redis"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := redis.New(ctx, ":6379")
	if err != nil {
		log.Fatalln(err)
	}
	v, err := client.Get(ctx, "X")
	fmt.Println(v)
	fmt.Println(err)
}
