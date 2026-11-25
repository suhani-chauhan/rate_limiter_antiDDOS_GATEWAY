package main

import (
	"context"
	"fmt"
	"time"

	ep "github.com/traffic-bigdata/eventproducer"
)

func main() {
	p, err := ep.New(ep.Config{Brokers: []string{"localhost:29092"}})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 20; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i%5)
		p.Emit(ep.NewEvent(ip, "GET", "/home", 200, ep.DecisionAllowed, "demo-agent", 3*time.Millisecond))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		fmt.Println("close error:", err)
	}
	fmt.Printf("stats: %+v\n", p.Stats())
}