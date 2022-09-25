package main

import (
	"fmt"
	"github.com/qtyty/delay-mq/v1/app/v1/queue"
	"time"
)

func main() {
	q := queue.NewQueue("test", func(payload string) {
		fmt.Println(payload)
	})

	for i := 0; i < 100; i++ {
		err := q.SendDelayJob("job1", "asoul"+string([]byte{byte(i)}), 10*time.Minute)
		if err != nil {
			fmt.Println(err)
		}
	}

	q.Start()
}
