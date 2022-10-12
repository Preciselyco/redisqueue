package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

func main() {
	rp, ok := os.LookupEnv("REDIS_PROVIDER")
	if !ok {
		logrus.Fatal("missing connection string to redis")
		return
	}
	opt, err := redis.ParseURL(rp)
	if err != nil {
		logrus.WithError(err).Fatal("Could not connect to Redis, exists")
		return
	}
	redisClient := redis.NewClient(opt)

	_, err = redisClient.Ping(context.Background()).Result()
	if err != nil {
		logrus.WithError(err).Fatal("Could talk to Redis, exists")
		return
	}

	if len(os.Args) > 2 {
		group := os.Args[1]
		stream := os.Args[2]
		// prune old inactive consumers
		cu := redisClient.XInfoConsumers(context.Background(), stream, group)
		cs, err := cu.Result()
		if err != nil {
			logrus.WithError(err).Fatalf("failed to list consumers for stream: %q group: %q", stream, group)
		}
		csCount := len(cs)
		logrus.Infof("Found %d consumers \n", csCount)
		for x, consumer := range cs {
			var action string
			if consumer.Pending == 0 && // don't remove with pending msg
				consumer.Idle > time.Hour.Milliseconds()*24 { // respect consumers having been idle for more than 24 h (PruneTimeout) before trying to delete
				action = "removing"
				rCmd := redisClient.XGroupDelConsumer(context.Background(), stream, group, consumer.Name)
				pendingMsgCount, err := rCmd.Result()
				if err != nil {
					logrus.WithError(err).Fatalf("failed to remove consumer: %q for stream: %q group: %q", consumer.Name, stream, group)
				}
				if pendingMsgCount > 0 {
					logrus.WithError(err).Fatalf("remove consumer: %q with pending messages: %d for stream: %q group: %q", consumer.Name, pendingMsgCount, stream, group)
				}

				// prevent ddos on redis
				time.Sleep(10 * time.Millisecond)
			} else {
				action = "skipping"
			}
			logrus.WithFields(logrus.Fields{"action": action, "name": consumer.Name, "idle": consumer.Idle, "pending": consumer.Pending}).Infof("%d / %d: Checking consumer", x+1, csCount)
		}
	}

	fmt.Println("Bye!")
}
