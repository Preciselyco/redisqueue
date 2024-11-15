package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Preciselyco/gopkg/logger"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

func msToTime(ms string) (time.Time, error) {
	msInt, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(0, msInt*int64(time.Millisecond)), nil
}

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

	if len(os.Args) > 1 {
		groupName := os.Args[1]
		ctx := context.Background()
		// find first 1000 event streams in redis
		l := logger.FromContext(ctx)
		sCmd := redisClient.ScanType(ctx, 0, "*.events.*", 1000, "STREAM")
		streams, _, err := sCmd.Result()
		if err != nil {
			l.Fatal("failed to list streams from redis")
		}

		// prune old messages
		for _, stream := range streams {
			ls := l.WithField("stream", stream)
			cg := redisClient.XInfoGroups(ctx, stream)
			cs, err := cg.Result()
			if err != nil {
				ls.Fatalf("failed to list groups for stream: %q", stream)
			}
			for _, group := range cs {
				if group.Name != groupName {
					continue // wrong group
				}
				if group.Consumers == 0 && group.Pending == 0 {
					// remove empty groups
					redisClient.XGroupDestroy(ctx, stream, group.Name)
					continue
				}
				lg := ls.WithFields(logrus.Fields{
					"consumers":         group.Consumers,
					"lag":               group.Lag,
					"pending":           group.Pending,
					"last-delivered-id": group.LastDeliveredID,
					"group":             group.Name})

				lg.Infof("checking group: %q for stream: %q for messages to prune", group.Name, stream)
				parts := strings.Split(group.LastDeliveredID, "-")
				if parts[0] == "0" {
					continue
				}
				ts, err := msToTime(parts[0])
				if err != nil {
					lg.WithError(err).Fatalf("failed to parse timestamp from last delivered id %q", group.LastDeliveredID)
				}

				if ts.Before(time.Now().Add(-time.Hour * 24)) {
					rCmd := redisClient.XTrimMinID(ctx, stream, group.LastDeliveredID)
					trimmedMsgCount, err := rCmd.Result()
					if err != nil {
						lg.WithError(err).Fatalf("failed to trim messages for stream: %q group: %q", stream, group.Name)
					}
					lg.WithField("message-count", trimmedMsgCount).Infof("trimmed messages from stream: %q", stream)
					// prevent ddos on redis
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	fmt.Println("Bye!")
}
