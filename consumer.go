package redisqueue

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Preciselyco/gopkg/logger"
	"github.com/Preciselyco/gopkg/stackerr"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// ConsumerFunc is a type alias for the functions that will be used to handle
// and process Messages.
type ConsumerFunc func(*Message) error

type registeredConsumer struct {
	fn ConsumerFunc
	id string
}

// ConsumerOptions provide options to configure the Consumer.
type ConsumerOptions struct {
	// Name sets the name of this consumer. This will be used when fetching from
	// Redis. If empty, the hostname will be used.
	Name string
	// GroupName sets the name of the consumer group. This will be used when
	// coordinating in Redis. If empty, the hostname will be used.
	GroupName string
	// VisibilityTimeout dictates the maximum amount of time a message should
	// stay in pending. If there is a message that has been idle for more than
	// this duration, the consumer will attempt to claim it.
	VisibilityTimeout time.Duration
	// BlockingTimeout designates how long the XREADGROUP call blocks for. If
	// this is 0, it will block indefinitely. While this is the most efficient
	// from a polling perspective, if this call never times out, there is no
	// opportunity to yield back to Go at a regular interval. This means it's
	// possible that if no messages are coming in, the consumer cannot
	// gracefully shutdown. Instead, it's recommended to set this to 1-5
	// seconds, or even longer, depending on how long your application can wait
	// to shutdown.
	BlockingTimeout time.Duration
	// ReclaimInterval is the amount of time in between calls to XPENDING to
	// attempt to reclaim jobs that have been idle for more than the visibility
	// timeout. A smaller duration will result in more frequent checks. This
	// will allow messages to be reaped faster, but it will put more load on
	// Redis.
	ReclaimInterval time.Duration
	// BufferSize determines the size of the channel uses to coordinate the
	// processing of the messages. This determines the maximum number of
	// in-flight messages.
	BufferSize int
	// Concurrency dictates how many goroutines to spawn to handle the messages.
	Concurrency int
	// RedisClient supersedes the RedisOptions field, and allows you to inject
	// an already-made Redis Client for use in the consumer. This may be either
	// the standard client or a cluster client.
	RedisClient redis.UniversalClient
	// RedisOptions allows you to configure the underlying Redis connection.
	// More info here:
	// https://pkg.go.dev/github.com/go-redis/redis/v7?tab=doc#Options.
	//
	// This field is used if RedisClient field is nil.
	RedisOptions *RedisOptions
	//
	// PruneTimeout dictates how long a consumer needs to be idle before we try to remove it
	PruneTimeout time.Duration
	//
	// UsePreflightCheck decides if the check for version 5 of the server should be run.
	// Default true
	UsePreflightCheck bool
	//
	// Automatically try to unregister consumer from streams when shutting down
	// Default false
	UnregisterOnShutdown bool
}

// Consumer adds a convenient wrapper around dequeuing and managing concurrency.
type Consumer struct {
	// Errors is a channel that you can receive from to centrally handle any
	// errors that may occur either by your ConsumerFuncs or by internal
	// processing functions. Because this is an unbuffered channel, you must
	// have a listener on it. If you don't parts of the consumer could stop
	// functioning when errors occur due to the blocking nature of unbuffered
	// channels.
	Errors chan error

	options   *ConsumerOptions
	redis     redis.UniversalClient
	consumers map[string]registeredConsumer
	streams   []string
	queue     chan *Message
	wg        *sync.WaitGroup

	stopReclaim chan struct{}
	stopPoll    chan struct{}
	stopWorkers chan struct{}
}

var defaultConsumerOptions = &ConsumerOptions{
	VisibilityTimeout: 60 * time.Second,
	BlockingTimeout:   5 * time.Second,
	ReclaimInterval:   1 * time.Second,
	BufferSize:        100,
	Concurrency:       10,
	PruneTimeout:      24 * time.Hour,
	UsePreflightCheck: true,
}

// NewConsumer uses a default set of options to create a Consumer. It sets Name
// to the hostname, GroupName to "redisqueue", VisibilityTimeout to 60 seconds,
// BufferSize to 100, and Concurrency to 10. In most production environments,
// you'll want to use NewConsumerWithOptions.
func NewConsumer(ctx context.Context) (*Consumer, error) {
	return NewConsumerWithOptions(ctx, defaultConsumerOptions)
}

// NewConsumerWithOptions creates a Consumer with custom ConsumerOptions. If
// Name is left empty, it defaults to the hostname; if GroupName is left empty,
// it defaults to "redisqueue"; if BlockingTimeout is 0, it defaults to 5
// seconds; if ReclaimInterval is 0, it defaults to 1 second.
func NewConsumerWithOptions(ctx context.Context, options *ConsumerOptions) (*Consumer, error) {
	hostname, _ := os.Hostname()

	if options.Name == "" {
		options.Name = hostname
	}
	if options.GroupName == "" {
		options.GroupName = "redisqueue"
	}
	if options.BlockingTimeout == 0 {
		options.BlockingTimeout = 5 * time.Second
	}
	if options.ReclaimInterval == 0 {
		options.ReclaimInterval = 1 * time.Second
	}

	var r redis.UniversalClient

	if options.RedisClient != nil {
		r = options.RedisClient
	} else {
		r = newRedisClient(options.RedisOptions)
	}

	if options.UsePreflightCheck {
		if err := redisPreflightChecks(ctx, r); err != nil {
			return nil, err
		}
	}

	return &Consumer{
		Errors: make(chan error),

		options:   options,
		redis:     r,
		consumers: make(map[string]registeredConsumer),
		streams:   make([]string, 0),
		queue:     make(chan *Message, options.BufferSize),
		wg:        &sync.WaitGroup{},

		stopReclaim: make(chan struct{}, 1),
		stopPoll:    make(chan struct{}, 1),
		stopWorkers: make(chan struct{}, options.Concurrency),
	}, nil
}

// RegisterWithLastID is the same as Register, except that it also lets you
// specify the oldest message to receive when first creating the consumer group.
// This can be any valid message ID, "0" for all messages in the stream, or "$"
// for only new messages.
//
// If the consumer group already exists the id field is ignored, meaning you'll
// receive unprocessed messages.
func (c *Consumer) RegisterWithLastID(stream string, id string, fn ConsumerFunc) {
	if len(id) == 0 {
		id = "0"
	}

	c.consumers[stream] = registeredConsumer{
		fn: fn,
		id: id,
	}
}

// Register takes in a stream name and a ConsumerFunc that will be called when a
// message comes in from that stream. Register must be called at least once
// before Run is called. If the same stream name is passed in twice, the first
// ConsumerFunc is overwritten by the second.
func (c *Consumer) Register(stream string, fn ConsumerFunc) {
	c.RegisterWithLastID(stream, "0", fn)
}

func (c *Consumer) createGroupsAndStreams(ctx context.Context) error {
	for stream, consumer := range c.consumers {
		c.streams = append(c.streams, stream)
		err := c.redis.XGroupCreateMkStream(ctx, stream, c.options.GroupName, consumer.id).Err()
		// ignoring the BUSYGROUP error makes this a noop
		if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
			return errors.Wrap(err, "error creating consumer group")
		}
	}
	return nil
}

// Run starts all of the worker goroutines and starts processing from the
// streams that have been registered with Register. All errors will be sent to
// the Errors channel. If Register was never called, an error will be sent and
// Run will terminate early. The same will happen if an error occurs when
// creating the consumer group in Redis. Run will block until Shutdown is called
// and all of the in-flight messages have been processed.
func (c *Consumer) Run(ctx context.Context) {
	if len(c.consumers) == 0 {
		c.Errors <- errors.New("at least one consumer function needs to be registered")
		return
	}

	err := c.createGroupsAndStreams(ctx)
	if err != nil {
		c.Errors <- errors.Wrap(err, "error creating consumer group")
		return
	}

	for i := 0; i < len(c.consumers); i++ {
		c.streams = append(c.streams, ">")
	}

	go c.reclaim(ctx)
	go c.poll()

	stop := newSignalHandler()
	go func() {
		<-stop
		c.Shutdown()
	}()

	c.wg.Add(c.options.Concurrency)

	for i := 0; i < c.options.Concurrency; i++ {
		go c.work()
	}

	// blocking, waiting for shutdown
	c.wg.Wait()
}

func (c *Consumer) removeConsumerFromStream(stream string) {
	name := c.options.Name
	rCmd := c.redis.XGroupDelConsumer(context.Background(), stream, c.options.GroupName, c.options.Name)
	pendingMsgCount, err := rCmd.Result()
	if err != nil {
		c.Errors <- fmt.Errorf("failed to remove consumer: %q for stream: %q group: %q", name, stream, c.options.GroupName)
	}
	if pendingMsgCount > 0 {
		c.Errors <- fmt.Errorf("removed consumer: %q with pending messages: %d for stream: %q group: %q", name, pendingMsgCount, stream, c.options.GroupName)
	}
}

func (c *Consumer) PruneConsumers(ctx context.Context) error {
	// find first 1000 event streams in redis
	l := logger.FromContext(ctx)
	sCmd := c.redis.ScanType(ctx, 0, "*.events.*", 1000, "STREAM")
	streams, _, err := sCmd.Result()
	if err != nil {
		return errors.New("failed to list streams from redis")
	}

	// prune old inactive consumers
	for _, stream := range streams {
		ls := l.WithField("stream", stream)
		cu := c.redis.XInfoConsumers(ctx, stream, c.options.GroupName)
		cs, err := cu.Result()
		if err != nil {
			return fmt.Errorf("failed to list consumers for stream: %q group: %q", stream, c.options.GroupName)
		}
		for _, consumer := range cs {
			if consumer.Name != c.options.Name && // don't remove self
				consumer.Pending == 0 && // don't remove with pending msg
				consumer.Idle > c.options.PruneTimeout { // respect PruneTimeout before trying to delete
				ls.WithFields(logrus.Fields{
					"consumer": consumer.Name,
					"pending":  consumer.Pending,
					"idle":     consumer.Idle,
					"inactive": consumer.Inactive}).Infof("removing consumer from group %q", c.options.GroupName)
				rCmd := c.redis.XGroupDelConsumer(ctx, stream, c.options.GroupName, consumer.Name)
				pendingMsgCount, err := rCmd.Result()
				if err != nil {
					return fmt.Errorf("failed to remove consumer: %q for stream: %q group: %q", consumer.Name, stream, c.options.GroupName)
				}
				if pendingMsgCount > 0 {
					return fmt.Errorf("remove consumer: %q with pending messages: %d for stream: %q group: %q", consumer.Name, pendingMsgCount, stream, c.options.GroupName)
				}

				// prevent ddos on redis
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
	return nil
}

func msToTime(ms string) (time.Time, error) {
	msInt, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(0, msInt*int64(time.Millisecond)), nil
}

func (c *Consumer) PruneMessages(ctx context.Context) error {
	// find first 1000 event streams in redis
	l := logger.FromContext(ctx)
	sCmd := c.redis.ScanType(ctx, 0, "*.events.*", 1000, "STREAM")
	streams, _, err := sCmd.Result()
	if err != nil {
		return errors.New("failed to list streams from redis")
	}

	// prune old inactive consumers
	for _, stream := range streams {
		ls := l.WithField("stream", stream)
		cg := c.redis.XInfoGroups(ctx, stream)
		cs, err := cg.Result()
		if err != nil {
			return fmt.Errorf("failed to list groups for stream: %q", stream)
		}
		for _, group := range cs {
			if group.Name != c.options.GroupName {
				continue // wrong group
			}
			lg := ls.WithFields(logrus.Fields{
				"consumers":         group.Consumers,
				"lag":               group.Lag,
				"pending":           group.Pending,
				"last-delivered-id": group.LastDeliveredID,
				"group":             group.Name})

			lg.Infof("checking group: %q for stream: %q for messages to prune", group.Name, stream)
			parts := strings.Split(group.LastDeliveredID, "-")
			ts, err := msToTime(parts[0])
			if err != nil {
				return stackerr.Wrap(err, "failed to parse timestamp from last delivered id %q", group.LastDeliveredID)
			}

			if ts.Before(time.Now().Add(-c.options.PruneTimeout)) {
				rCmd := c.redis.XTrimMinID(ctx, stream, group.LastDeliveredID)
				trimmedMsgCount, err := rCmd.Result()
				if err != nil {
					return fmt.Errorf("failed to trim messages for stream: %q group: %q", stream, c.options.GroupName)
				}
				lg.WithField("message-count", trimmedMsgCount).Infof("trimmed messages from stream: %q", stream)
				// prevent ddos on redis
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

// Shutdown stops new messages from being processed and tells the workers to
// wait until all in-flight messages have been processed, and then they exit.
// The order that things stop is 1) the reclaim process (if it's running), 2)
// the polling process, and 3) the worker processes.
func (c *Consumer) Shutdown() {
	c.stopReclaim <- struct{}{}
	if c.options.VisibilityTimeout == 0 {
		c.stopPoll <- struct{}{}
	}

	if c.options.UnregisterOnShutdown {
		// remove current consumer from streams
		for _, stream := range c.streams {
			c.removeConsumerFromStream(stream)
		}
	}
}

// reclaim runs in a separate goroutine and checks the list of pending messages
// in every stream. For every message, if it's been idle for longer than the
// VisibilityTimeout, it will attempt to claim that message for this consumer.
// If VisibilityTimeout is 0, this function returns early and no messages are
// reclaimed. It checks the list of pending messages according to
// ReclaimInterval.
func (c *Consumer) reclaim(ctx context.Context) {
	l := logger.FromContext(ctx)

	if c.options.VisibilityTimeout == 0 {
		return
	}

	ticker := time.NewTicker(c.options.ReclaimInterval)

	for {
		select {
		case <-c.stopReclaim:
			// once the reclaim process has stopped, stop the polling process
			c.stopPoll <- struct{}{}
			return
		case <-ticker.C:
			for stream := range c.consumers {
				start := "-"
				end := "+"

				for {
					res, err := c.redis.XPendingExt(context.Background(), &redis.XPendingExtArgs{
						Stream: stream,
						Group:  c.options.GroupName,
						Start:  start,
						End:    end,
						Count:  int64(c.options.BufferSize - len(c.queue)),
					}).Result()
					if err != nil && err != redis.Nil {
						c.Errors <- errors.Wrap(err, "error listing pending messages")
						break
					}

					if len(res) == 0 {
						break
					}

					msgs := make([]string, 0)

					for _, r := range res {
						if r.Idle >= c.options.VisibilityTimeout {
							l.WithField("message-id", r.ID).
								WithField("retry-count", r.RetryCount).
								WithField("consumer", r.Consumer).Info("trying to reclaim message")

							claimres, err := c.redis.XClaim(context.Background(), &redis.XClaimArgs{
								Stream:   stream,
								Group:    c.options.GroupName,
								Consumer: c.options.Name,
								MinIdle:  c.options.VisibilityTimeout,
								Messages: []string{r.ID},
							}).Result()
							if err != nil && err != redis.Nil {
								c.Errors <- errors.Wrapf(err, "error claiming %d message(s)", len(msgs))
								break
							}
							// If the Redis nil error is returned, it means that
							// the message no longer exists in the stream.
							// However, it is still in a pending state. This
							// could happen if a message was claimed by a
							// consumer, that consumer died, and the message
							// gets deleted (either through a XDEL call or
							// through MAXLEN). Since the message no longer
							// exists, the only way we can get it out of the
							// pending state is to acknowledge it.
							if err == redis.Nil {
								err = c.redis.XAck(context.Background(), stream, c.options.GroupName, r.ID).Err()
								if err != nil {
									c.Errors <- errors.Wrapf(err, "error acknowledging after failed claim for %q stream and %q message", stream, r.ID)
									continue
								}
							}
							c.enqueue(stream, claimres)
							for _, cm := range claimres {
								l.WithField("message-id", cm.ID).
									WithField("retry-count", r.RetryCount).
									WithField("consumer", r.Consumer).Info("reclaiming message")
							}
						}
					}

					newID, err := incrementMessageID(res[len(res)-1].ID)
					if err != nil {
						c.Errors <- err
						break
					}

					start = newID
				}
			}
		}
	}
}

// poll constantly checks the streams using XREADGROUP to see if there are any
// messages for this consumer to process. It blocks for up to 5 seconds instead
// of blocking indefinitely so that it can periodically check to see if Shutdown
// was called.
func (c *Consumer) poll() {
	for {
		select {
		case <-c.stopPoll:
			// once the polling has stopped (i.e. there will be no more messages
			// put onto c.queue), stop all of the workers
			for i := 0; i < c.options.Concurrency; i++ {
				c.stopWorkers <- struct{}{}
			}
			return
		default:
			res, err := c.redis.XReadGroup(context.Background(), &redis.XReadGroupArgs{
				Group:    c.options.GroupName,
				Consumer: c.options.Name,
				Streams:  c.streams,
				Count:    int64(c.options.BufferSize - len(c.queue)),
				Block:    c.options.BlockingTimeout,
			}).Result()
			if err != nil {
				if err, ok := err.(net.Error); ok && err.Timeout() {
					continue
				}
				if err == redis.Nil {
					continue
				}
				c.Errors <- errors.Wrap(err, "error reading redis stream")
				continue
			}

			for _, r := range res {
				c.enqueue(r.Stream, r.Messages)
			}
		}
	}
}

// enqueue takes a slice of XMessages, creates corresponding Messages, and sends
// them on the centralized channel for worker goroutines to process.
func (c *Consumer) enqueue(stream string, msgs []redis.XMessage) {
	for _, m := range msgs {
		msg := &Message{
			ID:     m.ID,
			Stream: stream,
			Values: m.Values,
		}
		c.queue <- msg
	}
}

// work is called in a separate goroutine. The number of work goroutines is
// determined by Concurreny. Once it gets a message from the centralized
// channel, it calls the corrensponding ConsumerFunc depending on the stream it
// came from. If no error is returned from the ConsumerFunc, the message is
// acknowledged in Redis.
func (c *Consumer) work() {
	defer c.wg.Done()

	for {
		select {
		case msg := <-c.queue:
			err := c.process(msg)
			if err != nil {
				c.Errors <- errors.Wrapf(err, "error calling ConsumerFunc for %q stream and %q message", msg.Stream, msg.ID)
				continue
			}
			err = c.redis.XAck(context.Background(), msg.Stream, c.options.GroupName, msg.ID).Err()
			if err != nil {
				c.Errors <- errors.Wrapf(err, "error acknowledging after success for %q stream and %q message", msg.Stream, msg.ID)
				continue
			}
		case <-c.stopWorkers:
			return
		}
	}
}

func (c *Consumer) process(msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = errors.Wrap(e, "ConsumerFunc panic")
				return
			}
			err = errors.Errorf("ConsumerFunc panic: %v", r)
		}
	}()
	err = c.consumers[msg.Stream].fn(msg)
	return
}
