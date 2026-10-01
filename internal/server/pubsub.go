package server

import "sync"

// subscriberQueue is how many messages can wait for a slow subscriber
// before it is disconnected.
const subscriberQueue = 1024

type message struct {
	channel string
	payload []byte
}

// broker maps channel names to the clients subscribed to them.
type broker struct {
	mu   sync.RWMutex
	subs map[string]map[*client]struct{}
}

func newBroker() *broker {
	return &broker{subs: make(map[string]map[*client]struct{})}
}

func (b *broker) subscribe(c *client, channel string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subs[channel] == nil {
		b.subs[channel] = make(map[*client]struct{})
	}
	b.subs[channel][c] = struct{}{}
}

func (b *broker) unsubscribe(c *client, channel string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.subs[channel], c)
	if len(b.subs[channel]) == 0 {
		delete(b.subs, channel)
	}
}

// publish queues payload for every subscriber of channel and returns how
// many received it.
//
// The publisher never waits for a subscriber. If one isn't reading and its
// queue is full, it is disconnected instead. Redis does the same with its
// client-output-buffer-limit. The alternative, blocking, would let one stalled
// subscriber freeze every client that publishes to its channel.
func (b *broker) publish(channel string, payload []byte) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	n := 0
	for c := range b.subs[channel] {
		select {
		case c.msgs <- message{channel, payload}:
			n++
		default:
			// Closing the socket makes the subscriber's own goroutine exit and
			// unsubscribe. Unsubscribing here would deadlock on b.mu.
			c.conn.Close()
		}
	}
	return n
}
