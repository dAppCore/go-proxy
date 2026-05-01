package proxy

import "sync"

// EventBus dispatches proxy lifecycle events to synchronous listeners.
//
//	bus := proxy.NewEventBus()
//	bus.Subscribe(proxy.EventLogin, func(e proxy.Event) {
//	    _ = e.Miner.User()
//	})
//	bus.Subscribe(proxy.EventAccept, stats.OnAccept)
type EventBus struct {
	listeners map[EventType][]EventHandler
	mu        sync.RWMutex
}

// EventType identifies one proxy lifecycle event.
//
// proxy.EventLogin
type EventType int

const (
	EventLogin  EventType = iota // miner completed login
	EventAccept                  // pool accepted a submitted share
	EventReject                  // pool rejected a share (or share expired)
	EventClose                   // miner TCP connection closed
)

// EventHandler is the callback signature for all event types.
//
// handler := func(e proxy.Event) { _ = e.Miner }
type EventHandler func(Event)

// Event carries the data for any proxy lifecycle event.
//
// bus.Dispatch(proxy.Event{Type: proxy.EventLogin, Miner: m})
type Event struct {
	Type    EventType
	Miner   *Miner // always set
	Job     *Job   // set for Accept and Reject events
	Diff    uint64 // effective difficulty of the share (Accept and Reject)
	Error   string // rejection reason (Reject only)
	Latency uint16 // pool response time in ms (Accept and Reject)
	Expired bool   // true if the share was accepted but against the previous job
}
