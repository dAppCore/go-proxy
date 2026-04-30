package proxy

import "testing"

func TestCoreImpl_EventBus_Subscribe_Good(t *testing.T) {
	bus := NewEventBus()
	order := make([]int, 0, 2)

	bus.Subscribe(EventLogin, func(Event) {
		order = append(order, 1)
	})
	bus.Subscribe(EventLogin, func(Event) {
		order = append(order, 2)
	})

	bus.Dispatch(Event{Type: EventLogin})

	if len(order) != 2 {
		t.Fatalf("expected two callbacks, got %d", len(order))
	}
	if order[0] != 1 || order[1] != 2 {
		t.Fatalf("expected callbacks to run in subscription order, got %v", order)
	}
}

func TestCoreImpl_EventBus_Subscribe_Bad(t *testing.T) {
	var bus *EventBus
	bus.Subscribe(EventLogin, nil)

	bus = NewEventBus()
	bus.Subscribe(EventLogin, nil)
	bus.Dispatch(Event{Type: EventLogin})

	if got := len(bus.listeners[EventLogin]); got != 0 {
		t.Fatalf("expected nil handler to be ignored, got %d listeners", got)
	}
}

func TestCoreImpl_EventBus_Subscribe_Ugly(t *testing.T) {
	var bus EventBus
	called := 0

	bus.Subscribe(EventAccept, func(Event) {
		called++
	})
	bus.Dispatch(Event{Type: EventAccept})

	if called != 1 {
		t.Fatalf("expected zero-value bus to initialize its listener map, got %d calls", called)
	}
	if bus.listeners == nil {
		t.Fatalf("expected zero-value bus to allocate listener storage")
	}
}

func TestCoreImpl_EventBus_Dispatch_Good(t *testing.T) {
	bus := NewEventBus()
	got := make([]string, 0, 1)

	bus.Subscribe(EventReject, func(e Event) {
		if e.Error != "invalid" {
			t.Fatalf("expected event payload to be forwarded, got %+v", e)
		}
		got = append(got, "first")
	})
	bus.Subscribe(EventReject, func(Event) {
		got = append(got, "second")
	})

	bus.Dispatch(Event{Type: EventReject, Error: "invalid"})

	if len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("expected dispatch order to be preserved, got %v", got)
	}
}

func TestCoreImpl_EventBus_Dispatch_Bad(t *testing.T) {
	var bus *EventBus
	bus.Dispatch(Event{Type: EventClose})

	bus = NewEventBus()
	bus.Dispatch(Event{Type: EventClose})
}

func TestCoreImpl_EventBus_Dispatch_Ugly(t *testing.T) {
	bus := NewEventBus()
	called := 0

	bus.Subscribe(EventClose, func(Event) {
		panic("boom")
	})
	bus.Subscribe(EventClose, func(Event) {
		called++
	})

	bus.Dispatch(Event{Type: EventClose})

	if called != 1 {
		t.Fatalf("expected panic in one handler not to stop later handlers, got %d calls", called)
	}
}
