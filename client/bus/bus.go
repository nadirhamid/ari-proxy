package bus

import (
        "fmt"
        "sync"

        "github.com/CyCoreSystems/ari/v5"
        "github.com/inconshreveable/log15"

        "github.com/nats-io/nats.go"
)

// EventChanBufferLength is the number of unhandled events which can be queued
// to the event channel buffer before further events are lost.
var EventChanBufferLength = 10

// Bus provides an ari.Bus interface to NATS
type Bus struct {
        prefix string

        log log15.Logger

        nc *nats.EncodedConn

        subs []*Subscription

        rwMux sync.RWMutex

}

// New returns a new Bus
func New(prefix string, nc *nats.EncodedConn, log log15.Logger) *Bus {
        return &Bus{
                prefix: prefix,
                log:    log,
                nc:     nc,
        }
}

func (b *Bus) subjectFromKey(key *ari.Key) string {
        if key == nil {
                return fmt.Sprintf("%sevent.>", b.prefix)
        }

        if key.Dialog != "" {
                return fmt.Sprintf("%sdialogevent.%s", b.prefix, key.Dialog)
        }

        subj := fmt.Sprintf("%sevent.", b.prefix)
        if key.App == "" {
                return subj + ">"
        }
        subj += key.App + "."

        if key.Node == "" {
                return subj + ">"
        }
        return subj + key.Node
}

// Subscription represents an ari.Subscription over NATS
type Subscription struct {
        key *ari.Key

        log log15.Logger

        subscription *nats.Subscription

        eventChan chan ari.Event

        events []string

        closed bool

        mu sync.RWMutex
}

// add appends a new subscription to the bus
func (b *Bus) add(s *Subscription) {
        b.rwMux.Lock()
        b.subs = append(b.subs, s)
        b.rwMux.Unlock()
}

// remove deletes the given subscription from the bus
func (b *Bus) remove(s *Subscription) {
        b.rwMux.Lock()
        for i, si := range b.subs {
                if s == si {
                        // Subs are pointers, so we have to explicitly remove them
                        // to prevent memory leaks
                        b.subs[i] = b.subs[len(b.subs)-1] // replace the current with the end
                        b.subs[len(b.subs)-1] = nil       // remove the end
                        b.subs = b.subs[:len(b.subs)-1]   // lop off the end

                        break
                }
        }
        b.rwMux.Unlock()
}

// Close implements ari.Bus
func (b *Bus) Close() {
        // No-op
}

// Send implements ari.Bus
func (b *Bus) Send(e ari.Event) {
        // No-op
}

// Subscribe implements ari.Bus
func (b *Bus) Subscribe(key *ari.Key, n ...string) ari.Subscription {
        var err error

        s := &Subscription{
                key:       key,
                log:       b.log,
                eventChan: make(chan ari.Event, EventChanBufferLength),
                events:    n,
        }

        s.subscription, err = b.nc.Subscribe(b.subjectFromKey(key), func(m *nats.Msg) {
                s.receive(m)
        })
        if err != nil {
                b.log.Error("failed to subscribe to NATS", "error", err)
                return nil
        }
        b.add( s )
        return s
}

// Unsubcribe removes an active subscription from the bus
func (b *Bus) Unsubscribe(key *ari.Key, eTypes ...string) {
        for _, sub := range b.subs {
                if sub.key.ID == key.ID {
                        // check if events match
                        if sub == nil || sub.key == nil {
                                        continue
                        }
                        fullMatch := true
                        if len( eTypes ) != len( sub.events ) {
                                fullMatch = false
                                break
                        }
                        for _, event1 := range sub.events {
                                foundEvent := false
                                for _, event2 := range eTypes {
                                        if event1 == event2 {
                                                foundEvent = true
                                                break
                                        }
                                }
                                if !foundEvent {
                                        fullMatch = false
                                        break
                                }
                        }

                        if !fullMatch {
                                // could not find the subscription
                                return
                        }
                        b.remove(sub)
                }
        }
}



// Events returns the channel on which events from this subscription will be sent
func (s *Subscription) Events() <-chan ari.Event {
        return s.eventChan
}

// Cancel destroys the subscription
func (s *Subscription) Cancel() {
        if s == nil {
                return
        }

        if s.subscription != nil {
                err := s.subscription.Unsubscribe()
                if err != nil {
                        s.log.Error("failed unsubscribe from NATS", "error", err)
                }
        }

        s.mu.Lock()
        if !s.closed {
                s.closed = true
                close(s.eventChan)
        }
        s.mu.Unlock()
}

func (s *Subscription) receive(o *nats.Msg) {
        e, err := ari.DecodeEvent(o.Data)
        if err != nil {
                s.log.Error("failed to convert received message to ari.Event", "error", err)
                return
        }

        if s.matchEvent(e) {
                s.mu.RLock()
                if !s.closed {
                        s.eventChan <- e
                }
                s.mu.RUnlock()
        }
}

func (s *Subscription) matchEvent(o ari.Event) bool {
        // First, filter by type
        var match bool
        for _, kind := range s.events {
                if kind == o.GetType() || kind == ari.Events.All {
                        match = true
                        break
                }
        }
        if !match {
                return false
        }

        // If we don't have a resource ID, we match everything
        // Next, match the entity
        if s.key == nil || s.key.ID == "" {
                return true
        }

        for _, k := range o.Keys() {
                if s.key.Match(k) {
                        return true
                }
        }
        return false
}