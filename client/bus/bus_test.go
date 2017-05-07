package bus

import (
	"testing"
	"time"

	"github.com/CyCoreSystems/ari"
	log15 "gopkg.in/inconshreveable/log15.v2"
)

func TestMatchEvent(t *testing.T) {
	key := &ari.Key{
		Kind: ari.ChannelKey,
		ID:   "testA",
		Node: "0test0",
		App:  "testApp",
	}

	e := &ari.StasisEnd{
		EventData: ari.EventData{
			Type:        "StasisEnd",
			Application: "testApp",
			Node:        "0test0",
			Timestamp:   ari.DateTime(time.Now()),
		},
		Header: make(ari.Header),
		Channel: ari.ChannelData{
			Key:          nil,
			ID:           "testB",
			Name:         "Local/bozo",
			State:        "up",
			Accountcode:  "49er",
			Caller:       ari.CallerID{},
			Connected:    ari.CallerID{},
			Creationtime: ari.DateTime(time.Now()),
			Dialplan: ari.DialplanCEP{
				Context:  "default",
				Exten:    "s",
				Priority: 1,
			},
		},
	}

	s := &Subscription{
		key:       key,
		log:       log15.New(),
		eventChan: make(chan ari.Event, EventChanBufferLength),
		events:    []string{"StasisEnd"},
	}

	if s.matchEvent(e) {
		t.Error("matched incorrect event")
	}
}