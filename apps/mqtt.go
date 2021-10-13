package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/dartali/hermes"
	"github.com/dartali/hermes/apps/mqtt"
)

func main() {
	net, err := hermes.New(factory)
	if err != nil {
		fmt.Printf("[FATAL] unable to create hermes: %s\n", err.Error())
		return
	}

	srv, err := mqtt.NewServer(net)
	if err != nil {
		fmt.Printf("[FATAL] unable to create hermes: %s\n", err.Error())
		return
	}

	srv.ListenAndServe()
}

var inMemStore = mqtt.NewInMemSessionStore()

func factory(id hermes.ReceiverID) (hermes.Receiver, error) {
	if mqtt.IsClientID(id) {
		return mqtt.NewClientRecv(), nil
	} else if mqtt.IsSessionID(id) {
		r, err := mqtt.NewSessionRecv(inMemStore, 1*time.Second)
		if err != nil {
			return nil, err
		}

		return r, nil
	} else if mqtt.IsPubSubID(id) {
		r, err := mqtt.NewPubSubRecv(mqtt.NewInMemMsgStore())
		if err != nil {
			return nil, err
		}

		return r, nil
	}

	return nil, errors.New("unknown_receiver")
}
