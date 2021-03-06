package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/dartali/hermes"
	"github.com/dartali/hermes/apps/mqtt"
)

func main() {
	opts := hermes.NewOpts()
	opts.RF = newRecvFactory()
	net, err := hermes.New(opts)
	if err != nil {
		fmt.Printf("[FATAL] unable to create hermes: %s\n", err.Error())
		return
	}

	lwtReplyCh := make(chan *mqtt.LWTJoinReply)
	err = net.Send("", mqtt.LWTID(), &mqtt.LWTJoinRequest{ReplyCh: lwtReplyCh})
	if err != nil {
		fmt.Printf("[FATAL] failed to register LWT module: %s\n", err.Error())
		return
	}

	rep := <-lwtReplyCh
	if rep.Err != nil {
		fmt.Printf("[FATAL] failed to register LWT module: %s\n", rep.Err.Error())
		return
	}

	srv, err := mqtt.NewServer(net)
	if err != nil {
		fmt.Printf("[FATAL] unable to create hermes: %s\n", err.Error())
		return
	}

	srv.ListenAndServe()
}

func newRecvFactory() hermes.ReceiverFactory {
	var inMemStore = mqtt.NewInMemSessionStore()

	return func(id hermes.ReceiverID) (hermes.Receiver, error) {
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
		} else if mqtt.IsEventBusID(id) {
			return mqtt.NewEventBusRecv(), nil
		} else if mqtt.IsLWTID(id) {
			return mqtt.NewLWTRecv(), nil
		}

		return nil, errors.New("unknown_receiver")
	}
}
