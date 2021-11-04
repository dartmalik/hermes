package hermes

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

type IOTDeviceMeasureRequest struct{}
type IOTDeviceMeasureReply struct {
	value float32
}

type IOTDevice struct{}

func (device *IOTDevice) receive(ctx Context, msg Message) {
	switch msg.Payload().(type) {
	case *IOTDeviceMeasureRequest:
		ctx.Reply(msg, &IOTDeviceMeasureReply{value: rand.Float32()})
	}
}

func newIOTDevice() *IOTDevice {
	return &IOTDevice{}
}

func IOTDeviceID(id string) ReceiverID {
	return ReceiverID(EntityTypeIOTDevice + "/" + id)
}

type IOTDeviceGroupAddRequest struct {
	deviceID ReceiverID
	replyCh  chan *IOTDeviceGroupAddReply
}
type IOTDeviceGroupAddReply struct {
	err error
}

type IOTDeviceGroupMeasureRequest struct {
	replyCh chan *IOTDeviceGroupMeasureReply
}
type IOTDeviceGroupMeasureReply struct {
	values []float32
	err    error
}

type IOTDeviceGroup struct {
	devices map[ReceiverID]bool
}

func IOTDeviceGroupID(id string) ReceiverID {
	return ReceiverID(EntityTypeIOTDeviceGroup + "/" + id)
}

func newIOTDeviceGroup() *IOTDeviceGroup {
	return &IOTDeviceGroup{devices: make(map[ReceiverID]bool)}
}

func (grp *IOTDeviceGroup) receive(ctx Context, mi Message) {
	switch msg := mi.Payload().(type) {
	case *IOTDeviceGroupAddRequest:
		grp.onAddDevice(ctx, msg)

	case *IOTDeviceGroupMeasureRequest:
		grp.onMeasure(ctx, msg)
	}
}

func (grp *IOTDeviceGroup) onAddDevice(ctx Context, req *IOTDeviceGroupAddRequest) {
	id := req.deviceID

	if id == "" {
		req.replyCh <- &IOTDeviceGroupAddReply{err: errors.New("invalid_device_id")}
		return
	}

	grp.devices[id] = true

	req.replyCh <- &IOTDeviceGroupAddReply{}
}

func (grp *IOTDeviceGroup) onMeasure(ctx Context, req *IOTDeviceGroupMeasureRequest) {
	values := make([]float32, 0, len(grp.devices))
	reqChs := make([]<-chan Message, 0, len(grp.devices))

	for id := range grp.devices {
		reqCh, err := ctx.Request(id, &IOTDeviceMeasureRequest{})
		if err != nil {
			req.replyCh <- &IOTDeviceGroupMeasureReply{err: err}
			return
		}

		reqChs = append(reqChs, reqCh)
	}

	for _, reqCh := range reqChs {
		select {
		case rep := <-reqCh:
			dmr, ok := rep.Payload().(*IOTDeviceMeasureReply)
			if !ok {
				req.replyCh <- &IOTDeviceGroupMeasureReply{err: errors.New("invalid_device_reply")}
				return
			}

			values = append(values, dmr.value)

		case <-time.After(100 * time.Millisecond):
			req.replyCh <- &IOTDeviceGroupMeasureReply{err: errors.New("request_timeout")}
			return
		}
	}

	req.replyCh <- &IOTDeviceGroupMeasureReply{values: values, err: nil}
}

const (
	EntityTypeIOTDevice      = "devices"
	EntityTypeIOTDeviceGroup = "deviceGroups"
)

func IOTDeviceFactory(id ReceiverID) (Receiver, error) {
	if strings.HasPrefix(string(id), EntityTypeIOTDevice) {
		return newIOTDevice().receive, nil
	} else if strings.HasPrefix(string(id), EntityTypeIOTDeviceGroup) {
		return newIOTDeviceGroup().receive, nil
	}

	return nil, errors.New("unkown_entity_type")
}

func TestRequestReply(t *testing.T) {
	opts := NewOpts()
	opts.RF = IOTDeviceFactory
	net, err := New(opts)
	if err != nil {
		t.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	gid := IOTDeviceGroupID("iot-dev-grp")

	deviceCount := 10

	err = addDevices(net, gid, deviceCount)
	if err != nil {
		t.Fatalf("failed to add devices: %s\n", err.Error())
	}

	err = measure(net, gid, deviceCount)
	if err != nil {
		t.Fatalf("failed to measure devices: %s\n", err.Error())
	}
}

func addDevices(net *Hermes, grp ReceiverID, deviceCount int) error {
	for di := 0; di < deviceCount; di++ {
		id := IOTDeviceID(fmt.Sprintf("d%d", di))

		replyCh := make(chan *IOTDeviceGroupAddReply, 1)
		err := net.Send("", grp, &IOTDeviceGroupAddRequest{deviceID: id, replyCh: replyCh})
		if err != nil {
			return err
		}

		rep := <-replyCh
		if rep.err != nil {
			return rep.err
		}
	}

	return nil
}

func measure(net *Hermes, grp ReceiverID, deviceCount int) error {
	replyCh := make(chan *IOTDeviceGroupMeasureReply)
	err := net.Send("", grp, &IOTDeviceGroupMeasureRequest{replyCh: replyCh})
	if err != nil {
		return err
	}

	rep := <-replyCh
	if len(rep.values) != deviceCount {
		return fmt.Errorf("expected %d values but got %d\n", deviceCount, len(rep.values))
	}

	return nil
}
