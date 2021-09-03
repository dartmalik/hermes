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
type IOTDeviceMeasureResponse struct {
	value float32
}

type IOTDevice struct{}

func (device *IOTDevice) receive(ctx *Context, msg Message) {
	switch msg.Payload().(type) {
	case *IOTDeviceMeasureRequest:
		ctx.Reply(msg, &IOTDeviceMeasureResponse{value: rand.Float32()})
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
}
type IOTDeviceGroupAddResponse struct {
	err error
}

type IOTDeviceGroupMeasureRequest struct{}
type IOTDeviceGroupMeasureResponse struct {
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

func (grp *IOTDeviceGroup) receive(ctx *Context, msg Message) {
	switch msg.Payload().(type) {
	case *IOTDeviceGroupAddRequest:
		grp.onAddDevice(ctx, msg)

	case *IOTDeviceGroupMeasureRequest:
		grp.onMeasure(ctx, msg)
	}
}

func (grp *IOTDeviceGroup) onAddDevice(ctx *Context, msg Message) {
	id := msg.Payload().(*IOTDeviceGroupAddRequest).deviceID

	if id == "" {
		ctx.Reply(msg, &IOTDeviceGroupAddResponse{err: errors.New("invalid_device_id")})
		return
	}

	grp.devices[id] = true

	ctx.Reply(msg, &IOTDeviceGroupAddResponse{})
}

func (grp *IOTDeviceGroup) onMeasure(ctx *Context, msg Message) {
	values := make([]float32, 0, len(grp.devices))
	reqChs := make([]chan Message, 0, len(grp.devices))

	for id := range grp.devices {
		reqCh := ctx.Request(id, &IOTDeviceMeasureRequest{})
		reqChs = append(reqChs, reqCh)
	}

	for _, reqCh := range reqChs {
		select {
		case reply := <-reqCh:
			dmr, ok := reply.Payload().(*IOTDeviceMeasureResponse)
			if !ok {
				ctx.Reply(msg, &IOTDeviceGroupMeasureResponse{err: errors.New("invalid_device_reply")})
				return
			}

			values = append(values, dmr.value)

		case <-time.After(100 * time.Millisecond):
			ctx.Reply(msg, &IOTDeviceGroupMeasureResponse{err: errors.New("request_timeout")})
			return
		}
	}

	ctx.Reply(msg, &IOTDeviceGroupMeasureResponse{values: values, err: nil})
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
	net, err := New(IOTDeviceFactory)
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

		m, err := net.RequestWithTimeout(grp, &IOTDeviceGroupAddRequest{deviceID: id}, 100*time.Millisecond)
		if err != nil {
			return err
		}

		res := m.Payload().(*IOTDeviceGroupAddResponse)
		if res.err != nil {
			return res.err
		}
	}

	return nil
}

func measure(net *Hermes, grp ReceiverID, deviceCount int) error {
	res, err := net.RequestWithTimeout(grp, &IOTDeviceGroupMeasureRequest{}, 1000*time.Millisecond)
	if err != nil {
		return err
	}

	gmr, ok := res.Payload().(*IOTDeviceGroupMeasureResponse)
	if !ok {
		return errors.New("received invalid response from device group")
	}

	if len(gmr.values) != deviceCount {
		return fmt.Errorf("expected %d values but got %d\n", deviceCount, len(gmr.values))
	}

	return nil
}
