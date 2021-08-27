package hermes

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

type IOTDeviceMeasureRequest struct{}
type IOTDeviceMeasureResponse struct {
	value float32
}

type IOTDevice struct{}

func (device *IOTDevice) receive(ctx *ActorContext, msg ActorMessage) {
	switch t := msg.Payload().(type) {
	case *IOTDeviceMeasureRequest:
		ctx.Reply(msg, &IOTDeviceMeasureResponse{value: rand.Float32()})

	default:
		fmt.Printf("unknown message of type: %s\n", t)
	}
}

type IOTDeviceGroupAddRequest struct {
	deviceID ActorID
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
	devices map[ActorID]bool
}

func newIOTDeviceGroup() *IOTDeviceGroup {
	return &IOTDeviceGroup{devices: make(map[ActorID]bool)}
}

func (grp *IOTDeviceGroup) receive(ctx *ActorContext, msg ActorMessage) {
	switch t := msg.Payload().(type) {
	case *IOTDeviceGroupAddRequest:
		grp.onAddDevice(ctx, msg)

	case *IOTDeviceGroupMeasureRequest:
		grp.onMeasure(ctx, msg)

	default:
		fmt.Printf("unknown message of type: %s\n", t)
	}
}

func (grp *IOTDeviceGroup) onAddDevice(ctx *ActorContext, msg ActorMessage) {
	id := msg.Payload().(*IOTDeviceGroupAddRequest).deviceID

	if id == "" {
		ctx.Reply(msg, &IOTDeviceGroupAddResponse{err: errors.New("invalid_device_id")})
		return
	}

	grp.devices[id] = true

	ctx.Reply(msg, &IOTDeviceGroupAddResponse{})
}

func (grp *IOTDeviceGroup) onMeasure(ctx *ActorContext, msg ActorMessage) {
	values := make([]float32, 0, len(grp.devices))
	reqChs := make([]chan ActorMessage, 0, len(grp.devices))

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

func TestRequestReply(t *testing.T) {
	sys, err := NewActorSystem()
	if err != nil {
		t.Fatalf("new actor system failed with error: %s\n", err.Error())
	}

	gid := ActorID("iot-dev-grp")
	grp := newIOTDeviceGroup()
	err = sys.Register(gid, grp.receive)
	if err != nil {
		t.Fatalf("failed to register device group: %s\n", err.Error())
	}

	deviceCount := 10

	err = addDevices(sys, gid, deviceCount)
	if err != nil {
		t.Fatalf("failed to add devices: %s\n", err.Error())
	}

	err = measure(sys, gid, deviceCount)
	if err != nil {
		t.Fatalf("failed to measure devices: %s\n", err.Error())
	}
}

func addDevices(sys *ActorSystem, grp ActorID, deviceCount int) error {
	for di := 0; di < deviceCount; di++ {
		id := ActorID(fmt.Sprintf("d%d", di))
		d := &IOTDevice{}

		err := sys.Register(id, d.receive)
		if err != nil {
			return err
		}

		m, err := sys.RequestWithTimeout(grp, &IOTDeviceGroupAddRequest{deviceID: id}, 100*time.Millisecond)
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

func measure(sys *ActorSystem, grp ActorID, deviceCount int) error {
	res, err := sys.RequestWithTimeout(grp, &IOTDeviceGroupMeasureRequest{}, 1000*time.Millisecond)
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
