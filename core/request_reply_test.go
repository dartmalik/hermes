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
	values := make([]float32, len(grp.devices))

	for id := range grp.devices {
		reply, err := ctx.RequestWithTimeout(id, &IOTDeviceMeasureRequest{}, 100*time.Millisecond)
		if err != nil {
			ctx.Reply(msg, &IOTDeviceGroupMeasureResponse{err: err})
			return
		}

		dmr, ok := reply.Payload().(*IOTDeviceMeasureResponse)
		if !ok {
			ctx.Reply(msg, &IOTDeviceGroupMeasureResponse{err: errors.New("invalid_device_reply")})
			return
		}

		values = append(values, dmr.value)
	}

	ctx.Reply(msg, &IOTDeviceGroupMeasureResponse{values: values, err: nil})
}

type IOTTesterStart struct{}

type IOTTester struct {
	t           *testing.T
	grp         ActorID
	deviceCount int
}

func (test *IOTTester) receive(ctx *ActorContext, msg ActorMessage) {
	switch ty := msg.Payload().(type) {
	case *IOTTesterStart:
		test.addDevices(ctx)

		test.measure(ctx)

	default:
		test.t.Fatalf("unknown message type: %s\n", ty)
	}
}

func (test *IOTTester) addDevices(ctx *ActorContext) {
	for di := 0; di < test.deviceCount; di++ {
		id := ActorID(fmt.Sprintf("d%d", di))
		d := &IOTDevice{}

		err := ctx.Register(id, d.receive)
		if err != nil {
			test.t.Fatalf("failed to register device with error: %s\n", err.Error())
		}

		m, err := ctx.RequestWithTimeout(test.grp, &IOTDeviceGroupAddRequest{deviceID: id}, 5000*time.Millisecond)
		if err != nil {
			test.t.Fatalf("failed to add device to group: %s\n", err.Error())
		}

		res := m.Payload().(*IOTDeviceGroupAddResponse)
		if res.err != nil {
			test.t.Fatalf("failed to add device to group: %s\n", res.err.Error())
		}
	}
}

func (test *IOTTester) measure(ctx *ActorContext) {
	res, err := ctx.RequestWithTimeout(test.grp, &IOTDeviceGroupMeasureRequest{}, 1000*time.Millisecond)
	if err != nil {
		test.t.Fatalf(err.Error())
	}

	gmr, ok := res.Payload().(*IOTDeviceGroupMeasureResponse)
	if !ok {
		test.t.Fatalf("received invalid response from device group")
	}

	if len(gmr.values) != test.deviceCount {
		test.t.Fatalf("expected %d values but got %d\n", test.deviceCount, len(gmr.values))
	}
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

	tester := &IOTTester{t: t, grp: "iot-dev-grp", deviceCount: 10}
	err = sys.Register("test", tester.receive)
	if err != nil {
		t.Fatalf("failed to register tester: %s\n", err.Error())
	}

	err = sys.Send("", "test", &IOTTesterStart{})
	if err != nil {
		t.Fatalf("failed to send message to tester: %s\n", err.Error())
	}

	time.Sleep(5000 * time.Millisecond)
}
