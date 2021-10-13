package mqtt

import (
	"errors"
	"math"
)

type Encoder struct{}

var (
	ErrInvalidMsg     = errors.New("invalid_msg")
	ErrInvalidMsgType = errors.New("invalid_msg_type")
)

func newEncoder() *Encoder {
	return &Encoder{}
}

func (enc *Encoder) encode(mi interface{}) ([]byte, error) {
	if mi == nil {
		return nil, ErrInvalidMsg
	}

	switch msg := mi.(type) {
	case *MqttConnAckMessage:
		return enc.encodeConnack(msg)

	case *MqttPublishMessage:
		return enc.encodePublish(msg)

	case *MqttPubAckMessage:
		return enc.encodePuback(mi)

	case *MqttPubRecMessage:
		return enc.encodePuback(mi)

	case *MqttPubRelMessage:
		return enc.encodePuback(mi)

	case *MqttPubCompMessage:
		return enc.encodePuback(mi)

	case *MqttSubAckMessage:
		return enc.encodeSuback(msg)

	case *MqttUnSubAckMessage:
		return enc.encodeUnsubAck(msg)

	case *MqttPingRespMessage:
		return enc.encodePingresp()
	}

	return nil, ErrInvalidMsgType
}

func (enc *Encoder) encodeConnack(msg *MqttConnAckMessage) ([]byte, error) {
	vhp := make([]byte, 2)

	if msg.SessionPresent() {
		vhp[0] |= 1
	}
	vhp[1] = msg.code

	p := newPacket()
	p.setPType(PacketTypeConnack)
	p.setRL(2)
	p.vhp = vhp

	return p.pack(), nil
}

func (enc *Encoder) encodePublish(msg *MqttPublishMessage) ([]byte, error) {
	ff := (byte(msg.QosLevel) << 1)
	if msg.Retain {
		ff |= 1
	}
	if msg.Duplicate {
		ff |= 1 << 3
	}

	rl := len(msg.TopicName) + 2
	rl += 2 // PID
	rl += len(msg.Payload)

	vhp := make([]byte, 0, rl)

	vhp, err := enc.encodeBytes(vhp, []byte(msg.TopicName))
	if err != nil {
		return nil, err
	}

	vhp = enc.encodeUint16(vhp, uint16(msg.PacketId))

	_, err = enc.encodeBytes(vhp, msg.Payload)
	if err != nil {
		return nil, err
	}

	p := newPacket()
	p.setPType(PacketTypeConnack)
	p.setFlags(ff)
	p.setRL(uint(rl))
	p.vhp = vhp

	return p.pack(), nil
}

func (enc *Encoder) encodePuback(mi interface{}) ([]byte, error) {
	var pid MqttPacketId
	p := newPacket()

	switch msg := mi.(type) {
	case *MqttPubAckMessage:
		pid = msg.PacketId
		p.setPType(PacketTypePuback)

	case *MqttPubRecMessage:
		pid = msg.PacketId
		p.setPType(PacketTypePubrec)

	case *MqttPubRelMessage:
		pid = msg.PacketId
		p.setPType(PacketTypePubrel)

	case *MqttPubCompMessage:
		pid = msg.PacketId
		p.setPType(PacketTypePubcomp)
	}

	p.setRL(2)
	p.vhp = make([]byte, 0, 2)
	enc.encodeUint16(p.vhp, uint16(pid))

	return p.pack(), nil
}

func (enc *Encoder) encodeSuback(msg *MqttSubAckMessage) ([]byte, error) {
	rl := len(msg.ReturnCodes) + 2 // 1*RCs + pid

	vhp := make([]byte, 0, rl)
	vhp = enc.encodeUint16(vhp, uint16(msg.PacketId))
	for _, s := range msg.ReturnCodes {
		vhp = enc.encodeUint8(vhp, uint8(s))
	}

	p := newPacket()
	p.setPType(PacketTypeSuback)
	p.setRL(uint(rl))
	p.vhp = vhp

	return p.pack(), nil
}

func (enc *Encoder) encodeUnsubAck(msg *MqttUnSubAckMessage) ([]byte, error) {
	vhp := make([]byte, 0, 2)
	vhp = enc.encodeUint16(vhp, uint16(msg.PacketId))

	p := newPacket()
	p.setPType(PacketTypeUnsuback)
	p.setRL(2)
	p.vhp = vhp

	return p.pack(), nil
}

func (enc *Encoder) encodePingresp() ([]byte, error) {
	p := newPacket()
	p.setPType(PacketTypePingresp)

	return p.pack(), nil
}

func (enc *Encoder) encodeUint8(buff []byte, val uint8) []byte {
	return append(buff, val)
}

func (enc *Encoder) encodeUint16(buff []byte, val uint16) []byte {
	buff = append(buff, byte((val&0xFF00)>>8))
	buff = append(buff, byte(val&0x00FF))

	return buff
}

func (enc *Encoder) encodeBytes(buff []byte, val []byte) ([]byte, error) {
	if len(val) > math.MaxUint16 {
		return buff, ErrInvalidStr
	}

	buff = enc.encodeUint16(buff, uint16(len(val)))
	buff = append(buff, []byte(val)...)

	return buff, nil
}
