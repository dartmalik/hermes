package mqtt

import "errors"

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
	}

	return nil, ErrInvalidMsgType
}

func (enc *Encoder) encodeConnack(msg *MqttConnAckMessage) ([]byte, error) {
	p := newPacket()

	p.setPType(PacketTypeConnack)

	p.setRL(2)

	p.vhp = make([]byte, 2)
	if msg.SessionPresent() {
		p.vhp[0] |= 1
	}
	p.vhp[1] = msg.code

	return p.pack(), nil
}
