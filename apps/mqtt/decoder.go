package mqtt

import (
	"errors"
)

const (
	PacketTypeReserved0 byte = iota
	PacketTypeConnect
	PacketTypeConnack
	PacketTypePublish
	PacketTypePuback
	PacketTypePubrec
	PacketTypePubrel
	PacketTypePubcomp
	PacketTypeSubscribe
	PacketTypeSuback
	PacketTypeUnsubscribe
	PacketTypeUnsuback
	PacketTypePingreq
	PacketTypePingresp
	PacketTypeDisconnect
	PacketTypeReserved15

	DecoderStatePTF      = 1
	DecoderStateRLen     = 2
	DecoderStateVHP      = 3
	DecoderStateComplete = 4

	Protocol    = "MQTT"
	ProtocolLvl = 4
)

var (
	ErrCodecInvalidPacketType   = errors.New("invalid_packet_type")
	ErrCodecInvalidFixedFlags   = errors.New("invalid_fixed_flags")
	ErrCodecInvalidRemainingLen = errors.New("invalid_remaining_len")
	ErrCodecInvalidPacket       = errors.New("invalid_packet")
	ErrCodecInvalidProtocol     = errors.New("invalid_protocol")
	ErrCodecInvalidProtoLvl     = errors.New("invalid_protocol_level")
	ErrCodecInvalidConnectFlags = errors.New("invalid_connect_flags")
	ErrCodecInvalidKeepAlive    = errors.New("invalid_keep_alive")
	ErrCodecInvalidClientID     = errors.New("invalid_client_id")
	ErrCodecInvalidWillTopic    = errors.New("invalid_will_topic")
	ErrCodecInvalidWillMsg      = errors.New("invalid_will_msg")
	ErrCodecInvalidWillQos      = errors.New("invalid_will_qos")
	ErrCodecInvalidWillFlags    = errors.New("invalid_will_flags")
	ErrCodecInvalidUsername     = errors.New("invalid_username")
	ErrCodecInvalidPassword     = errors.New("invalid_password")
	ErrCodecInvalidUint8        = errors.New("invalid_uint8")
	ErrCodecInvalidUint16       = errors.New("invalid_uint16")
	ErrCodecInvalidStr          = errors.New("invalid_string")
	ErrCodecInvalidBytes        = errors.New("invalid_bytes")
	ErrCodecInvalidPacketID     = errors.New("invalid_packet_id")
	ErrCodecInvalidPayload      = errors.New("invalid_payload")
	ErrCodecInvalidSubscription = errors.New("invalid_subscription")
)

type Packet struct {
	ptf byte
	rl  []byte
	vhp []byte
}

func newPacket() *Packet {
	return &Packet{rl: make([]byte, 0, 4)}
}

func (p *Packet) setPType(t byte) {
	p.ptf &= 0x0F
	p.ptf |= (t << 4)
}

func (p *Packet) pType() byte {
	return byte((p.ptf & 0xF0) >> 4)
}

func (p *Packet) setFlags(flags byte) {
	p.ptf &= 0xF0
	p.ptf |= flags
}

func (p *Packet) flags() int {
	return int(p.ptf & 0x0F)
}

func (p *Packet) setRL(x uint) {
	p.rl = p.rl[:0]

	for x > 0 {
		eb := byte(x % 128)
		x = x / 128

		if x > 0 {
			eb = eb | 128
		}

		p.rl = append(p.rl, eb)
	}
}

func (p *Packet) remainingLength() int {
	mult := 1
	value := 0

	for _, eb := range p.rl {
		value += int(eb&127) * mult
		mult *= 128
	}

	return value
}

func (p *Packet) pack() []byte {
	sz := 1 + len(p.rl) + len(p.vhp)
	b := make([]byte, 0, sz)

	b = append(b, p.ptf)
	b = append(b, p.rl...)
	b = append(b, p.vhp...)

	return b
}

type Decoder struct {
	state  int
	packet *Packet
}

func newDecoder() *Decoder {
	return &Decoder{state: DecoderStatePTF}
}

func (dec *Decoder) decode(buff []byte) ([]interface{}, error) {
	mm := make(map[int]interface{})
	mi := 0

	for len(buff) > 0 || dec.state == DecoderStateComplete {
		switch dec.state {
		case DecoderStatePTF:
			b, s, err := dec.decodePTF(buff)
			if err != nil {
				return nil, err
			}
			buff = b
			dec.state = s

		case DecoderStateRLen:
			b, s, err := dec.decodeRL(buff)
			if err != nil {
				return nil, err
			}
			buff = b
			dec.state = s

		case DecoderStateVHP:
			b, s, err := dec.decodeVHP(buff)
			if err != nil {
				return nil, err
			}
			buff = b
			dec.state = s

		case DecoderStateComplete:
			msg, err := dec.msg()
			if err != nil {
				return nil, err
			}

			mm[mi] = msg
			mi++
			dec.packet = nil
			dec.state = DecoderStatePTF
		}
	}

	msgs := make([]interface{}, 0, len(mm))
	for i := 0; i < len(mm); i++ {
		msgs = append(msgs, mm[i])
	}

	return msgs, nil
}

func (dec *Decoder) decodePTF(buff []byte) ([]byte, int, error) {
	ptf := buff[0]
	pt := byte((ptf & 0xF0) >> 4)
	if pt == PacketTypeReserved0 || pt == PacketTypeReserved15 {
		return buff, DecoderStatePTF, ErrCodecInvalidPacketType
	}

	dec.packet = newPacket()
	dec.packet.ptf = ptf

	return buff[1:], DecoderStateRLen, nil
}

func (dec *Decoder) decodeRL(buff []byte) ([]byte, int, error) {
	s := DecoderStateRLen
	for len(buff) > 0 {
		if len(dec.packet.rl) == 4 {
			return buff, DecoderStatePTF, ErrCodecInvalidRemainingLen
		}

		eb := buff[0]
		buff = buff[1:]
		dec.packet.rl = append(dec.packet.rl, eb)

		if eb&128 == 0 {
			s = DecoderStateVHP
			break
		}
	}

	if s == DecoderStateVHP {
		rl := dec.packet.remainingLength()
		if rl > 128*128*128 {
			return buff, DecoderStatePTF, ErrCodecInvalidRemainingLen
		}

		dec.packet.vhp = make([]byte, 0, rl)
	}

	return buff, s, nil
}

func (dec *Decoder) decodeVHP(buff []byte) ([]byte, int, error) {
	rl := dec.packet.remainingLength()
	sz := rl - len(dec.packet.vhp)
	if sz > len(buff) {
		sz = len(buff)
	}

	dec.packet.vhp = append(dec.packet.vhp, buff[:sz]...)

	s := DecoderStateVHP
	if len(dec.packet.vhp) == rl {
		s = DecoderStateComplete
	}

	return buff[sz:], s, nil
}

func (dec *Decoder) msg() (interface{}, error) {
	switch dec.packet.pType() {
	case PacketTypeConnect:
		return dec.decodeConnect()

	case PacketTypeConnack:
		return dec.decodeConnack()

	case PacketTypePublish:
		return dec.decodePublish()

	case PacketTypePubcomp:
		fallthrough
	case PacketTypePubrel:
		fallthrough
	case PacketTypePubrec:
		fallthrough
	case PacketTypePuback:
		return dec.decodePuback()

	case PacketTypeSubscribe:
		return dec.decodeSubscribe()

	case PacketTypeUnsubscribe:
		return dec.decodeUnsubscribe()

	case PacketTypePingreq:
		return dec.decodePingreq()

	case PacketTypeDisconnect:
		return dec.decodeDisconnect()
	}

	return nil, ErrCodecInvalidPacketType
}

func (dec *Decoder) decodeConnect() (interface{}, error) {
	proto, vhp, err := dec.decodeStr(dec.packet.vhp)
	if err != nil || proto != Protocol {
		return nil, ErrCodecInvalidProtocol
	}

	plvl, vhp, err := dec.decodeUint8(vhp)
	if err != nil || plvl != ProtocolLvl {
		return nil, ErrCodecInvalidProtoLvl
	}

	cf, vhp, err := dec.decodeUint8(vhp)
	if err != nil || cf&MqttConnectFlagsReserved != 0 {
		return nil, ErrCodecInvalidConnectFlags
	}

	ka, vhp, err := dec.decodeUint16(vhp)
	if err != nil {
		return nil, ErrCodecInvalidKeepAlive
	}

	cid, vhp, err := dec.decodeStr(vhp)
	if err != nil || cid == "" {
		return nil, ErrCodecInvalidClientID
	}

	var willTopic string
	var willMsg []byte
	if cf&MqttConnectFlagsWill != 0 {
		willTopic, vhp, err = dec.decodeStr(vhp)
		if err != nil {
			return nil, ErrCodecInvalidWillTopic
		}

		willMsg, vhp, err = dec.decodeBytes(vhp)
		if err != nil {
			return nil, ErrCodecInvalidWillMsg
		}

		willQoS := MqttQoSLevel((cf & MqttConnectFlagsWillQoS) >> 3)
		if willQoS > MqttQoSLevel2 {
			return nil, ErrCodecInvalidWillQos
		}
	} else {
		if cf&MqttConnectFlagsWillQoS != 0 || cf&MqttConnectFlagsWillRetain != 0 {
			return nil, ErrCodecInvalidWillFlags
		}
	}

	var username string
	if cf&MqttConnectFlagsUsername != 0 {
		username, vhp, err = dec.decodeStr(vhp)
		if err != nil {
			return nil, ErrCodecInvalidUsername
		}
	}

	var password string
	if cf&MqttConnectFlagsPassword != 0 {
		password, _, err = dec.decodeStr(vhp)
		if err != nil {
			return nil, ErrCodecInvalidPassword
		}
	}

	msg := &MqttConnectMessage{
		protocol:      proto,
		protocolLevel: plvl,
		flags:         cf,
		keepAlive:     ka,
		clientId:      MqttClientId(cid),
		username:      username,
		password:      password,
		willTopic:     MqttTopicName(willTopic),
		willMsg:       willMsg,
	}

	return msg, nil
}

func (dec *Decoder) decodeConnack() (interface{}, error) {
	msg := &MqttConnAckMessage{}

	msg.SetSessionPresent(dec.packet.vhp[0]&0x1 != 0)
	msg.code = dec.packet.vhp[1]

	return msg, nil
}

func (dec *Decoder) decodePublish() (interface{}, error) {
	ff := dec.packet.flags()
	qos := MqttQoSLevel((ff & 0x06) >> 1)
	dup := (ff&0x08)>>3 != 0
	retain := (ff & 0x01) != 0

	if qos > MqttQoSLevel2 {
		return nil, ErrCodecInvalidFixedFlags
	}
	if qos == MqttQoSLevel0 && dup {
		return nil, ErrCodecInvalidFixedFlags
	}

	topic, vhp, err := dec.decodeTopicName(dec.packet.vhp)
	if err != nil || topic == "" {
		return nil, ErrInvalidTopicName
	}

	var pid MqttPacketId
	if qos > MqttQoSLevel0 {
		pid, vhp, err = dec.decodePID(vhp, qos)
		if err != nil {
			return nil, ErrCodecInvalidPacketID
		}
	}

	vhsize := len(dec.packet.vhp) - len(vhp)
	psize := dec.packet.remainingLength() - vhsize
	payload, _, err := dec.decodeNumBytes(vhp, psize)
	if err != nil {
		return nil, ErrCodecInvalidPayload
	}

	msg := &MqttPublishMessage{
		TopicName: topic,
		Payload:   payload,
		Duplicate: dup,
		QosLevel:  qos,
		Retain:    retain,
	}
	if qos > MqttQoSLevel0 {
		msg.PacketId = pid
	}

	return msg, nil
}

func (dec *Decoder) decodePuback() (interface{}, error) {
	pid, _, err := dec.decodePID(dec.packet.vhp, MqttQoSLevel2)
	if err != nil {
		return nil, ErrCodecInvalidPacketID
	}

	switch dec.packet.pType() {
	case PacketTypePuback:
		return &MqttPubAckMessage{PacketId: MqttPacketId(pid)}, nil

	case PacketTypePubrec:
		return &MqttPubRecMessage{PacketId: pid}, nil

	case PacketTypePubrel:
		return &MqttPubRelMessage{PacketId: pid}, nil

	case PacketTypePubcomp:
		return &MqttPubCompMessage{PacketId: pid}, nil
	}

	return nil, ErrCodecInvalidPacketType
}

func (dec *Decoder) decodeSubscribe() (interface{}, error) {
	pid, vhp, err := dec.decodePID(dec.packet.vhp, MqttQoSLevel2)
	if err != nil {
		return nil, ErrCodecInvalidPacketID
	}

	sm := make(map[int]*MqttSubscription)
	si := 0
	for len(vhp) > 0 {
		var f string
		f, vhp, err = dec.decodeStr(vhp)
		if err != nil || f == "" {
			return nil, ErrCodecInvalidSubscription
		}

		var qos uint8
		qos, vhp, err = dec.decodeUint8(vhp)
		if err != nil || qos > uint8(MqttQoSLevel2) {
			return nil, ErrCodecInvalidSubscription
		}

		sub := &MqttSubscription{QosLevel: MqttQoSLevel(qos), TopicFilter: MqttTopicFilter(f)}
		sm[si] = sub
		si++
	}

	if len(sm) == 0 {
		return nil, ErrCodecInvalidSubscription
	}

	subs := make([]*MqttSubscription, 0, len(sm))
	for i := 0; i < len(sm); i++ {
		subs = append(subs, sm[i])
	}

	msg := &MqttSubscribeMessage{PacketId: pid, Subscriptions: subs}

	return msg, nil
}

func (dec *Decoder) decodeUnsubscribe() (interface{}, error) {
	pid, vhp, err := dec.decodePID(dec.packet.vhp, MqttQoSLevel2)
	if err != nil {
		return nil, ErrCodecInvalidPacketID
	}

	fm := make(map[int]string)
	si := 0
	for len(vhp) > 0 {
		var f string
		f, vhp, err = dec.decodeStr(vhp)
		if err != nil || f == "" {
			return nil, ErrCodecInvalidSubscription
		}

		fm[si] = f
		si++
	}

	if len(fm) == 0 {
		return nil, ErrCodecInvalidPacket
	}

	filters := make([]MqttTopicFilter, 0, len(fm))
	for i := 0; i < len(fm); i++ {
		filters = append(filters, MqttTopicFilter(fm[i]))
	}

	msg := &MqttUnsubscribeMessage{PacketId: pid, TopicFilters: filters}

	return msg, nil
}

func (dec *Decoder) decodePingreq() (interface{}, error) {
	return &MqttPingReqMessage{}, nil
}

func (dec *Decoder) decodeDisconnect() (interface{}, error) {
	return &MqttDisconnectMessage{}, nil
}

func (dec *Decoder) decodeUint8(buff []byte) (uint8, []byte, error) {
	if len(buff) < 1 {
		return 0, buff, ErrCodecInvalidUint8
	}

	return uint8(buff[0]), buff[1:], nil
}

func (dec *Decoder) decodeUint16(buff []byte) (uint16, []byte, error) {
	if len(buff) < 2 {
		return 0, buff, ErrCodecInvalidUint16
	}

	return uint16(buff[0])<<8 | uint16(buff[1]), buff[2:], nil
}

func (dec *Decoder) decodeStr(buff []byte) (string, []byte, error) {
	sz, buff, err := dec.decodeUint16(buff)
	if err != nil {
		return "", buff, err
	}

	if len(buff) < int(sz) {
		return "", buff, ErrCodecInvalidStr
	}

	return string(buff[:sz]), buff[sz:], nil
}

func (dec *Decoder) decodeBytes(buff []byte) ([]byte, []byte, error) {
	sz, buff, err := dec.decodeUint16(buff)
	if err != nil {
		return nil, buff, err
	}

	return dec.decodeNumBytes(buff, int(sz))
}

func (dec *Decoder) decodeNumBytes(buff []byte, num int) ([]byte, []byte, error) {
	if len(buff) < num {
		return nil, buff, ErrCodecInvalidBytes
	}

	if num == 0 {
		return nil, buff, nil
	} else {
		return buff[:num], buff[num:], nil
	}
}

func (dec *Decoder) decodeTopicName(buff []byte) (MqttTopicName, []byte, error) {
	t, buff, err := dec.decodeStr(buff)
	if err != nil {
		return "", buff, err
	}

	topic, err := NewTopicName(t)

	return topic, buff, err
}

func (dec *Decoder) decodePID(buff []byte, qos MqttQoSLevel) (MqttPacketId, []byte, error) {
	if qos < MqttQoSLevel1 {
		return 0, buff, ErrCodecInvalidPacketID
	}

	ui, buff, err := dec.decodeUint16(buff)
	if err != nil {
		return 0, buff, err
	}

	return MqttPacketId(ui), buff, nil
}
