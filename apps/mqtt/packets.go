package mqtt

import (
	"errors"
	"strings"
)

type ClientId string

type QoSLevel int

const (
	QoSLevel0     QoSLevel = 0
	QoSLevel1     QoSLevel = 1
	QoSLevel2     QoSLevel = 2
	QosLevelCount QoSLevel = 3
)

var (
	ErrInvalidTopicName = errors.New("invalid_topic_name")
)

type TopicName string

func NewTopicName(value string) (TopicName, error) {
	if strings.ContainsAny(value, "#+") {
		return "", ErrInvalidTopicName
	}

	return TopicName(value), nil
}

type TopicFilter string

func (f TopicFilter) topicName() TopicName {
	segments := strings.Split(string(f), "/")
	var builder strings.Builder

	for _, segment := range segments {
		if segment == "#" || segment == "+" {
			break
		}

		builder.WriteString(segment)
	}

	return TopicName(builder.String())
}

const (
	ConnectFlagsReserved     = 1 << 0
	ConnectFlagsCleanSession = 1 << 1
	ConnectFlagsWill         = 1 << 2
	ConnectFlagsWillQoS      = 1<<3 | 1<<4
	ConnectFlagsWillRetain   = 1 << 5
	ConnectFlagsPassword     = 1 << 6
	ConnectFlagsUsername     = 1 << 7
)

type ConnectMessage struct {
	Protocol      string
	ProtocolLevel uint8
	Flags         uint8
	KeepAlive     uint16
	ClientId      ClientId
	Username      string
	Password      string
	WillTopic     TopicName
	WillMsg       []byte
}

func (m *ConnectMessage) Reserved() bool {
	return m.Flags&ConnectFlagsReserved != 0
}

func (m *ConnectMessage) HasCleanSession() bool {
	return m.Flags&ConnectFlagsCleanSession != 0
}

func (m *ConnectMessage) SetCleanSession(status bool) {
	if status {
		m.Flags |= ConnectFlagsCleanSession
	} else {
		m.Flags = m.Flags &^ ConnectFlagsCleanSession
	}
}

func (m *ConnectMessage) HasWill() bool {
	return m.Flags&ConnectFlagsWill != 0
}

func (m *ConnectMessage) WillQoS() QoSLevel {
	q := uint8((m.Flags & ConnectFlagsWillQoS) >> 3)

	switch q {
	case 0:
		return QoSLevel0
	case 1:
		return QoSLevel1
	case 2:
		return QoSLevel2
	default:
		return QosLevelCount
	}
}

func (m *ConnectMessage) HasWillRetain() bool {
	return m.Flags&ConnectFlagsWillRetain != 0
}

func (m *ConnectMessage) HasPassword() bool {
	return m.Flags&ConnectFlagsPassword != 0
}

func (m *ConnectMessage) HasUsername() bool {
	return m.Flags&ConnectFlagsUsername != 0
}

const (
	ConnackFlagsSessionPresent    = 1 << 0
	ConnackCodeAccepted           = 0x00
	ConnackCodeInvalidProtocolVer = 0x01
	ConnackCodeIdRejected         = 0x02
	ConnackCodeUnavailable        = 0x03
	ConnackCodeBadUserOrPass      = 0x04
	ConnackCodeNotAuthz           = 0x05
)

type ConnAckMessage struct {
	ackFlags uint8
	code     uint8
}

func (m *ConnAckMessage) SessionPresent() bool {
	return m.ackFlags&ConnackFlagsSessionPresent != 0
}

func (m *ConnAckMessage) SetSessionPresent(status bool) {
	if status {
		m.ackFlags |= ConnackFlagsSessionPresent
	} else {
		m.ackFlags = m.ackFlags &^ ConnackFlagsSessionPresent
	}
}

type DisconnectMessage struct{}

type PingReqMessage struct{}

type PingRespMessage struct{}

type PacketId uint16

type PublishMessage struct {
	TopicName TopicName
	Payload   []byte
	PacketId  PacketId
	Duplicate bool
	QosLevel  QoSLevel
	Retain    bool
}

type PubAckMessage struct {
	PacketId PacketId
}

type PubRecMessage struct {
	PacketId PacketId
}

type PubRelMessage struct {
	PacketId PacketId
}

type PubCompMessage struct {
	PacketId PacketId
}

type Subscription struct {
	QosLevel    QoSLevel
	TopicFilter TopicFilter
}

type SubscribeMessage struct {
	PacketId      PacketId
	Subscriptions []*Subscription
}

func (m *SubscribeMessage) topicFilter() []TopicFilter {
	filters := make([]TopicFilter, len(m.Subscriptions))
	for _, sub := range m.Subscriptions {
		filters = append(filters, sub.TopicFilter)
	}

	return filters
}

type SubAckStatus uint8

const (
	SubAckQos0Success = 0x00
	SubAckQos1Success = 0x01
	SubAckQos2Success = 0x02
	SubAckFailure     = 0x80
)

type SubAckMessage struct {
	PacketId    PacketId
	ReturnCodes []SubAckStatus
}

type UnsubscribeMessage struct {
	PacketId     PacketId
	TopicFilters []TopicFilter
}

type UnSubAckMessage struct {
	PacketId PacketId
}
