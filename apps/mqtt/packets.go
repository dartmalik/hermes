package mqtt

import (
	"errors"
	"strings"
)

type MqttClientId string

type MqttQoSLevel int

const (
	MqttQoSLevel0     MqttQoSLevel = 0
	MqttQoSLevel1     MqttQoSLevel = 1
	MqttQoSLevel2     MqttQoSLevel = 2
	MqttQosLevelCount MqttQoSLevel = 3
)

var (
	ErrInvalidTopicName = errors.New("invalid_topic_name")
)

type MqttTopicName string

func NewTopicName(value string) (MqttTopicName, error) {
	if strings.ContainsAny(value, "#+") {
		return "", ErrInvalidTopicName
	}

	return MqttTopicName(value), nil
}

type MqttTopicFilter string

func (f MqttTopicFilter) topicName() MqttTopicName {
	segments := strings.Split(string(f), "/")
	var builder strings.Builder

	for _, segment := range segments {
		if segment == "#" || segment == "+" {
			break
		}

		builder.WriteString(segment)
	}

	return MqttTopicName(builder.String())
}

const (
	MqttConnectFlagsReserved     = 1 << 0
	MqttConnectFlagsCleanSession = 1 << 1
	MqttConnectFlagsWill         = 1 << 2
	MqttConnectFlagsWillQoS      = 1<<3 | 1<<4
	MqttConnectFlagsWillRetain   = 1 << 5
	MqttConnectFlagsPassword     = 1 << 6
	MqttConnectFlagsUsername     = 1 << 7
)

type MqttConnectMessage struct {
	protocol      string
	protocolLevel uint8
	flags         uint8
	keepAlive     uint16
	clientId      MqttClientId
	username      string
	password      string
	willTopic     MqttTopicName
	willMsg       []byte
}

func (m *MqttConnectMessage) Reserved() bool {
	return m.flags&MqttConnectFlagsReserved != 0
}

func (m *MqttConnectMessage) HasCleanSession() bool {
	return m.flags&MqttConnectFlagsCleanSession != 0
}

func (m *MqttConnectMessage) SetCleanSession(status bool) {
	if status {
		m.flags |= MqttConnectFlagsCleanSession
	} else {
		m.flags = m.flags &^ MqttConnectFlagsCleanSession
	}
}

func (m *MqttConnectMessage) HasWill() bool {
	return m.flags&MqttConnectFlagsWill != 0
}

func (m *MqttConnectMessage) WillQoS() MqttQoSLevel {
	q := uint8((m.flags & MqttConnectFlagsWillQoS) >> 3)

	switch q {
	case 0:
		return MqttQoSLevel0
	case 1:
		return MqttQoSLevel1
	case 2:
		return MqttQoSLevel2
	default:
		return MqttQosLevelCount
	}
}

func (m *MqttConnectMessage) HasWillRetain() bool {
	return m.flags&MqttConnectFlagsWillRetain != 0
}

func (m *MqttConnectMessage) HasPassword() bool {
	return m.flags&MqttConnectFlagsPassword != 0
}

func (m *MqttConnectMessage) HasUsername() bool {
	return m.flags&MqttConnectFlagsUsername != 0
}

const (
	MqttConnackFlagsSessionPresent    = 1 << 0
	MqttConnackCodeAccepted           = 0x00
	MqttConnackCodeInvalidProtocolVer = 0x01
	MqttConnackCodeIdRejected         = 0x02
	MqttConnackCodeUnavailable        = 0x03
	MqttConnackCodeBadUserOrPass      = 0x04
	MqttConnackCodeNotAuthz           = 0x05
)

type MqttConnAckMessage struct {
	ackFlags uint8
	code     uint8
}

func (m *MqttConnAckMessage) SessionPresent() bool {
	return m.ackFlags&MqttConnackFlagsSessionPresent != 0
}

func (m *MqttConnAckMessage) SetSessionPresent(status bool) {
	if status {
		m.ackFlags |= MqttConnackFlagsSessionPresent
	} else {
		m.ackFlags = m.ackFlags &^ MqttConnackFlagsSessionPresent
	}
}

type MqttDisconnectMessage struct{}

type MqttPingReqMessage struct{}

type MqttPacketId uint16

type MqttPublishMessage struct {
	TopicName MqttTopicName
	Payload   []byte
	PacketId  MqttPacketId
	Duplicate bool
	QosLevel  MqttQoSLevel
	Retain    bool
}

type MqttPubAckMessage struct {
	PacketId MqttPacketId
}

type MqttPubRecMessage struct {
	PacketId MqttPacketId
}

type MqttPubRelMessage struct {
	PacketId MqttPacketId
}

type MqttPubCompMessage struct {
	PacketId MqttPacketId
}

type MqttSubscription struct {
	QosLevel    MqttQoSLevel
	TopicFilter MqttTopicFilter
}

type MqttSubscribeMessage struct {
	PacketId      MqttPacketId
	Subscriptions []*MqttSubscription
}

func (m *MqttSubscribeMessage) topicFilter() []MqttTopicFilter {
	filters := make([]MqttTopicFilter, len(m.Subscriptions))
	for _, sub := range m.Subscriptions {
		filters = append(filters, sub.TopicFilter)
	}

	return filters
}

type MqttSubAckStatus uint8

const (
	MqttSubAckQos0Success = 0x00
	MqttSubAckQos1Success = 0x01
	MqttSubAckQos2Success = 0x02
	MqttSubAckFailure     = 0x80
)

func toSubAckStatus(qosLevel MqttQoSLevel) MqttSubAckStatus {
	switch qosLevel {
	case MqttQoSLevel0:
		return MqttSubAckQos0Success

	case MqttQoSLevel1:
		return MqttSubAckQos1Success

	case MqttQoSLevel2:
		return MqttSubAckQos2Success

	default:
		return MqttSubAckFailure
	}
}

type MqttSubAckMessage struct {
	PacketId    MqttPacketId
	ReturnCodes []MqttSubAckStatus
}

type MqttUnsubscribeMessage struct {
	PacketId     MqttPacketId
	TopicFilters []MqttTopicFilter
}

type MqttUnSubAckMessage struct {
	PacketId MqttPacketId
}
