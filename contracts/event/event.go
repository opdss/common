package event

type Topic string

type Event interface {
	Topic() Topic
	Payload() any
}
