package event

type EventBus interface {
	Subscribe(Topic, Subscriber)
	UnSubscribe(Topic, Subscriber)
	Publish(Event)
	PublishAsync(Event)
}

type Subscriber interface {
	Handle(Event)
}

type SubscribeFunc func(Event)

func (f SubscribeFunc) Handle(evt Event) {
	f(evt)
}
