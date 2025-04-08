package event

import "context"

type EventBusWithContext interface {
	SubscribeWithContext(string, SubscriberWithContext)
	UnSubscribeWithContext(string, SubscriberWithContext)
	PublishWithContext(context.Context, Event)
	PublishAsyncWithContext(context.Context, Event)
}

type SubscriberWithContext interface {
	Handle(context.Context, Event)
}

type SubscribeFuncWithContext func(context.Context, Event)

func (f SubscribeFuncWithContext) Handle(ctx context.Context, evt Event) {
	f(ctx, evt)
}
