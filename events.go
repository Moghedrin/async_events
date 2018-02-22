package events

import (
	"fmt"
	"runtime"
	"sync"
)

type Event interface{}
type Trigger interface{}
type ListenForCondition func(Event) bool
type TriggerToEvent func(Trigger) Event

type TriggerRegister interface {
	RegisterTrigger(string, <-chan Trigger) error
	StartTriggerCombine()<-chan Trigger
}

type ListenerRegister interface {
	RegisterListener(string, ListenForCondition, chan<- Event) error
	StartListenerLoop()
	SendEvent(Event)
}

type TriggerFunctor interface {
	TriggerHandler() TriggerToEvent
}

func CreateTrigger(src string, triggerReg TriggerRegister) (chan<- Trigger, error) {
	toret := make(chan Trigger, 16)
	if err := triggerReg.RegisterTrigger(src, toret); err != nil {
		return nil, err
	}
	return toret, nil
}

func CreateListener(dst string, cond ListenForCondition, listenerReg ListenerRegister) (<-chan Event, error) {
	toret := make(chan Event, 16)
	if err := listenerReg.RegisterListener(dst, cond, toret); err != nil {
		return nil, err
	}
	return toret, nil
}

func StartPipeline(triggers TriggerRegister, functor TriggerFunctor, listeners ListenerRegister) {
	triggerStream := triggers.StartTriggerCombine()
	go listeners.StartListenerLoop()
	for trigger := range(triggerStream) {
		event := functor.TriggerHandler()(trigger)
		listeners.SendEvent(event)
	}
}

type GenTriggerRegister struct {
	sync.RWMutex
	triggerMap map[string]<-chan Trigger
}

type listenerRecord struct {
	Cond ListenForCondition
	EventStream chan<- Event
}

type GenListenerRegister struct {
	sync.RWMutex
	listenerMap map[string]listenerRecord
	eventStream chan Event
}

type GenTriggerFunctor struct {}

type EventPipeline struct {
	Triggers TriggerRegister
	Function TriggerFunctor
	Listeners ListenerRegister
}

func NewGenTriggerFunctor() *GenTriggerFunctor {
	return &GenTriggerFunctor{}
}

func (gtf *GenTriggerFunctor)TriggerHandler() TriggerToEvent {
	return func(i Trigger) Event { return i; }
}

func NewGenTriggerRegister() *GenTriggerRegister {
	return &GenTriggerRegister{ triggerMap : make(map[string]<-chan Trigger) }
}

func (gtr *GenTriggerRegister)RegisterTrigger(src string, triggerChan <-chan Trigger) error {
	gtr.RLock()
	if _, ok := gtr.triggerMap[src]; ok {
		return fmt.Errorf("Source [%s] is already present in Trigger Register.")
	}
	gtr.RUnlock()
	gtr.Lock()
	gtr.triggerMap[src] = triggerChan
	gtr.Unlock()
	return nil
}

func (gtr *GenTriggerRegister)StartTriggerCombine()<-chan Trigger {
	toret := make(chan Trigger, 16)
	go func(gtri *GenTriggerRegister, fanInChan chan<- Trigger) {
		for {
			gtri.RLock()
			for _, source := range(gtri.triggerMap) {
				select {
				case s := <-source:
					fanInChan <- s
				default:
					runtime.Gosched()
				} }
			gtri.RUnlock()
			runtime.Gosched()
		}
	}(gtr, toret)
	return toret
}

func NewGenListenerRegister() *GenListenerRegister {
	return &GenListenerRegister{ listenerMap : make(map[string]listenerRecord), eventStream : make(chan Event, 16) }
}

func (glr *GenListenerRegister)RegisterListener(dst string, cond ListenForCondition, estream chan<- Event) error {
	glr.RLock()
	if _, ok := glr.listenerMap[dst]; ok {
		return fmt.Errorf("Destination [%s] is already present in Listener Register.")
	}
	glr.RUnlock()
	glr.Lock()
	glr.listenerMap[dst] = listenerRecord{ Cond : cond, EventStream : estream }
	glr.Unlock()
	return nil
}

func (glr *GenListenerRegister)StartListenerLoop() {
	for event := range(glr.eventStream) {
		glr.RLock()
		for _, listener := range(glr.listenerMap) {
			if listener.Cond(event) {
				select {
				case listener.EventStream<-event:
				default:
					// If we can't push the event to the listener, it's not our problem.
				}
			}
		}
		glr.RUnlock()
	}
}

func (glr *GenListenerRegister)SendEvent(event Event) {
	glr.eventStream <- event
}

func NewEventPipeline(triggers TriggerRegister, functor TriggerFunctor, listeners ListenerRegister) *EventPipeline {
	toret := &EventPipeline{ Triggers : triggers, Function : functor, Listeners : listeners }
	go StartPipeline(toret.Triggers, toret.Function, toret.Listeners)
	return toret
}

func NewGenEventPipeline() *EventPipeline {
	tr := NewGenTriggerRegister()
	tf := NewGenTriggerFunctor()
	lr := NewGenListenerRegister()
	return NewEventPipeline(tr, tf, lr)
}

func (ep *EventPipeline)CreateTrigger(src string) (chan<- Trigger, error) {
	return CreateTrigger(src, ep.Triggers)
}

func (ep *EventPipeline)CreateListener(dst string, cond ListenForCondition) (<-chan Event, error) {
	return CreateListener(dst, cond, ep.Listeners)
}

func AlwaysTrue(_ Event) bool {
	return true
}
