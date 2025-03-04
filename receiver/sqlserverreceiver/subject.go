package sqlserverreceiver

import "sync"

// This is an implementation of a Pub/Sub Subject that can be published to and subscribed to.

type Subject struct {
	mutex       sync.RWMutex
	subscribers []chan any // We don't know the type yet, but we do want it to ultimately be a specific struct or interface.
	quit        chan struct{}
	closed      bool
}

// NewSubject creates a new Subject instance.
// size is the size of the buffer for each subscriber.
func NewSubject(size int) Subject {
	return Subject{
		subscribers: make([]chan any, size),
		quit:        make(chan struct{}),
	}
}

// Subscribe adds a new subscriber to the Subject.
func (s *Subject) Subscribe() <-chan any {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ch := make(chan any, 1)
	s.subscribers = append(s.subscribers, ch)

	return ch
}

// Publish sends a message to all subscribers.
func (s *Subject) Publish(msg any) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	for _, ch := range s.subscribers {
		select {
		case ch <- msg:
		default:
		}
	}
}

// Close closes the Subject and all its subscribers.
func (s *Subject) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.closed {
		return
	}

	s.closed = true
	close(s.quit)

	for _, ch := range s.subscribers {
		close(ch)
	}
}
