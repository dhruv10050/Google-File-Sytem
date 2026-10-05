package chunkserver

import (
	"container/list"
	"sync"
	"time"
)

// dataBuffer holds pushed-but-not-yet-applied mutation data, keyed by the
// client-chosen data id (§3.1 step 3: "an internal LRU buffer cache until
// the data is used or aged out"). It is bounded by total bytes and by age.
type dataBuffer struct {
	mu       sync.Mutex
	maxBytes int64
	ttl      time.Duration
	bytes    int64
	order    *list.List // front = oldest
	items    map[string]*list.Element
}

type bufItem struct {
	id   string
	data []byte
	at   time.Time
}

func newDataBuffer(maxBytes int64, ttl time.Duration) *dataBuffer {
	return &dataBuffer{maxBytes: maxBytes, ttl: ttl, order: list.New(), items: map[string]*list.Element{}}
}

func (b *dataBuffer) Put(id string, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e, ok := b.items[id]; ok {
		b.removeLocked(e)
	}
	b.items[id] = b.order.PushBack(&bufItem{id: id, data: data, at: time.Now()})
	b.bytes += int64(len(data))
	b.evictLocked()
}

func (b *dataBuffer) Get(id string) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.items[id]
	if !ok {
		return nil, false
	}
	return e.Value.(*bufItem).data, true
}

func (b *dataBuffer) Delete(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if e, ok := b.items[id]; ok {
		b.removeLocked(e)
	}
}

func (b *dataBuffer) removeLocked(e *list.Element) {
	it := e.Value.(*bufItem)
	b.bytes -= int64(len(it.data))
	delete(b.items, it.id)
	b.order.Remove(e)
}

func (b *dataBuffer) evictLocked() {
	now := time.Now()
	for e := b.order.Front(); e != nil; e = b.order.Front() {
		it := e.Value.(*bufItem)
		if b.bytes <= b.maxBytes && now.Sub(it.at) < b.ttl {
			return
		}
		b.removeLocked(e)
	}
}

// Expire drops entries older than the TTL; called periodically.
func (b *dataBuffer) Expire() {
	b.mu.Lock()
	b.evictLocked()
	b.mu.Unlock()
}
