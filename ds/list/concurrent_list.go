package list

import "sync"

type ConcurrentList struct {
	mu   sync.RWMutex
	list *LinkedList
}

func NewConcurrentList() *ConcurrentList {
	return &ConcurrentList{
		list: NewLinkedList(),
	}
}

func (cl *ConcurrentList) InsertAt(pos int, val []byte) bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.list.InsertAt(pos, val)
}

func (cl *ConcurrentList) GetAt(pos int) []byte {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.list.GetAt(pos)
}

func (cl *ConcurrentList) RemoveAt(pos int) []byte {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.list.RemoveAt(pos)
}

func (cl *ConcurrentList) ForEach(consumer func([]byte) bool) {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	cl.list.ForEach(consumer)
}

func (cl *ConcurrentList) Len() int {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return cl.list.Len()
}
