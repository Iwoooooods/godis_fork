package set

import "sync"

type Set interface {
	Add(members ...string) int
	Remove(members ...string) int
	Contains(member string) bool
	Members() []string
	Cardinality() int
}

// ConcurrentSet is a set of strings
type ConcurrentSet struct {
	members map[string]struct{}
	mu      sync.RWMutex
}

func NewSet() *ConcurrentSet {
	return &ConcurrentSet{
		members: make(map[string]struct{}),
	}
}

func (s *ConcurrentSet) Add(members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	counter := 0
	for _, m := range members {
		if _, ok := s.members[m]; !ok {
			s.members[m] = struct{}{}
			counter++
		}
	}

	return counter
}

func (s *ConcurrentSet) Remove(members ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	removed := 0
	for _, member := range members {
		if _, exists := s.members[member]; exists {
			delete(s.members, member)
			removed++
		}
	}
	return removed
}

// Contains checks membership (SISMEMBER)
func (s *ConcurrentSet) Contains(member string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.members[member]
	return exists
}

// Members returns all elements as slice (SMEMBERS)
func (s *ConcurrentSet) Members() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	members := make([]string, 0, len(s.members))
	for member := range s.members {
		members = append(members, member)
	}
	return members
}

// Cardinality returns set size (SCARD)
func (s *ConcurrentSet) Cardinality() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.members)
}

func (s *ConcurrentSet) Intersect(other *ConcurrentSet) *ConcurrentSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	result := NewSet()
	result.mu.Lock()
	defer result.mu.Unlock()

	for member := range s.members {
		if _, exists := other.members[member]; exists {
			result.members[member] = struct{}{}
		}
	}
	return result
}

func (s *ConcurrentSet) Union(other *ConcurrentSet) *ConcurrentSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	result := NewSet()
	for member := range s.members {
		result.members[member] = struct{}{}
	}
	for member := range other.members {
		result.members[member] = struct{}{}
	}
	return result
}

func (s *ConcurrentSet) Diff(other *ConcurrentSet) *ConcurrentSet {
	s.mu.RLock()
	defer s.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	result := NewSet()
	for member := range s.members {
		if _, exists := other.members[member]; !exists {
			result.members[member] = struct{}{}
		}
	}
	return result
}

// Iteration helper (for SCAN-like operations)
func (s *ConcurrentSet) ForEach(consumer func(member string) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for member := range s.members {
		if !consumer(member) {
			break
		}
	}
}
