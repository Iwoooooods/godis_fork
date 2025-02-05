package list

import "log"

type Node struct {
	Value string
	Next  *Node
	Prev  *Node
}

// linked list in redis stores string only
type LinkedList struct {
	head   *Node
	tail   *Node
	length int
}

func NewLinkedList() *LinkedList {
	return &LinkedList{
		head:   nil,
		tail:   nil,
		length: 0,
	}
}

func (ll *LinkedList) Len() int {
	return ll.length
}

func (ll *LinkedList) InsertAt(pos int, val []byte) bool {
	if pos < 0 || pos > ll.length {
		log.Printf("invalid pos argument")
		return false
	}

	newNode := &Node{Value: string(val)}

	if ll.length == 0 {
		ll.head = newNode
		ll.tail = newNode
		ll.length++
		return true
	}

	if pos == 0 {
		newNode.Next = ll.head
		ll.head.Prev = newNode
		ll.head = newNode
		ll.length++
		return true
	}

	if pos == ll.length {
		newNode.Prev = ll.tail
		ll.tail.Next = newNode
		ll.tail = newNode
		ll.length++
		return true
	}

	curr := ll.head
	for i := 0; i < pos; i++ {
		curr = curr.Next
	}
	newNode.Prev = curr.Prev
	newNode.Next = curr
	curr.Prev.Next = newNode
	curr.Prev = newNode
	ll.length++
	return true

}

func (ll *LinkedList) RemoveAt(pos int) []byte {
	if pos < 0 || pos >= ll.length || ll.length == 0 {
		return nil
	}

	var removedNode *Node

	// Remove head
	if pos == 0 {
		removedNode = ll.head
		ll.head = ll.head.Next
		if ll.head != nil {
			ll.head.Prev = nil
		} else {
			ll.tail = nil
		}
	} else if pos == ll.length-1 { // Remove tail
		removedNode = ll.tail
		ll.tail = ll.tail.Prev
		ll.tail.Next = nil
	} else { // Remove from middle
		curr := ll.head
		for i := 0; i < pos; i++ {
			curr = curr.Next
		}
		removedNode = curr
		curr.Prev.Next = curr.Next
		curr.Next.Prev = curr.Prev
	}

	ll.length--
	return []byte(removedNode.Value)
}

func (ll *LinkedList) GetAt(pos int) []byte {
	// Handle negative indices
	if pos < 0 {
		pos = ll.length + pos // Convert to positive index
	}

	if pos >= ll.length {
		return nil
	}

	curr := ll.head
	for i := 0; i < pos; i++ {
		curr = curr.Next
	}
	return []byte(curr.Value)
}

func (ll *LinkedList) ForEach(consumer func([]byte) bool) {
	curr := ll.head
	for curr != nil {
		if !consumer([]byte(curr.Value)) {
			break
		}
		curr = curr.Next
	}
}

func (ll *LinkedList) Clear() {
	ll.head = nil
	ll.tail = nil
	ll.length = 0
}
