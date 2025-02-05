package list

type List interface {
	InsertAt(pos int, val []byte) bool
	GetAt(pos int) []byte
	RemoveAt(pos int) []byte
	ForEach(consumer func([]byte) bool)
	Len() int
}
