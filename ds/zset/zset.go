package zset

// SortedSet is a set which keys sorted by bound score
type SortedSet struct {
	dict     map[string]*Element
	skiplist *skiplist
}

// Make makes a new SortedSet
func NewSortedSet() *SortedSet {
	return &SortedSet{
		dict:     make(map[string]*Element),
		skiplist: newSkiplist(),
	}
}

// Add puts member into set,  and returns whether it has inserted new node
func (ss *SortedSet) Add(member string, score float64) bool {
	element, ok := ss.dict[member]
	ss.dict[member] = &Element{
		Member: member,
		Score:  score,
	}
	if ok {
		if score != element.Score {
			ss.skiplist.remove(member, element.Score)
			ss.skiplist.insert(member, score)
		}
		return false
	}
	ss.skiplist.insert(member, score)
	return true
}

// Len returns number of members in set
func (ss *SortedSet) Len() int64 {
	return int64(len(ss.dict))
}

// Get returns the given member
func (sortedSet *SortedSet) Get(member string) (element *Element, ok bool) {
	element, ok = sortedSet.dict[member]
	if !ok {
		return nil, false
	}
	return element, true
}

// Remove removes the given member from set
func (ss *SortedSet) Remove(member string) bool {
	v, ok := ss.dict[member]
	if ok {
		ss.skiplist.remove(member, v.Score)
		delete(ss.dict, member)
		return true
	}
	return false
}

func (ss *SortedSet) Score(member string) (float64, bool) {
	element, ok := ss.dict[member]
	if !ok {
		return 0, false
	}
	return element.Score, true
}

// GetRank returns the rank of the given member, sort by ascending order, rank starts from 0
func (ss *SortedSet) GetRank(member string, desc bool) (rank int64) {
	element, ok := ss.dict[member]
	if !ok {
		return -1
	}
	r := ss.skiplist.getRank(member, element.Score)
	if desc {
		r = ss.skiplist.length - r
	} else {
		r--
	}
	return r
}

func (ss *SortedSet) Range(start Border, stop Border, byScore bool) []*Element {
	var results []*Element

	if byScore {
		min := start.Value().(float64)
		max := stop.Value().(float64)
		if min > max {
			min, max = max, min
		}

		consumer := func(element *Element) bool {
			if min <= element.Score && element.Score <= max {
				if start.(*FloatBorder).excluded && min == element.Score {
					return true
				}
				if stop.(*FloatBorder).excluded && max == element.Score {
					return true
				}
				results = append(results, element)
			}
			return true
		}

		ss.skiplist.forEach(false, consumer)
	} else {
		min := start.Value().(int64)
		if min < 0 {
			min = min + ss.skiplist.length
			if min < 0 {
				return []*Element{}
			}
		}
		max := stop.Value().(int64)
		if max < 0 {
			max = max + ss.skiplist.length
			if max < 0 {
				return []*Element{}
			}
		}

		if min > max || min >= ss.skiplist.length {
			return []*Element{}
		}
		if max >= ss.skiplist.length {
			max = ss.skiplist.length - 1
		}

		current := ss.skiplist.header.level[0].forward
		var i int64 = 0
		for current != nil && i <= max {
			if i >= min {
				results = append(results, &current.Element)
			}
			current = current.level[0].forward
			i++
		}
	}
	return results
}
