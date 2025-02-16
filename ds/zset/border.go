package zset

import (
	"strconv"
	"strings"
)

type Border interface {
	greater(b Border) bool
	less(b Border) bool
	Value() any
}

type FloatBorder struct {
	value    float64
	excluded bool
}

func (f *FloatBorder) greater(b Border) bool {
	if ib, ok := b.(*IntBorder); ok {
		return f.value > float64(ib.value)
	}
	if fb, ok := b.(*FloatBorder); ok {
		return f.value > fb.value
	}
	return false
}

func (f *FloatBorder) less(b Border) bool {
	if ib, ok := b.(*IntBorder); ok {
		return f.value < float64(ib.value)
	}
	if fb, ok := b.(*FloatBorder); ok {
		return f.value < fb.value
	}
	return false
}

func (fb *FloatBorder) Value() any {
	return fb.value
}

type IntBorder struct {
	value int64
}

func (i *IntBorder) greater(b Border) bool {
	if ib, ok := b.(*IntBorder); ok {
		return i.value > ib.value
	}
	if fb, ok := b.(*FloatBorder); ok {
		return i.value > int64(fb.value)
	}
	return false
}

func (i *IntBorder) less(b Border) bool {
	if ib, ok := b.(*IntBorder); ok {
		return i.value < ib.value
	}
	if fb, ok := b.(*FloatBorder); ok {
		return i.value < int64(fb.value)
	}
	return false
}

func (ib *IntBorder) Value() any {
	return ib.value
}

// ParseFloatBorder parses float score
// if starts with (, means its excluded
func ParseFloatBorder(arg []byte) (Border, error) {
	var newBorder FloatBorder
	argStr := string(arg)

	if strings.HasPrefix(argStr, "(") {
		newBorder.excluded = true
		argStr = argStr[1:]
	}

	val, err := strconv.ParseFloat(argStr, 64)
	if err != nil {
		return &FloatBorder{}, err
	}
	newBorder.value = val
	return &newBorder, nil
}

func ParseIntBorder(arg []byte) (Border, error) {
	val, err := strconv.ParseInt(string(arg), 10, 64)
	if err != nil {
		return &IntBorder{}, err
	}
	return &IntBorder{
		value: val,
	}, nil
}
