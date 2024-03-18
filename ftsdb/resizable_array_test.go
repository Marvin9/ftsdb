package ftsdb

import (
	"testing"
)

func TestResizableArray(t *testing.T) {
	arr := NewFastArray()
	n := 1000000

	for i := 0; i < n; i++ {
		arr.Insert(i)
	}

	for i := 0; i < n; i++ {
		val := arr.At(i)
		if val != i {
			t.Errorf("index %d expected %d, got %d", i, i, val)
		}
	}
}
