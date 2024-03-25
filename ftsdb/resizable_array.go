package ftsdb

type ArrayOps interface {
	Insert(element interface{})
	At(index int) interface{}
}

type FastArray struct {
	size int
	arr  []interface{}
}

func NewFastArray() *FastArray {
	return &FastArray{
		arr: make([]interface{}, 0),
	}
}

func (arr *FastArray) Insert(element interface{}) {
	arr.arr = append(arr.arr, element)
	arr.size = len(arr.arr)
}

func (arr *FastArray) At(index int) interface{} {
	return arr.arr[index]
}
