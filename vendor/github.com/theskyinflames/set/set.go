package set

import (
	"errors"
	"sync"
)

var (
	ErrAlreadyExists   error = errors.New("item already exists")
	ErrKeyDoesNotExist error = errors.New("key does not exist")
)

type (
	Key func() string

	Set struct {
		sync.RWMutex
		set sync.Map
	}

	Iterator func() (item interface{}, lastItem bool)
)

func (s *Set) Add(key Key, item interface{}) (err error) {
	_, loaded := s.set.LoadOrStore(key(), item)
	if loaded {
		err = ErrAlreadyExists
	}
	return
}

func (s *Set) Iterator() (iterator Iterator) {

	items := s.getItemsSlice()

	c := 0
	iterator = func() (item interface{}, lastItem bool) {
		if c < len(items) {
			item = items[c]
			c++
		}
		lastItem = c == len(items)
		return
	}
	return
}

func (s *Set) getItemsSlice() (items []interface{}) {
	items = make([]interface{}, s.Count())
	c := 0
	catchingItems := func(key, value interface{}) bool {
		items[c] = value
		c++
		return true
	}
	s.set.Range(catchingItems)
	return
}

func (s *Set) Peek(key Key) (item interface{}, err error) {
	s.Lock()
	s.Unlock()

	item, ok := s.set.Load(key())
	if !ok {
		err = ErrKeyDoesNotExist
	}
	return
}

func (s *Set) Pop(key Key) (item interface{}, err error) {

	item, err = s.Peek(key)
	if err != nil {
		return
	}

	s.Lock()
	defer s.Unlock()

	s.set.Delete(key())
	return
}

func (s *Set) Count() (count int) {
	count = 0
	f := func(key, value interface{}) bool {
		count++
		return true
	}
	s.set.Range(f)
	return
}

func (s *Set) Clear() {
	s.set = sync.Map{}
	return
}
