package fbp

import (
	"sync"

	"github.com/theskyinflames/set"
)

type KeyGetter interface {
	Key() func() string
}

func NewInformationPackage(ID string, data []KeyGetter) *InformationPackage {
	ip := &InformationPackage{
		ID: ID,
		Status: &set.Set{
			RWMutex: sync.RWMutex{},
		},
	}
	if data != nil {
		for _, item := range data {
			ip.Status.Add(item.Key(), item)
		}
	}
	return ip
}

type InformationPackage struct {
	ID     string
	Status *set.Set
}
