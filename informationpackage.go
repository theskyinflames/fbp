package fbp

import (
	"sync"

	"github.com/theskyinflames/set"
)

type KeyGetter interface {
	Key() func() string
}

func NewInformationPackage(ID string, data KeyGetter) *InformationPackage {
	ip := &InformationPackage{
		ID: ID,
		Status: &set.Set{
			RWMutex: sync.RWMutex{},
		},
	}
	if data != nil {
		ip.Status.Add(data.Key(), data)
	}
	return ip
}

type InformationPackage struct {
	ID     string
	Status *set.Set
}
