package fbp

import (
	"sync"

	"github.com/theskyinflames/set"
)

func NewInformationPackage(ID string) *InformationPackage {
	return &InformationPackage{
		ID: ID,
		Status: &set.Set{
			RWMutex: sync.RWMutex{},
		},
	}
}

type InformationPackage struct {
	ID     string
	Status *set.Set
}
