package fbp

func NewPort(id string, in chan *InformationPackage, out chan *InformationPackage) *Port {
	return &Port{
		ID:  id,
		In:  in,
		Out: out,
	}
}

type Port struct {
	ID  string
	In  chan *InformationPackage
	Out chan *InformationPackage
}
