package fbp

func NewPort(ID string, in chan *InformationPackage, out chan *InformationPackage) *Port {
	return &Port{
		ID:  ID,
		In:  in,
		Out: out,
	}
}

type Port struct {
	ID  string
	In  chan *InformationPackage
	Out chan *InformationPackage
}
