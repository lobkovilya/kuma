package proxystate

import (
	"github.com/gogo/protobuf/proto"
	"github.com/kumahq/kuma/v2/pkg/xds/clusterstate"
)

type ClusterState interface {
	Subscribe(func(*clusterstate.State, []clusterstate.Diff))
}

type Provider struct {
	clisterState ClusterState
}

func NewProvider(cs ClusterState) *Provider {
	return &Provider{clisterState: cs}
}

func (p *Provider) Get(typeURL string, proxyLabels map[string]string) (proto.Message, error) {
	return nil, nil
}
