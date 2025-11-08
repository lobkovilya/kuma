package clusterstate

import (
	"github.com/kumahq/kuma/v2/pkg/core/kri"
	"github.com/kumahq/kuma/v2/pkg/core/resources/manager"
	core_model "github.com/kumahq/kuma/v2/pkg/core/resources/model"
)

type Index map[kri.Identifier]core_model.Resource

type State struct {
	Resources map[core_model.ResourceType]core_model.ResourceList
	Indexes   map[core_model.ResourceType]Index
}

type Diff struct {
	Type core_model.ResourceType

	Added   []kri.Identifier
	Deleted []kri.Identifier
	Updated []kri.Identifier
}

func Compare(old, new *State) []Diff {
	return nil
}

type Provider struct {
	ro manager.ReadOnlyResourceManager
}

func NewProvider(ro manager.ReadOnlyResourceManager) *Provider {
	return &Provider{
		ro: ro,
	}
}

// GetState returns pointer read-only State object. Can be called concurrently.
// Single State object can be shared across multiple goroutines.
func (p *Provider) GetState() (*State, error) {
	return nil, nil
}

// Subscribe accepts a callback that will be invoked when there is diff.
// Depending on the strategy of Provider it might happen not immediately when change happened in the cluster.
// Diff is empty only on the initial invokation, all the following invokations for the given function
// are guaranteed to have non-empty diff. State can be shared across multiple gorotuines that's why it's read-only.
func (p *Provider) Subscribe(fn func(*State, []Diff)) {
}

func (p *Provider) Start(<-chan struct{}) error {
	return nil
}

func (p *Provider) NeedLeaderElection() bool {
	return false
}
