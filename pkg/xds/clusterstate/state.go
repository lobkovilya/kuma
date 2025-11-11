package clusterstate

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kumahq/kuma/v2/pkg/core/kri"
	"github.com/kumahq/kuma/v2/pkg/core/resources/manager"
	core_model "github.com/kumahq/kuma/v2/pkg/core/resources/model"
	"github.com/kumahq/kuma/v2/pkg/core/resources/registry"
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
	if old == nil && new == nil {
		return nil
	}

	var diffs []Diff
	resourceTypes := make(map[core_model.ResourceType]bool)

	if old != nil {
		for resType := range old.Indexes {
			resourceTypes[resType] = true
		}
	}
	if new != nil {
		for resType := range new.Indexes {
			resourceTypes[resType] = true
		}
	}

	for resType := range resourceTypes {
		var oldIndex, newIndex Index
		if old != nil {
			oldIndex = old.Indexes[resType]
		}
		if new != nil {
			newIndex = new.Indexes[resType]
		}

		diff := Diff{
			Type:    resType,
			Added:   []kri.Identifier{},
			Deleted: []kri.Identifier{},
			Updated: []kri.Identifier{},
		}

		// Find added and updated
		for id, newRes := range newIndex {
			if oldRes, exists := oldIndex[id]; exists {
				// Check if updated
				if newRes.GetMeta().GetModificationTime().After(oldRes.GetMeta().GetModificationTime()) {
					diff.Updated = append(diff.Updated, id)
				}
			} else {
				// Added
				diff.Added = append(diff.Added, id)
			}
		}

		// Find deleted
		for id := range oldIndex {
			if _, exists := newIndex[id]; !exists {
				diff.Deleted = append(diff.Deleted, id)
			}
		}

		// Only add diff if there are changes
		if len(diff.Added) > 0 || len(diff.Deleted) > 0 || len(diff.Updated) > 0 {
			diffs = append(diffs, diff)
		}
	}

	return diffs
}

type Provider struct {
	ro          manager.ReadOnlyResourceManager
	state       atomic.Pointer[State]
	subscribers []func(*State, []Diff)
	mu          sync.RWMutex
	interval    time.Duration
}

func NewProvider(ro manager.ReadOnlyResourceManager) *Provider {
	return &Provider{
		ro:       ro,
		interval: 5 * time.Second,
	}
}

// Subscribe accepts a callback that will be invoked when there is diff.
// Depending on the strategy of Provider it might happen not immediately when change happened in the cluster.
// Diff is empty only on the initial invokation, all the following invokations for the given function
// are guaranteed to have non-empty diff. State can be shared across multiple gorotuines that's why it's read-only.
func (p *Provider) Subscribe(fn func(*State, []Diff)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subscribers = append(p.subscribers, fn)
}

func (p *Provider) buildState(ctx context.Context) (*State, error) {
	state := &State{
		Resources: make(map[core_model.ResourceType]core_model.ResourceList),
		Indexes:   make(map[core_model.ResourceType]Index),
	}

	for _, resType := range registry.Global().ObjectTypes() {
		desc, err := registry.Global().DescriptorFor(resType)
		if err != nil {
			continue
		}

		if !desc.IsPluginOriginated {
			continue
		}

		list := desc.NewList()
		if err := p.ro.List(ctx, list); err != nil {
			return nil, err
		}

		state.Resources[resType] = list

		index := make(Index)
		for _, item := range list.GetItems() {
			index[kri.FromResourceMeta(item.GetMeta(), resType)] = item
		}
		state.Indexes[resType] = index
	}

	return state, nil
}

func (p *Provider) Start(stop <-chan struct{}) error {
	ctx := context.Background()

	// Build initial state
	initialState, err := p.buildState(ctx)
	if err != nil {
		return err
	}
	p.state.Store(initialState)

	// Notify subscribers with empty diff (initial invocation)
	p.mu.RLock()
	subscribers := make([]func(*State, []Diff), len(p.subscribers))
	copy(subscribers, p.subscribers)
	p.mu.RUnlock()

	for _, fn := range subscribers {
		fn(initialState, nil)
	}

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return nil
		case <-ticker.C:
			newState, err := p.buildState(ctx)
			if err != nil {
				continue
			}

			oldState := p.state.Load()
			diffs := Compare(oldState, newState)

			if len(diffs) > 0 {
				p.state.Store(newState)

				p.mu.RLock()
				subscribers := make([]func(*State, []Diff), len(p.subscribers))
				copy(subscribers, p.subscribers)
				p.mu.RUnlock()

				for _, fn := range subscribers {
					fn(newState, diffs)
				}
			}
		}
	}
}

func (p *Provider) NeedLeaderElection() bool {
	return false
}
