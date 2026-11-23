// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2023 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/mochi-mqtt/server/v2/packets"
)

var (
	SharePrefix = "$SHARE" // the prefix indicating a share topic
	SysPrefix   = "$SYS"   // the prefix indicating a system info topic
)

// TopicAliases contains inbound and outbound topic alias registrations.
type TopicAliases struct {
	Inbound  *InboundTopicAliases
	Outbound *OutboundTopicAliases
}

// NewTopicAliases returns an instance of TopicAliases.
func NewTopicAliases(topicAliasMaximum uint16) TopicAliases {
	return TopicAliases{
		Inbound:  NewInboundTopicAliases(topicAliasMaximum),
		Outbound: NewOutboundTopicAliases(topicAliasMaximum),
	}
}

// NewInboundTopicAliases returns a pointer to InboundTopicAliases.
func NewInboundTopicAliases(topicAliasMaximum uint16) *InboundTopicAliases {
	return &InboundTopicAliases{
		maximum:  topicAliasMaximum,
		internal: map[uint16]string{},
	}
}

// InboundTopicAliases contains a map of topic aliases received from the client.
type InboundTopicAliases struct {
	internal map[uint16]string
	sync.RWMutex
	maximum uint16
}

// Set sets a new alias for a specific topic.
func (a *InboundTopicAliases) Set(id uint16, topic string) string {
	a.Lock()
	defer a.Unlock()

	if a.maximum == 0 {
		return topic // ?
	}

	if existing, ok := a.internal[id]; ok && topic == "" {
		return existing
	}

	a.internal[id] = topic
	return topic
}

// OutboundTopicAliases contains a map of topic aliases sent from the broker to the client.
type OutboundTopicAliases struct {
	internal map[string]uint16
	sync.RWMutex
	cursor  uint32
	maximum uint16
}

// NewOutboundTopicAliases returns a pointer to OutboundTopicAliases.
func NewOutboundTopicAliases(topicAliasMaximum uint16) *OutboundTopicAliases {
	return &OutboundTopicAliases{
		maximum:  topicAliasMaximum,
		internal: map[string]uint16{},
	}
}

// Set sets a new topic alias for a topic and returns the alias value, and a boolean
// indicating if the alias already existed.
func (a *OutboundTopicAliases) Set(topic string) (uint16, bool) {
	a.Lock()
	defer a.Unlock()

	if a.maximum == 0 {
		return 0, false
	}

	if i, ok := a.internal[topic]; ok {
		return i, true
	}

	i := atomic.LoadUint32(&a.cursor)
	if i+1 > uint32(a.maximum) {
		// if i+1 > math.MaxUint16 {
		return 0, false
	}

	a.internal[topic] = uint16(i) + 1
	atomic.StoreUint32(&a.cursor, i+1)
	return uint16(i) + 1, false
}

// SharedSubscriptions contains a map of subscriptions to a shared filter,
// keyed on share group then client id.
type SharedSubscriptions struct {
	internal map[string]map[string]packets.Subscription
	sync.RWMutex
	generation uint64
}

// NewSharedSubscriptions returns a new instance of Subscriptions.
func NewSharedSubscriptions() *SharedSubscriptions {
	return &SharedSubscriptions{
		internal: map[string]map[string]packets.Subscription{},
	}
}

// Add creates a new shared subscription for a group and client id pair.
func (s *SharedSubscriptions) Add(group, id string, val packets.Subscription) {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.internal[group]; !ok {
		s.internal[group] = map[string]packets.Subscription{}
	}
	if existing, ok := s.internal[group][id]; !ok || !subscriptionEqual(existing, val) {
		s.generation++
	}
	s.internal[group][id] = val
}

// Delete deletes a client id from a shared subscription group.
func (s *SharedSubscriptions) Delete(group, id string) {
	s.Lock()
	defer s.Unlock()
	members, ok := s.internal[group]
	if !ok {
		return
	}

	if _, ok := members[id]; ok {
		delete(members, id)
		s.generation++
	}

	if len(members) == 0 {
		delete(s.internal, group)
	}
}

// Get returns the subscription properties for a client id in a share group, if one exists.
func (s *SharedSubscriptions) Get(group, id string) (val packets.Subscription, ok bool) {
	s.RLock()
	defer s.RUnlock()
	if _, ok := s.internal[group]; !ok {
		return val, ok
	}

	val, ok = s.internal[group][id]
	return val, ok
}

// GroupLen returns the number of groups subscribed to the filter.
func (s *SharedSubscriptions) GroupLen() int {
	s.RLock()
	defer s.RUnlock()
	val := len(s.internal)
	return val
}

// Len returns the total number of shared subscriptions to a filter across all groups.
func (s *SharedSubscriptions) Len() int {
	s.RLock()
	defer s.RUnlock()
	n := 0
	for _, group := range s.internal {
		n += len(group)
	}
	return n
}

// Generation returns the current membership generation of the shared subscriptions.
// It changes whenever a member is added, removed, or has its subscription changed.
func (s *SharedSubscriptions) Generation() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.generation
}

// Snapshot returns a copy of the shared subscriptions and their current generation.
func (s *SharedSubscriptions) Snapshot() (map[string]map[string]packets.Subscription, uint64) {
	s.RLock()
	defer s.RUnlock()
	m := map[string]map[string]packets.Subscription{}
	for group, subs := range s.internal {
		if _, ok := m[group]; !ok {
			m[group] = map[string]packets.Subscription{}
		}

		for id, sub := range subs {
			m[group][id] = sub
		}
	}
	return m, s.generation
}

// GetAll returns all shared subscription groups and their subscriptions.
func (s *SharedSubscriptions) GetAll() map[string]map[string]packets.Subscription {
	m, _ := s.Snapshot()
	return m
}

// subscriptionEqual reports whether two subscriptions describe the same membership.
func subscriptionEqual(a, b packets.Subscription) bool {
	if a.Filter != b.Filter ||
		a.Identifier != b.Identifier ||
		a.RetainHandling != b.RetainHandling ||
		a.Qos != b.Qos ||
		a.RetainAsPublished != b.RetainAsPublished ||
		a.NoLocal != b.NoLocal ||
		a.FwdRetainedFlag != b.FwdRetainedFlag ||
		len(a.ShareName) != len(b.ShareName) ||
		len(a.Identifiers) != len(b.Identifiers) {
		return false
	}

	for i, v := range a.ShareName {
		if b.ShareName[i] != v {
			return false
		}
	}

	for k, v := range a.Identifiers {
		if b.Identifiers[k] != v {
			return false
		}
	}

	return true
}

// InlineSubFn is the signature for a callback function which will be called
// when an inline client receives a message on a topic it is subscribed to.
// The sub argument contains information about the subscription that was matched for any filters.
type InlineSubFn func(cl *Client, sub packets.Subscription, pk packets.Packet)

// InlineSubscriptions represents a map of internal subscriptions keyed on client.
type InlineSubscriptions struct {
	internal map[int]InlineSubscription
	sync.RWMutex
}

// NewInlineSubscriptions returns a new instance of InlineSubscriptions.
func NewInlineSubscriptions() *InlineSubscriptions {
	return &InlineSubscriptions{
		internal: map[int]InlineSubscription{},
	}
}

// Add adds a new internal subscription for a client id.
func (s *InlineSubscriptions) Add(val InlineSubscription) {
	s.Lock()
	defer s.Unlock()
	s.internal[val.Identifier] = val
}

// GetAll returns all internal subscriptions.
func (s *InlineSubscriptions) GetAll() map[int]InlineSubscription {
	s.RLock()
	defer s.RUnlock()
	m := map[int]InlineSubscription{}
	for k, v := range s.internal {
		m[k] = v
	}
	return m
}

// Get returns an internal subscription for a client id.
func (s *InlineSubscriptions) Get(id int) (val InlineSubscription, ok bool) {
	s.RLock()
	defer s.RUnlock()
	val, ok = s.internal[id]
	return val, ok
}

// Len returns the number of internal subscriptions.
func (s *InlineSubscriptions) Len() int {
	s.RLock()
	defer s.RUnlock()
	val := len(s.internal)
	return val
}

// Delete removes an internal subscription by the client id.
func (s *InlineSubscriptions) Delete(id int) {
	s.Lock()
	defer s.Unlock()
	delete(s.internal, id)
}

// Subscriptions is a map of subscriptions keyed on client.
type Subscriptions struct {
	internal map[string]packets.Subscription
	sync.RWMutex
}

// NewSubscriptions returns a new instance of Subscriptions.
func NewSubscriptions() *Subscriptions {
	return &Subscriptions{
		internal: map[string]packets.Subscription{},
	}
}

// Add adds a new subscription for a client. ID can be a filter in the
// case this map is client state, or a client id if particle state.
func (s *Subscriptions) Add(id string, val packets.Subscription) {
	s.Lock()
	defer s.Unlock()
	s.internal[id] = val
}

// GetAll returns all subscriptions.
func (s *Subscriptions) GetAll() map[string]packets.Subscription {
	s.RLock()
	defer s.RUnlock()
	m := map[string]packets.Subscription{}
	for k, v := range s.internal {
		m[k] = v
	}
	return m
}

// Get returns a subscriptions for a specific client or filter id.
func (s *Subscriptions) Get(id string) (val packets.Subscription, ok bool) {
	s.RLock()
	defer s.RUnlock()
	val, ok = s.internal[id]
	return val, ok
}

// Len returns the number of subscriptions.
func (s *Subscriptions) Len() int {
	s.RLock()
	defer s.RUnlock()
	val := len(s.internal)
	return val
}

// Delete removes a subscription by client or filter id.
func (s *Subscriptions) Delete(id string) {
	s.Lock()
	defer s.Unlock()
	delete(s.internal, id)
}

// ClientSubscriptions is a map of aggregated subscriptions for a client.
type ClientSubscriptions map[string]packets.Subscription

type InlineSubscription struct {
	packets.Subscription
	Handler InlineSubFn
}

// SharedSelection is the immutable result of selecting one member of a shared
// subscription group for a publish. All of the values which decide how the
// message is delivered are bound together here: the group and member which
// were selected, the filter which selected them, the member's own no-local and
// qos values, and the generations which produced the selection.
type SharedSelection struct {
	Group                  string
	Client                 string // the selected group member
	Filter                 string // the member's full shared filter, including $share/<group>/
	Subscription           packets.Subscription
	NoLocal                bool
	Qos                    byte
	Generation             uint64 // selection generation; changes only when the selected member changes
	SubscriptionGeneration uint64 // shared subscription membership generation at selection time
}

// sharedCandidate is one member of a shared subscription group which matched a publish.
type sharedCandidate struct {
	group      string
	filter     string
	client     string
	sub        packets.Subscription
	generation uint64
}

// sharedSelectionCache keeps the last selected member of each shared group.
// Keeping the selection outside the per-publish Subscribers snapshot is what
// stops a later publish from replacing the identity selected for an earlier
// one: the same member is re-used until it is no longer present or no longer
// eligible. The generation is only advanced when that identity actually changes.
type sharedSelectionCache struct {
	sync.Mutex
	serial  uint64
	entries map[string]SharedSelection // group + "\x00" + filter
}

func newSharedSelectionCache() *sharedSelectionCache {
	return &sharedSelectionCache{
		entries: map[string]SharedSelection{},
	}
}

func sharedSelectionKey(group, filter string) string {
	return group + "\x00" + filter
}

// sharedSelectedClient extracts a client id from a SharedSelected key. Keys
// are normally the client id, but a group-qualified key is used when the same
// client is selected by more than one group.
func sharedSelectedClient(key string) string {
	if i := strings.IndexByte(key, 0); i >= 0 {
		return key[i+1:]
	}
	return key
}

// bindSharedSubscription attaches the member's subscription identifier to the
// filter it was selected by. The identifiers map then travels with the selected
// candidate, so another group's filter or identifier cannot replace it.
func bindSharedSubscription(sub packets.Subscription) packets.Subscription {
	if sub.Identifiers == nil {
		sub.Identifiers = map[string]int{sub.Filter: sub.Identifier}
		return sub
	}

	if sub.Identifier > 0 {
		identifiers := make(map[string]int, len(sub.Identifiers)+1)
		for filter, id := range sub.Identifiers {
			identifiers[filter] = id
		}
		identifiers[sub.Filter] = sub.Identifier
		sub.Identifiers = identifiers
	}

	return sub
}

// Subscribers contains the shared and non-shared subscribers matching a topic.
type Subscribers struct {
	Shared              map[string]map[string]packets.Subscription
	SharedSelected      map[string]packets.Subscription
	SharedSelections    []SharedSelection
	Subscriptions       map[string]packets.Subscription
	InlineSubscriptions map[int]InlineSubscription

	sharedGenerations map[string]uint64 // full filter -> membership generation
	selectionCache    *sharedSelectionCache
}

// SelectShared returns one subscriber for each shared subscription group.
func (s *Subscribers) SelectShared() {
	s.SelectSharedWith(nil)
}

// SelectSharedWith returns one subscriber for each shared subscription group,
// skipping candidates which are not eligible. A group is identified by its
// share name: several matching filters belonging to the same group still result
// in one selected member, and the selected member's own filter, qos and
// no-local values are preserved rather than merged with another candidate.
func (s *Subscribers) SelectSharedWith(eligible func(client string, sub packets.Subscription) bool) {
	s.SharedSelections = s.selectShared(eligible)

	s.SharedSelected = map[string]packets.Subscription{}
	for _, selected := range s.SharedSelections {
		key := selected.Client
		if _, ok := s.SharedSelected[key]; ok {
			// More than one group selected this member. Keep both selections
			// instead of letting the second one replace the first.
			key = sharedSelectionKey(selected.Group, selected.Client)
		}

		cls, ok := s.SharedSelected[key]
		if !ok {
			cls = selected.Subscription
		}

		s.SharedSelected[key] = cls.Merge(selected.Subscription)
	}
}

// PrepareSharedSelections finalises shared subscription selection for a
// publish. A hook may have supplied explicit selections in SharedSelected; in
// that case they are adopted as-is. Otherwise one member is selected for every
// shared group.
func (s *Subscribers) PrepareSharedSelections(eligible func(client string, sub packets.Subscription) bool) {
	if len(s.SharedSelections) > 0 {
		return
	}

	if len(s.SharedSelected) > 0 {
		s.SharedSelections = make([]SharedSelection, 0, len(s.SharedSelected))
		for key, sub := range s.SharedSelected {
			client := sharedSelectedClient(key)
			group := SharedGroupName(sub.Filter)
			if group == "" {
				group = sub.Filter
			}

			s.SharedSelections = append(s.SharedSelections, SharedSelection{
				Group:                  group,
				Client:                 client,
				Filter:                 sub.Filter,
				Subscription:           bindSharedSubscription(sub),
				NoLocal:                sub.NoLocal,
				Qos:                    sub.Qos,
				SubscriptionGeneration: s.sharedGenerations[sub.Filter],
			})
		}

		sort.Slice(s.SharedSelections, func(i, j int) bool {
			if s.SharedSelections[i].Group != s.SharedSelections[j].Group {
				return s.SharedSelections[i].Group < s.SharedSelections[j].Group
			}
			if s.SharedSelections[i].Filter != s.SharedSelections[j].Filter {
				return s.SharedSelections[i].Filter < s.SharedSelections[j].Filter
			}
			return s.SharedSelections[i].Client < s.SharedSelections[j].Client
		})
		return
	}

	s.SelectSharedWith(eligible)
}

// selectShared chooses one member per share group. Candidates are ordered by
// filter and client so selection is deterministic; a previous selection is
// retained while that member is still present and eligible.
func (s *Subscribers) selectShared(eligible func(client string, sub packets.Subscription) bool) []SharedSelection {
	if len(s.Shared) == 0 {
		return nil
	}

	groups := map[string][]sharedCandidate{}
	for filter, members := range s.Shared {
		group := SharedGroupName(filter)
		if group == "" {
			group = filter
		}

		for client, sub := range members {
			groups[group] = append(groups[group], sharedCandidate{
				group:      group,
				filter:     filter,
				client:     client,
				sub:        sub,
				generation: s.sharedGenerations[filter],
			})
		}
	}

	names := make([]string, 0, len(groups))
	for group := range groups {
		names = append(names, group)
	}
	sort.Strings(names)

	selected := make([]SharedSelection, 0, len(names))
	for _, group := range names {
		candidates := groups[group]
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].filter != candidates[j].filter {
				return candidates[i].filter < candidates[j].filter
			}
			return candidates[i].client < candidates[j].client
		})

		var generation uint64
		for _, candidate := range candidates {
			if candidate.generation > generation {
				generation = candidate.generation
			}
		}

		candidate, ok := s.cachedSharedSelection(group, candidates, eligible)
		if !ok {
			for _, c := range candidates {
				if eligible != nil && !eligible(c.client, c.sub) {
					continue
				}
				candidate = c
				ok = true
				break
			}
		}
		if !ok {
			continue
		}

		sel := SharedSelection{
			Group:                  candidate.group,
			Client:                 candidate.client,
			Filter:                 candidate.filter,
			Subscription:           bindSharedSubscription(candidate.sub),
			NoLocal:                candidate.sub.NoLocal,
			Qos:                    candidate.sub.Qos,
			SubscriptionGeneration: generation,
		}
		sel.Generation = s.rememberSharedSelection(sel)
		selected = append(selected, sel)
	}

	return selected
}

// cachedSharedSelection returns the most recent selection for a group which is
// still a live, eligible candidate.
func (s *Subscribers) cachedSharedSelection(group string, candidates []sharedCandidate, eligible func(client string, sub packets.Subscription) bool) (sharedCandidate, bool) {
	if s.selectionCache == nil {
		return sharedCandidate{}, false
	}

	s.selectionCache.Lock()
	entries := make([]SharedSelection, 0, len(s.selectionCache.entries))
	for _, entry := range s.selectionCache.entries {
		if entry.Group == group {
			entries = append(entries, entry)
		}
	}
	s.selectionCache.Unlock()

	var best SharedSelection
	var found bool
	for _, candidate := range candidates {
		for _, entry := range entries {
			if entry.Client != candidate.client || entry.Filter != candidate.filter {
				continue
			}
			if found && entry.Generation <= best.Generation {
				continue
			}
			if eligible != nil && !eligible(candidate.client, candidate.sub) {
				continue
			}
			best = entry
			found = true
		}
	}

	if !found {
		return sharedCandidate{}, false
	}

	for _, candidate := range candidates {
		if candidate.client == best.Client && candidate.filter == best.Filter {
			return candidate, true
		}
	}

	return sharedCandidate{}, false
}

// rememberSharedSelection records a selected identity in the cache and returns
// its selection generation. Re-selecting the same member keeps the generation;
// a different member gets a new one.
func (s *Subscribers) rememberSharedSelection(sel SharedSelection) uint64 {
	if s.selectionCache == nil {
		return 0
	}

	s.selectionCache.Lock()
	defer s.selectionCache.Unlock()

	key := sharedSelectionKey(sel.Group, sel.Filter)
	var latest uint64
	for _, entry := range s.selectionCache.entries {
		if entry.Group == sel.Group && entry.Generation > latest {
			latest = entry.Generation
		}
	}

	if existing, ok := s.selectionCache.entries[key]; ok &&
		existing.Client == sel.Client &&
		existing.Filter == sel.Filter &&
		existing.Generation == latest &&
		subscriptionEqual(existing.Subscription, sel.Subscription) {
		sel.Generation = existing.Generation
	} else {
		s.selectionCache.serial++
		sel.Generation = s.selectionCache.serial
	}

	s.selectionCache.entries[key] = sel
	return sel.Generation
}

// MergeSharedSelected merges the selected subscribers for a shared subscription group
// and the non-shared subscribers, to ensure that no subscriber gets multiple messages
// due to have both types of subscription matching the same filter.
func (s *Subscribers) MergeSharedSelected() {
	for key, sub := range s.SharedSelected {
		client := sharedSelectedClient(key)
		cls, ok := s.Subscriptions[client]
		if !ok {
			cls = sub
		}

		s.Subscriptions[client] = cls.Merge(sub)
	}
}

// TopicsIndex is a prefix/trie tree containing topic subscribers and retained messages.
type TopicsIndex struct {
	Retained        *packets.Packets
	root            *particle // a leaf containing a message and more leaves.
	sharedSelection *sharedSelectionCache
}

// NewTopicsIndex returns a pointer to a new instance of Index.
func NewTopicsIndex() *TopicsIndex {
	return &TopicsIndex{
		Retained: packets.NewPackets(),
		root: &particle{
			particles:     newParticles(),
			subscriptions: NewSubscriptions(),
		},
		sharedSelection: newSharedSelectionCache(),
	}
}

// InlineSubscribe adds a new internal subscription for a topic filter, returning
// true if the subscription was new.
func (x *TopicsIndex) InlineSubscribe(subscription InlineSubscription) bool {
	x.root.Lock()
	defer x.root.Unlock()

	var existed bool
	n := x.set(subscription.Filter, 0)
	_, existed = n.inlineSubscriptions.Get(subscription.Identifier)
	n.inlineSubscriptions.Add(subscription)

	return !existed
}

// InlineUnsubscribe removes an internal subscription for a topic filter associated with a specific client,
// returning true if the subscription existed.
func (x *TopicsIndex) InlineUnsubscribe(id int, filter string) bool {
	x.root.Lock()
	defer x.root.Unlock()

	particle := x.seek(filter, 0)
	if particle == nil {
		return false
	}

	particle.inlineSubscriptions.Delete(id)

	if particle.inlineSubscriptions.Len() == 0 {
		x.trim(particle)
	}
	return true
}

// Subscribe adds a new subscription for a client to a topic filter, returning
// true if the subscription was new.
func (x *TopicsIndex) Subscribe(client string, subscription packets.Subscription) bool {
	x.root.Lock()
	defer x.root.Unlock()

	var existed bool
	prefix, _ := isolateParticle(subscription.Filter, 0)
	if strings.EqualFold(prefix, SharePrefix) {
		group, _ := isolateParticle(subscription.Filter, 1)
		n := x.set(subscription.Filter, 2)
		_, existed = n.shared.Get(group, client)
		n.shared.Add(group, client, subscription)
	} else {
		n := x.set(subscription.Filter, 0)
		_, existed = n.subscriptions.Get(client)
		n.subscriptions.Add(client, subscription)
	}

	return !existed
}

// Unsubscribe removes a subscription filter for a client, returning true if the
// subscription existed.
func (x *TopicsIndex) Unsubscribe(filter, client string) bool {
	x.root.Lock()
	defer x.root.Unlock()

	var d int
	prefix, _ := isolateParticle(filter, 0)
	shareSub := strings.EqualFold(prefix, SharePrefix)
	if shareSub {
		d = 2
	}

	particle := x.seek(filter, d)
	if particle == nil {
		return false
	}

	if shareSub {
		group, _ := isolateParticle(filter, 1)
		particle.shared.Delete(group, client)
	} else {
		particle.subscriptions.Delete(client)
	}

	x.trim(particle)
	return true
}

// RetainMessage saves a message payload to the end of a topic address. Returns
// 1 if a retained message was added, and -1 if the retained message was removed.
// 0 is returned if sequential empty payloads are received.
func (x *TopicsIndex) RetainMessage(pk packets.Packet) int64 {
	x.root.Lock()
	defer x.root.Unlock()

	n := x.set(pk.TopicName, 0)
	n.Lock()
	defer n.Unlock()
	if len(pk.Payload) > 0 {
		n.retainPath = pk.TopicName
		x.Retained.Add(pk.TopicName, pk)
		return 1
	}

	var out int64
	if pke, ok := x.Retained.Get(pk.TopicName); ok && len(pke.Payload) > 0 && pke.FixedHeader.Retain {
		out = -1 // if a retained packet existed, return -1
	}

	n.retainPath = ""
	x.Retained.Delete(pk.TopicName) // [MQTT-3.3.1-6] [MQTT-3.3.1-7]
	x.trim(n)

	return out
}

// set creates a topic address in the index and returns the final particle.
func (x *TopicsIndex) set(topic string, d int) *particle {
	var key string
	var hasNext = true
	n := x.root
	for hasNext {
		key, hasNext = isolateParticle(topic, d)
		d++

		p := n.particles.get(key)
		if p == nil {
			p = newParticle(key, n)
			n.particles.add(p)
		}
		n = p
	}

	return n
}

// seek finds the particle at a specific index in a topic filter.
func (x *TopicsIndex) seek(filter string, d int) *particle {
	var key string
	var hasNext = true
	n := x.root
	for hasNext {
		key, hasNext = isolateParticle(filter, d)
		n = n.particles.get(key)
		d++
		if n == nil {
			return nil
		}
	}

	return n
}

// trim removes empty filter particles from the index.
func (x *TopicsIndex) trim(n *particle) {
	for n.parent != nil && n.retainPath == "" && n.particles.len()+n.subscriptions.Len()+n.shared.Len()+n.inlineSubscriptions.Len() == 0 {
		key := n.key
		n = n.parent
		n.particles.delete(key)
	}
}

// Messages returns a slice of any retained messages which match a filter.
func (x *TopicsIndex) Messages(filter string) []packets.Packet {
	return x.scanMessages(filter, 0, nil, []packets.Packet{})
}

// scanMessages returns all retained messages on topics matching a given filter.
func (x *TopicsIndex) scanMessages(filter string, d int, n *particle, pks []packets.Packet) []packets.Packet {
	if n == nil {
		n = x.root
	}

	if len(filter) == 0 || x.Retained.Len() == 0 {
		return pks
	}

	if !strings.ContainsRune(filter, '#') && !strings.ContainsRune(filter, '+') {
		if pk, ok := x.Retained.Get(filter); ok {
			pks = append(pks, pk)
		}
		return pks
	}

	key, hasNext := isolateParticle(filter, d)
	if key == "+" || key == "#" || d == -1 {
		for _, adjacent := range n.particles.getAll() {
			if d == 0 && adjacent.key == SysPrefix {
				continue
			}

			if !hasNext {
				if adjacent.retainPath != "" {
					if pk, ok := x.Retained.Get(adjacent.retainPath); ok {
						pks = append(pks, pk)
					}
				}
			}

			if hasNext || (d >= 0 && key == "#") {
				pks = x.scanMessages(filter, d+1, adjacent, pks)
			}
		}
		return pks
	}

	if particle := n.particles.get(key); particle != nil {
		if hasNext {
			return x.scanMessages(filter, d+1, particle, pks)
		}

		if pk, ok := x.Retained.Get(particle.retainPath); ok {
			pks = append(pks, pk)
		}
	}

	return pks
}

// Subscribers returns a map of clients who are subscribed to matching filters,
// their subscription ids and highest qos.
func (x *TopicsIndex) Subscribers(topic string) *Subscribers {
	return x.scanSubscribers(topic, 0, nil, &Subscribers{
		Shared:              map[string]map[string]packets.Subscription{},
		SharedSelected:      map[string]packets.Subscription{},
		SharedSelections:    []SharedSelection{},
		Subscriptions:       map[string]packets.Subscription{},
		InlineSubscriptions: map[int]InlineSubscription{},
		sharedGenerations:   map[string]uint64{},
		selectionCache:      x.sharedSelection,
	})
}

// scanSubscribers returns a list of client subscriptions matching an indexed topic address.
func (x *TopicsIndex) scanSubscribers(topic string, d int, n *particle, subs *Subscribers) *Subscribers {
	if n == nil {
		n = x.root
	}

	if len(topic) == 0 {
		return subs
	}

	key, hasNext := isolateParticle(topic, d)
	for _, partKey := range []string{key, "+"} {
		if particle := n.particles.get(partKey); particle != nil { // [MQTT-3.3.2-3]
			if hasNext {
				x.scanSubscribers(topic, d+1, particle, subs)
			} else {
				x.gatherSubscriptions(topic, particle, subs)
				x.gatherSharedSubscriptions(particle, subs)
				x.gatherInlineSubscriptions(particle, subs)

				if wild := particle.particles.get("#"); wild != nil && partKey != "+" {
					x.gatherSubscriptions(topic, wild, subs) // also match any subs where filter/# is filter as per 4.7.1.2
					x.gatherSharedSubscriptions(wild, subs)
					x.gatherInlineSubscriptions(particle, subs)
				}
			}
		}
	}

	if particle := n.particles.get("#"); particle != nil {
		x.gatherSubscriptions(topic, particle, subs)
		x.gatherSharedSubscriptions(particle, subs)
		x.gatherInlineSubscriptions(particle, subs)
	}

	return subs
}

// gatherSubscriptions collects any matching subscriptions, and gathers any identifiers or highest qos values.
func (x *TopicsIndex) gatherSubscriptions(topic string, particle *particle, subs *Subscribers) {
	if subs.Subscriptions == nil {
		subs.Subscriptions = map[string]packets.Subscription{}
	}

	for client, sub := range particle.subscriptions.GetAll() {
		if len(sub.Filter) > 0 && topic[0] == '$' && (sub.Filter[0] == '+' || sub.Filter[0] == '#') { // don't match $ topics with top level wildcards [MQTT-4.7.1-1] [MQTT-4.7.1-2]
			continue
		}

		cls, ok := subs.Subscriptions[client]
		if !ok {
			cls = sub
		}

		subs.Subscriptions[client] = cls.Merge(sub)
	}
}

// gatherSharedSubscriptions gathers all shared subscriptions for a particle.
func (x *TopicsIndex) gatherSharedSubscriptions(particle *particle, subs *Subscribers) {
	if subs.Shared == nil {
		subs.Shared = map[string]map[string]packets.Subscription{}
	}
	if subs.sharedGenerations == nil {
		subs.sharedGenerations = map[string]uint64{}
	}

	shares, generation := particle.shared.Snapshot()
	for _, members := range shares {
		for client, sub := range members {
			if _, ok := subs.Shared[sub.Filter]; !ok {
				subs.Shared[sub.Filter] = map[string]packets.Subscription{}
			}

			subs.Shared[sub.Filter][client] = sub
			if generation > subs.sharedGenerations[sub.Filter] {
				subs.sharedGenerations[sub.Filter] = generation
			}
		}
	}
}

// gatherSharedSubscriptions gathers all inline subscriptions for a particle.
func (x *TopicsIndex) gatherInlineSubscriptions(particle *particle, subs *Subscribers) {
	if subs.InlineSubscriptions == nil {
		subs.InlineSubscriptions = map[int]InlineSubscription{}
	}

	for id, inline := range particle.inlineSubscriptions.GetAll() {
		subs.InlineSubscriptions[id] = inline
	}
}

// isolateParticle extracts a particle between d / and d+1 / without allocations.
func isolateParticle(filter string, d int) (particle string, hasNext bool) {
	var next, end int
	for i := 0; end > -1 && i <= d; i++ {
		end = strings.IndexRune(filter, '/')

		switch {
		case d > -1 && i == d && end > -1:
			hasNext = true
			particle = filter[next:end]
		case end > -1:
			hasNext = false
			filter = filter[end+1:]
		default:
			hasNext = false
			particle = filter[next:]
		}
	}

	return
}

// IsSharedFilter returns true if the filter uses the share prefix.
func IsSharedFilter(filter string) bool {
	prefix, _ := isolateParticle(filter, 0)
	return strings.EqualFold(prefix, SharePrefix)
}

// SharedGroupName returns the share group of a shared filter, or an empty
// string if the filter does not use the shared subscription syntax.
func SharedGroupName(filter string) string {
	prefix, hasNext := isolateParticle(filter, 0)
	if !hasNext || !strings.EqualFold(prefix, SharePrefix) {
		return ""
	}

	group, hasNext := isolateParticle(filter, 1)
	if !hasNext {
		return ""
	}
	return group
}

// IsValidFilter returns true if the filter is valid.
func IsValidFilter(filter string, forPublish bool) bool {
	if !forPublish && len(filter) == 0 { // publishing can accept zero-length topic filter if topic alias exists, so we don't enforce for publish.
		return false // [MQTT-4.7.3-1]
	}

	if forPublish {
		if len(filter) >= len(SysPrefix) && strings.EqualFold(filter[0:len(SysPrefix)], SysPrefix) {
			// 4.7.2 Non-normative - The Server SHOULD prevent Clients from using such Topic Names [$SYS] to exchange messages with other Clients.
			return false
		}

		if strings.ContainsRune(filter, '+') || strings.ContainsRune(filter, '#') {
			return false //[MQTT-3.3.2-2]
		}
	}

	wildhash := strings.IndexRune(filter, '#')
	if wildhash >= 0 && wildhash != len(filter)-1 { // [MQTT-4.7.1-2]
		return false
	}

	prefix, hasNext := isolateParticle(filter, 0)
	if !hasNext && strings.EqualFold(prefix, SharePrefix) {
		return false // [MQTT-4.8.2-1]
	}

	if hasNext && strings.EqualFold(prefix, SharePrefix) {
		group, hasNext := isolateParticle(filter, 1)
		if !hasNext {
			return false // [MQTT-4.8.2-1]
		}

		if strings.ContainsRune(group, '+') || strings.ContainsRune(group, '#') {
			return false // [MQTT-4.8.2-2]
		}
	}

	return true
}

// particle is a child node on the tree.
type particle struct {
	key                 string               // the key of the particle
	parent              *particle            // a pointer to the parent of the particle
	particles           particles            // a map of child particles
	subscriptions       *Subscriptions       // a map of subscriptions made by clients to this ending address
	shared              *SharedSubscriptions // a map of shared subscriptions keyed on group name
	inlineSubscriptions *InlineSubscriptions // a map of inline subscriptions for this particle
	retainPath          string               // path of a retained message
	sync.Mutex                               // mutex for when making changes to the particle
}

// newParticle returns a pointer to a new instance of particle.
func newParticle(key string, parent *particle) *particle {
	return &particle{
		key:                 key,
		parent:              parent,
		particles:           newParticles(),
		subscriptions:       NewSubscriptions(),
		shared:              NewSharedSubscriptions(),
		inlineSubscriptions: NewInlineSubscriptions(),
	}
}

// particles is a concurrency safe map of particles.
type particles struct {
	internal map[string]*particle
	sync.RWMutex
}

// newParticles returns a map of particles.
func newParticles() particles {
	return particles{
		internal: map[string]*particle{},
	}
}

// add adds a new particle.
func (p *particles) add(val *particle) {
	p.Lock()
	p.internal[val.key] = val
	p.Unlock()
}

// getAll returns all particles.
func (p *particles) getAll() map[string]*particle {
	p.RLock()
	defer p.RUnlock()
	m := map[string]*particle{}
	for k, v := range p.internal {
		m[k] = v
	}
	return m
}

// get returns a particle by id (key).
func (p *particles) get(id string) *particle {
	p.RLock()
	defer p.RUnlock()
	return p.internal[id]
}

// len returns the number of particles.
func (p *particles) len() int {
	p.RLock()
	defer p.RUnlock()
	val := len(p.internal)
	return val
}

// delete removes a particle.
func (p *particles) delete(id string) {
	p.Lock()
	defer p.Unlock()
	delete(p.internal, id)
}
