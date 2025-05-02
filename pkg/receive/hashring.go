// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/thanos-io/thanos/pkg/strutil"
	"golang.org/x/exp/slices"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/pkg/errors"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

// HashringAlgorithm is the algorithm used to distribute series in the ring.
type HashringAlgorithm string

const (
	AlgorithmHashmod       HashringAlgorithm = "hashmod"
	AlgorithmKetama        HashringAlgorithm = "ketama"
	AlgorithmAlignedKetama HashringAlgorithm = "aligned_ketama"

	// SectionsPerNode is the number of sections in the ring assigned to each node
	// in the ketama hashring. A higher number yields a better series distribution,
	// but also comes with a higher memory cost.
	SectionsPerNode = 1000
)

// insufficientNodesError is returned when a hashring does not
// have enough nodes to satisfy a request for a node.
type insufficientNodesError struct {
	have uint64
	want uint64
}

// Error implements the error interface.
func (i *insufficientNodesError) Error() string {
	return fmt.Sprintf("insufficient nodes; have %d, want %d", i.have, i.want)
}

// Hashring finds the correct node to handle a given time series
// for a specified tenant.
// It returns the node and any error encountered.
type Hashring interface {
	// Get returns the first node that should handle the given tenant and time series.
	Get(tenant string, timeSeries *prompb.TimeSeries) (Endpoint, error)
	// GetN returns the nth node that should handle the given tenant and time series.
	GetN(tenant string, timeSeries *prompb.TimeSeries, n uint64) (Endpoint, error)
	// Nodes returns a sorted slice of nodes that are in this hashring. Addresses could be duplicated
	// if, for example, the same address is used for multiple tenants in the multi-hashring.
	Nodes() []Endpoint
}

// SingleNodeHashring always returns the same node.
type SingleNodeHashring string

// Get implements the Hashring interface.
func (s SingleNodeHashring) Get(tenant string, ts *prompb.TimeSeries) (Endpoint, error) {
	return s.GetN(tenant, ts, 0)
}

func (s SingleNodeHashring) Nodes() []Endpoint {
	return []Endpoint{{Address: string(s), CapNProtoAddress: string(s)}}
}

// GetN implements the Hashring interface.
func (s SingleNodeHashring) GetN(_ string, _ *prompb.TimeSeries, n uint64) (Endpoint, error) {
	if n > 0 {
		return Endpoint{}, &insufficientNodesError{have: 1, want: n + 1}
	}
	return Endpoint{
		Address:          string(s),
		CapNProtoAddress: string(s),
	}, nil
}

// simpleHashring represents a group of nodes handling write requests by hashmoding individual series.
type simpleHashring []Endpoint

func newSimpleHashring(endpoints []Endpoint) (Hashring, error) {
	for i := range endpoints {
		if endpoints[i].AZ != "" {
			return nil, errors.New("Hashmod algorithm does not support AZ aware hashring configuration. Either use Ketama or remove AZ configuration.")
		}
	}
	slices.SortFunc(endpoints, func(a, b Endpoint) int {
		return strings.Compare(a.Address, b.Address)
	})

	return simpleHashring(endpoints), nil
}

func (s simpleHashring) Nodes() []Endpoint {
	return s
}

// Get returns a target to handle the given tenant and time series.
func (s simpleHashring) Get(tenant string, ts *prompb.TimeSeries) (Endpoint, error) {
	return s.GetN(tenant, ts, 0)
}

// GetN returns the nth target to handle the given tenant and time series.
func (s simpleHashring) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (Endpoint, error) {
	if n >= uint64(len(s)) {
		return Endpoint{}, &insufficientNodesError{have: uint64(len(s)), want: n + 1}
	}

	return s[(labelpb.HashWithPrefix(tenant, ts.Labels)+n)%uint64(len(s))], nil
}

type section struct {
	az            string
	endpointIndex uint64
	hash          uint64
	replicas      []uint64
}

type sections []*section

func (p sections) Len() int           { return len(p) }
func (p sections) Less(i, j int) bool { return p[i].hash < p[j].hash }
func (p sections) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p sections) Sort()              { sort.Sort(p) }

// ketamaHashring represents a group of nodes handling write requests with consistent hashing.
type ketamaHashring struct {
	endpoints    []Endpoint
	sections     sections
	numEndpoints uint64
}

func newKetamaHashring(endpoints []Endpoint, sectionsPerNode int, replicationFactor uint64) (*ketamaHashring, error) {
	numSections := len(endpoints) * sectionsPerNode

	if len(endpoints) < int(replicationFactor) {
		return nil, errors.New("ketama: amount of endpoints needs to be larger than replication factor")

	}
	hash := xxhash.New()
	availabilityZones := make(map[string]struct{})
	ringSections := make(sections, 0, numSections)

	for endpointIndex, endpoint := range endpoints {
		availabilityZones[endpoint.AZ] = struct{}{}
		for i := 1; i <= sectionsPerNode; i++ {
			_, _ = hash.Write([]byte(endpoint.Address + ":" + strconv.Itoa(i)))
			n := &section{
				az:            endpoint.AZ,
				endpointIndex: uint64(endpointIndex),
				hash:          hash.Sum64(),
				replicas:      make([]uint64, 0, replicationFactor),
			}

			ringSections = append(ringSections, n)
			hash.Reset()
		}
	}
	sort.Sort(ringSections)
	calculateSectionReplicas(ringSections, replicationFactor, availabilityZones)

	return &ketamaHashring{
		endpoints:    endpoints,
		sections:     ringSections,
		numEndpoints: uint64(len(endpoints)),
	}, nil
}

// groupByAZ groups endpoints by Availability Zone and sorts them by their inferred ordinal.
// It returns a 2D slice where each inner slice represents an AZ (sorted alphabetically)
// and contains endpoints sorted by ordinal. All inner slices are truncated to the
// length of the largest common sequence of ordinals starting from 0 across all AZs.
func groupByAZ(endpoints []Endpoint) ([][]Endpoint, error) {
	if len(endpoints) == 0 {
		return nil, errors.New("no endpoints provided")
	}

	// Group endpoints by AZ and then by ordinal.
	azEndpoints := make(map[string]map[int]Endpoint)
	for _, ep := range endpoints {
		ordinal, err := strutil.ExtractPodOrdinal(ep.Address)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to extract ordinal from address %s", ep.Address)
		}

		if _, ok := azEndpoints[ep.AZ]; !ok {
			azEndpoints[ep.AZ] = make(map[int]Endpoint)
		}

		if _, exists := azEndpoints[ep.AZ][ordinal]; exists {
			return nil, fmt.Errorf("duplicate endpoint ordinal %d for address %s in AZ %s", ordinal, ep.Address, ep.AZ)
		}
		azEndpoints[ep.AZ][ordinal] = ep
	}

	// Get sorted list of AZ names.
	sortedAZs := make([]string, 0, len(azEndpoints))
	for az := range azEndpoints {
		sortedAZs = append(sortedAZs, az)
	}
	sort.Strings(sortedAZs)

	// Determine the maximum common ordinal across all AZs.
	maxCommonOrdinal := -1
	for i := 0; ; i++ {
		presentInAllAZs := true
		for _, az := range sortedAZs {
			if _, ok := azEndpoints[az][i]; !ok {
				presentInAllAZs = false
				// If even ordinal 0 is missing in any AZ, it's an invalid configuration for balancing.
				if i == 0 {
					return nil, fmt.Errorf("AZ %q is missing endpoint with ordinal 0", az)
				}
				break // Stop checking this ordinal once one AZ is missing it.
			}
		}

		if !presentInAllAZs {
			maxCommonOrdinal = i - 1
			break
		}
	}

	// If maxCommonOrdinal is still -1, it means not even ordinal 0 was common.
	if maxCommonOrdinal < 0 {
		// This case should be caught by the i==0 check inside the loop,
		// but added for robustness in case the logic changes.
		return nil, errors.New("no common endpoints with ordinal 0 found across all AZs")
	}

	// Build the final result, truncated to the maxCommonOrdinal.
	numAZs := len(sortedAZs)
	result := make([][]Endpoint, numAZs)
	for i, az := range sortedAZs {
		// Pre-allocate slice capacity for efficiency.
		result[i] = make([]Endpoint, 0, maxCommonOrdinal+1)
		for j := 0; j <= maxCommonOrdinal; j++ {
			// We know the endpoint exists due to the previous check.
			result[i] = append(result[i], azEndpoints[az][j])
		}
	}

	return result, nil
}

// newAlignedKetamaHashring creates a Ketama hash ring where replicas are strictly aligned across Availability Zones.
//
// It expects endpoints to be named following a pattern allowing ordinal extraction (e.g., pod-0, pod-1).
// It first groups endpoints by AZ using groupByAZ, which ensures:
//  1. AZs are sorted alphabetically.
//  2. Endpoints within each AZ group are sorted by ordinal.
//  3. All AZ groups are truncated to the same length (largest common sequence of ordinals starting from 0).
//
// The function requires the number of distinct AZs found to be exactly equal to the replicationFactor.
// Each section on the hash ring corresponds to a primary endpoint (taken from the first AZ) and its
// aligned replicas in other AZs (endpoints with the same ordinal). The hash for a section is calculated
// based *only* on the primary endpoint's address.
//
// Parameters:
//   - endpoints: A slice of all available endpoints.
//   - sectionsPerNode: The number of sections (points) to add to the ring for each primary endpoint and its replicas.
//   - replicationFactor: The desired number of replicas for each piece of data; must match the number of AZs.
//
// Returns:
//   - A pointer to the initialized ketamaHashring.
//   - An error if constraints are not met (e.g., AZ count != replicationFactor, missing ordinals, non-aligned ordinals).
func newAlignedKetamaHashring(endpoints []Endpoint, sectionsPerNode int, replicationFactor uint64) (*ketamaHashring, error) {
	if replicationFactor == 0 {
		return nil, errors.New("replication factor cannot be zero")
	}
	if sectionsPerNode <= 0 {
		return nil, errors.New("sections per node must be positive")
	}

	// Group by AZ, sort AZs, sort endpoints by ordinal within AZs, and ensure common length.
	groupedEndpoints, err := groupByAZ(endpoints)
	if err != nil {
		return nil, errors.Wrap(err, "failed to group endpoints by AZ")
	}

	numAZs := len(groupedEndpoints)
	if numAZs == 0 {
		// Should be caught by groupByAZ, but double-check.
		return nil, errors.New("no endpoint groups found after grouping by AZ")
	}
	if uint64(numAZs) != replicationFactor {
		return nil, fmt.Errorf("number of AZs (%d) must equal replication factor (%d)", numAZs, replicationFactor)
	}

	// groupedEndpoints[0] is safe because groupByAZ guarantees at least ordinal 0 exists if no error.
	numEndpointsPerAZ := len(groupedEndpoints[0])
	if numEndpointsPerAZ == 0 {
		// Should be caught by groupByAZ, but double-check.
		return nil, errors.New("AZ groups are empty after grouping")
	}

	// Create a flat list of endpoints, ordered AZ by AZ. This order is important for replica index calculation.
	totalEndpoints := numAZs * numEndpointsPerAZ
	flatEndpoints := make([]Endpoint, 0, totalEndpoints)
	for azIndex := 0; azIndex < numAZs; azIndex++ {
		flatEndpoints = append(flatEndpoints, groupedEndpoints[azIndex]...)
	}

	hasher := xxhash.New()
	ringSections := make(sections, 0, numEndpointsPerAZ*sectionsPerNode) // Correct capacity.

	// Iterate through primary endpoints (those in the first AZ) to define sections.
	for primaryOrdinalIndex := 0; primaryOrdinalIndex < numEndpointsPerAZ; primaryOrdinalIndex++ {
		primaryEndpoint := groupedEndpoints[0][primaryOrdinalIndex]
		primaryOrdinal, err := strutil.ExtractPodOrdinal(primaryEndpoint.Address) // Get ordinal once for comparison.
		if err != nil {
			// Should not happen if groupByAZ worked, but check defensively.
			return nil, errors.Wrapf(err, "failed to extract ordinal from primary endpoint %s", primaryEndpoint.Address)
		}

		// Create multiple sections per primary node for better distribution.
		for sectionIndex := 1; sectionIndex <= sectionsPerNode; sectionIndex++ {
			hasher.Reset()
			// Hash is based *only* on the primary endpoint address and section index.
			_, _ = hasher.Write([]byte(primaryEndpoint.Address + ":" + strconv.Itoa(sectionIndex)))
			sectionHash := hasher.Sum64()

			sec := &section{
				hash:          sectionHash,
				az:            primaryEndpoint.AZ,          // AZ of the primary.
				endpointIndex: uint64(primaryOrdinalIndex), // Index within the AZ.
				replicas:      make([]uint64, 0, replicationFactor),
			}

			// Find indices of all replicas (including primary) in the flat list and verify alignment.
			for azIndex := 0; azIndex < numAZs; azIndex++ {
				// Calculate index in the flatEndpoints slice.
				replicaFlatIndex := azIndex*numEndpointsPerAZ + primaryOrdinalIndex
				replicaEndpoint := flatEndpoints[replicaFlatIndex]

				// Verify that the replica in this AZ has the same ordinal as the primary.
				replicaOrdinal, err := strutil.ExtractPodOrdinal(replicaEndpoint.Address)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to extract ordinal from replica endpoint %s in AZ %s", replicaEndpoint.Address, replicaEndpoint.AZ)
				}
				if replicaOrdinal != primaryOrdinal {
					return nil, fmt.Errorf("ordinal mismatch for primary endpoint %s (ordinal %d): replica %s in AZ %s has ordinal %d",
						primaryEndpoint.Address, primaryOrdinal, replicaEndpoint.Address, replicaEndpoint.AZ, replicaOrdinal)
				}

				sec.replicas = append(sec.replicas, uint64(replicaFlatIndex))
			}
			ringSections = append(ringSections, sec)
		}
	}

	// Sort sections by hash value for ring lookup.
	sort.Sort(ringSections)

	return &ketamaHashring{
		endpoints:    flatEndpoints,
		sections:     ringSections,
		numEndpoints: uint64(totalEndpoints),
	}, nil
}

func (k *ketamaHashring) Nodes() []Endpoint {
	return k.endpoints
}

func sizeOfLeastOccupiedAZ(azSpread map[string]int64) int64 {
	minValue := int64(math.MaxInt64)
	for _, value := range azSpread {
		if value < minValue {
			minValue = value
		}
	}
	return minValue
}

// calculateSectionReplicas pre-calculates replicas for each section,
// ensuring that replicas for each ring section are owned by different endpoints.
func calculateSectionReplicas(ringSections sections, replicationFactor uint64, availabilityZones map[string]struct{}) {
	for i, s := range ringSections {
		replicas := make(map[uint64]struct{})
		azSpread := make(map[string]int64)
		for az := range availabilityZones {
			// This is to make sure each az is initially represented
			azSpread[az] = 0
		}
		j := i - 1
		for uint64(len(replicas)) < replicationFactor {
			j = (j + 1) % len(ringSections)
			rep := ringSections[j]
			if _, ok := replicas[rep.endpointIndex]; ok {
				continue
			}
			if len(azSpread) > 1 && azSpread[rep.az] > 0 && azSpread[rep.az] > sizeOfLeastOccupiedAZ(azSpread) {
				// We want to ensure even AZ spread before we add more replicas within the same AZ
				continue
			}
			replicas[rep.endpointIndex] = struct{}{}
			azSpread[rep.az]++
			s.replicas = append(s.replicas, rep.endpointIndex)
		}
	}
}

func (c ketamaHashring) Get(tenant string, ts *prompb.TimeSeries) (Endpoint, error) {
	return c.GetN(tenant, ts, 0)
}

func (c ketamaHashring) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (Endpoint, error) {
	if n >= c.numEndpoints {
		return Endpoint{}, &insufficientNodesError{have: c.numEndpoints, want: n + 1}
	}

	v := labelpb.HashWithPrefix(tenant, ts.Labels)

	var i uint64
	i = uint64(sort.Search(len(c.sections), func(i int) bool {
		return c.sections[i].hash >= v
	}))

	numSections := uint64(len(c.sections))
	if i == numSections {
		i = 0
	}

	endpointIndex := c.sections[i].replicas[n]
	return c.endpoints[endpointIndex], nil
}

type tenantSet map[string]tenantMatcher

func (t tenantSet) match(tenant string) (bool, error) {
	// Fast path for the common case of direct match.
	if mt, ok := t[tenant]; ok && isExactMatcher(mt) {
		return true, nil
	} else {
		for tenantPattern, matcherType := range t {
			switch matcherType {
			case TenantMatcherGlob:
				matches, err := filepath.Match(tenantPattern, tenant)
				if err != nil {
					return false, fmt.Errorf("error matching tenant pattern %s (tenant %s): %w", tenantPattern, tenant, err)
				}
				if matches {
					return true, nil
				}
			case TenantMatcherTypeExact:
				// Already checked above, skipping.
				fallthrough
			default:
				continue
			}

		}
	}
	return false, nil
}

// multiHashring represents a set of hashrings.
// Which hashring to use for a tenant is determined
// by the tenants field of the hashring configuration.
type multiHashring struct {
	cache      map[string]Hashring
	hashrings  []Hashring
	tenantSets []tenantSet

	// We need a mutex to guard concurrent access
	// to the cache map, as this is both written to
	// and read from.
	mu sync.RWMutex

	nodes []Endpoint
}

// Get returns a target to handle the given tenant and time series.
func (m *multiHashring) Get(tenant string, ts *prompb.TimeSeries) (Endpoint, error) {
	return m.GetN(tenant, ts, 0)
}

// GetN returns the nth target to handle the given tenant and time series.
func (m *multiHashring) GetN(tenant string, ts *prompb.TimeSeries, n uint64) (Endpoint, error) {
	m.mu.RLock()
	h, ok := m.cache[tenant]
	m.mu.RUnlock()
	if ok {
		return h.GetN(tenant, ts, n)
	}
	var found bool

	// If the tenant is not in the cache, then we need to check
	// every tenant in the configuration.
	for i, t := range m.tenantSets {
		// If the hashring has no tenants, then it is
		// considered a default hashring and matches everything.
		if t == nil {
			found = true
		} else {
			var err error
			if found, err = t.match(tenant); err != nil {
				return Endpoint{}, err
			}
		}
		if found {
			m.mu.Lock()
			m.cache[tenant] = m.hashrings[i]
			m.mu.Unlock()

			return m.hashrings[i].GetN(tenant, ts, n)
		}
	}
	return Endpoint{}, errors.New("no matching hashring to handle tenant")
}

func (m *multiHashring) Nodes() []Endpoint {
	return m.nodes
}

// newMultiHashring creates a multi-tenant hashring for a given slice of
// groups.
// Which hashring to use for a tenant is determined
// by the tenants field of the hashring configuration.
func NewMultiHashring(algorithm HashringAlgorithm, replicationFactor uint64, cfg []HashringConfig) (Hashring, error) {
	m := &multiHashring{
		cache: make(map[string]Hashring),
	}

	for _, h := range cfg {
		var hashring Hashring
		var err error
		activeAlgorithm := algorithm
		if h.Algorithm != "" {
			activeAlgorithm = h.Algorithm
		}
		hashring, err = newHashring(activeAlgorithm, h.Endpoints, replicationFactor, h.Hashring, h.Tenants)
		if err != nil {
			return nil, err
		}
		m.nodes = append(m.nodes, hashring.Nodes()...)
		m.hashrings = append(m.hashrings, hashring)
		var t map[string]tenantMatcher
		if len(h.Tenants) != 0 {
			t = make(map[string]tenantMatcher)
		}
		for _, tenant := range h.Tenants {
			t[tenant] = h.TenantMatcherType
		}
		m.tenantSets = append(m.tenantSets, t)
	}
	slices.SortFunc(m.nodes, func(a, b Endpoint) int {
		return strings.Compare(a.Address, b.Address)
	})
	return m, nil
}

func newHashring(algorithm HashringAlgorithm, endpoints []Endpoint, replicationFactor uint64, hashring string, tenants []string) (Hashring, error) {
	switch algorithm {
	case AlgorithmHashmod:
		return newSimpleHashring(endpoints)
	case AlgorithmKetama:
		return newKetamaHashring(endpoints, SectionsPerNode, replicationFactor)
	case AlgorithmAlignedKetama:
		return newAlignedKetamaHashring(endpoints, SectionsPerNode, replicationFactor)
	default:
		l := log.NewNopLogger()
		level.Warn(l).Log("msg", "Unrecognizable hashring algorithm. Fall back to hashmod algorithm.",
			"hashring", hashring,
			"tenants", tenants)
		return newSimpleHashring(endpoints)
	}
}
