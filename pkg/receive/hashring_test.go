// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package receive

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/strutil"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/store/storepb/prompb"
)

func TestHashringGet(t *testing.T) {
	t.Parallel()

	ts := &prompb.TimeSeries{
		Labels: []labelpb.ZLabel{
			{
				Name:  "foo",
				Value: "bar",
			},
			{
				Name:  "baz",
				Value: "qux",
			},
		},
	}

	for _, tc := range []struct {
		name   string
		cfg    []HashringConfig
		nodes  map[string]struct{}
		tenant string
	}{
		{
			name:   "empty",
			cfg:    nil,
			tenant: "tenant1",
		},
		{
			name: "simple",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
				},
			},
			nodes: map[string]struct{}{"node1": {}},
		},
		{
			name: "specific",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant2"},
				},
				{
					Endpoints: []Endpoint{{Address: "node1"}},
				},
			},
			nodes:  map[string]struct{}{"node2": {}},
			tenant: "tenant2",
		},
		{
			name: "many tenants",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant2"},
				},
				{
					Endpoints: []Endpoint{{Address: "node3"}},
					Tenants:   []string{"tenant3"},
				},
			},
			nodes:  map[string]struct{}{"node1": {}},
			tenant: "tenant1",
		},
		{
			name: "many tenants error",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []Endpoint{{Address: "node2"}},
					Tenants:   []string{"tenant2"},
				},
				{
					Endpoints: []Endpoint{{Address: "node3"}},
					Tenants:   []string{"tenant3"},
				},
			},
			tenant: "tenant4",
		},
		{
			name: "many nodes",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}, {Address: "node2"}, {Address: "node3"}},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []Endpoint{{Address: "node4"}, {Address: "node5"}, {Address: "node6"}},
				},
			},
			nodes: map[string]struct{}{
				"node1": {},
				"node2": {},
				"node3": {},
			},
			tenant: "tenant1",
		},
		{
			name: "many nodes default",
			cfg: []HashringConfig{
				{
					Endpoints: []Endpoint{{Address: "node1"}, {Address: "node2"}, {Address: "node3"}},
					Tenants:   []string{"tenant1"},
				},
				{
					Endpoints: []Endpoint{{Address: "node4"}, {Address: "node5"}, {Address: "node6"}},
				},
			},
			nodes: map[string]struct{}{
				"node4": {},
				"node5": {},
				"node6": {},
			},
		},
		{
			name: "glob hashring match",
			cfg: []HashringConfig{
				{
					Endpoints:         []Endpoint{{Address: "node1"}, {Address: "node2"}, {Address: "node3"}},
					Tenants:           []string{"prefix*"},
					TenantMatcherType: TenantMatcherGlob,
				},
				{
					Endpoints: []Endpoint{{Address: "node4"}, {Address: "node5"}, {Address: "node6"}},
				},
			},
			nodes: map[string]struct{}{
				"node1": {},
				"node2": {},
				"node3": {},
			},
			tenant: "prefix-1",
		},
		{
			name: "glob hashring not match",
			cfg: []HashringConfig{
				{
					Endpoints:         []Endpoint{{Address: "node1"}, {Address: "node2"}, {Address: "node3"}},
					Tenants:           []string{"prefix*"},
					TenantMatcherType: TenantMatcherGlob,
				},
				{
					Endpoints: []Endpoint{{Address: "node4"}, {Address: "node5"}, {Address: "node6"}},
				},
			},
			nodes: map[string]struct{}{
				"node4": {},
				"node5": {},
				"node6": {},
			},
			tenant: "suffix-1",
		},
		{
			name: "glob hashring multiple matches",
			cfg: []HashringConfig{
				{
					Endpoints:         []Endpoint{{Address: "node1"}, {Address: "node2"}, {Address: "node3"}},
					Tenants:           []string{"t1-*", "t2", "t3-*"},
					TenantMatcherType: TenantMatcherGlob,
				},
				{
					Endpoints: []Endpoint{{Address: "node4"}, {Address: "node5"}, {Address: "node6"}},
				},
			},
			nodes: map[string]struct{}{
				"node1": {},
				"node2": {},
				"node3": {},
			},
			tenant: "t2",
		},
	} {
		hs, err := NewMultiHashring(AlgorithmHashmod, 3, tc.cfg)
		require.NoError(t, err)

		h, err := hs.Get(tc.tenant, ts)
		if tc.nodes != nil {
			if err != nil {
				t.Errorf("case %q: got unexpected error: %v", tc.name, err)
				continue
			}
			if _, ok := tc.nodes[h.Address]; !ok {
				t.Errorf("case %q: got unexpected node %q", tc.name, h)
			}
			continue
		}
		if err == nil {
			t.Errorf("case %q: expected error", tc.name)
		}
	}
}

func TestKetamaHashringGet(t *testing.T) {
	t.Parallel()

	baseTS := &prompb.TimeSeries{
		Labels: []labelpb.ZLabel{
			{
				Name:  "pod",
				Value: "nginx",
			},
		},
	}
	tests := []struct {
		name         string
		endpoints    []Endpoint
		expectedNode string
		ts           *prompb.TimeSeries
		n            uint64
	}{
		{
			name:         "base case",
			endpoints:    []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}},
			ts:           baseTS,
			expectedNode: "node-2",
		},
		{
			name:         "base case with replication",
			endpoints:    []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}},
			ts:           baseTS,
			n:            1,
			expectedNode: "node-1",
		},
		{
			name:         "base case with replication",
			endpoints:    []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}},
			ts:           baseTS,
			n:            2,
			expectedNode: "node-3",
		},
		{
			name:         "base case with replication and reordered nodes",
			endpoints:    []Endpoint{{Address: "node-1"}, {Address: "node-3"}, {Address: "node-2"}},
			ts:           baseTS,
			n:            2,
			expectedNode: "node-3",
		},
		{
			name:         "base case with new node at beginning of ring",
			endpoints:    []Endpoint{{Address: "node-0"}, {Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}},
			ts:           baseTS,
			expectedNode: "node-2",
		},
		{
			name:         "base case with new node at end of ring",
			endpoints:    []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}, {Address: "node-4"}},
			ts:           baseTS,
			expectedNode: "node-2",
		},
		{
			name:      "base case with different timeseries",
			endpoints: []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}},
			ts: &prompb.TimeSeries{
				Labels: []labelpb.ZLabel{
					{
						Name:  "pod",
						Value: "thanos",
					},
				},
			},
			expectedNode: "node-3",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hashRing, err := newKetamaHashring(test.endpoints, 10, test.n+1)
			require.NoError(t, err)

			result, err := hashRing.GetN("tenant", test.ts, test.n)
			require.NoError(t, err)
			require.Equal(t, test.expectedNode, result.Address)
		})
	}
}

func TestAlignedKetamaHashringGet(t *testing.T) {
	t.Parallel()

	ep0a := Endpoint{Address: podDNS("pod", 0), AZ: "zone-a"}
	ep1a := Endpoint{Address: podDNS("pod", 1), AZ: "zone-a"}
	ep0b := Endpoint{Address: podDNS("pod", 0), AZ: "zone-b"}
	ep1b := Endpoint{Address: podDNS("pod", 1), AZ: "zone-b"}
	ep0c := Endpoint{Address: podDNS("pod", 0), AZ: "zone-c"}
	ep1c := Endpoint{Address: podDNS("pod", 1), AZ: "zone-c"}
	invalidEp := Endpoint{Address: "invalid-address", AZ: "zone-a"}
	duplicateEp0a := Endpoint{Address: podDNS("anotherpod", 0), AZ: "zone-a"}

	tsForReplicaTest := &prompb.TimeSeries{
		Labels: []labelpb.ZLabel{{Name: "test", Value: "replica-routing"}},
	}

	testCases := map[string]struct {
		inputEndpoints    []Endpoint
		replicationFactor uint64
		sectionsPerNode   int

		tenant           string
		ts               *prompb.TimeSeries
		n                uint64
		expectedEndpoint Endpoint

		expectConstructorError   bool
		constructorErrorContains string
		expectGetNError          bool
		getNErrorContains        string
	}{
		"valid 2 AZs, RF=2, get replica 0": {
			inputEndpoints:         []Endpoint{ep1a, ep0b, ep0a, ep1b},
			replicationFactor:      2,
			sectionsPerNode:        SectionsPerNode,
			tenant:                 "tenant1",
			ts:                     tsForReplicaTest,
			n:                      0,
			expectedEndpoint:       ep0a,
			expectConstructorError: false,
			expectGetNError:        false,
		},
		"valid 2 AZs, RF=2, get replica 1": {
			inputEndpoints:         []Endpoint{ep1a, ep0b, ep0a, ep1b},
			replicationFactor:      2,
			sectionsPerNode:        SectionsPerNode,
			tenant:                 "tenant1",
			ts:                     tsForReplicaTest,
			n:                      1,
			expectedEndpoint:       ep0b,
			expectConstructorError: false,
			expectGetNError:        false,
		},
		"valid 3 AZs, RF=3, get replica 0": {
			inputEndpoints:         []Endpoint{ep1a, ep0b, ep0a, ep1b, ep0c, ep1c},
			replicationFactor:      3,
			sectionsPerNode:        SectionsPerNode,
			tenant:                 "tenant1",
			ts:                     tsForReplicaTest,
			n:                      0,
			expectedEndpoint:       ep0a,
			expectConstructorError: false,
			expectGetNError:        false,
		},
		"valid 3 AZs, RF=3, get replica 1": {
			inputEndpoints:         []Endpoint{ep1a, ep0b, ep0a, ep1b, ep0c, ep1c},
			replicationFactor:      3,
			sectionsPerNode:        SectionsPerNode,
			tenant:                 "tenant1",
			ts:                     tsForReplicaTest,
			n:                      1,
			expectedEndpoint:       ep0b,
			expectConstructorError: false,
			expectGetNError:        false,
		},
		"valid 3 AZs, RF=3, get replica 2": {
			inputEndpoints:         []Endpoint{ep1a, ep0b, ep0a, ep1b, ep0c, ep1c},
			replicationFactor:      3,
			sectionsPerNode:        SectionsPerNode,
			tenant:                 "tenant1",
			ts:                     tsForReplicaTest,
			n:                      2,
			expectedEndpoint:       ep0c,
			expectConstructorError: false,
			expectGetNError:        false,
		},
		"error: empty input": {
			inputEndpoints:           []Endpoint{},
			replicationFactor:        1,
			sectionsPerNode:          SectionsPerNode,
			expectConstructorError:   true,
			constructorErrorContains: "no endpoints provided",
		},
		"error: invalid address": {
			inputEndpoints:           []Endpoint{ep0a, invalidEp, ep0b},
			replicationFactor:        2,
			sectionsPerNode:          SectionsPerNode,
			expectConstructorError:   true,
			constructorErrorContains: "failed to extract ordinal from address invalid-address",
		},
		"error: duplicate ordinal": {
			inputEndpoints:           []Endpoint{ep0a, ep1a, ep0b, duplicateEp0a},
			replicationFactor:        2,
			sectionsPerNode:          SectionsPerNode,
			expectConstructorError:   true,
			constructorErrorContains: "duplicate endpoint",
		},
		"error: missing ordinal 0": {
			inputEndpoints:           []Endpoint{ep1a, ep1b},
			replicationFactor:        2,
			sectionsPerNode:          SectionsPerNode,
			expectConstructorError:   true,
			constructorErrorContains: "failed to group endpoints by AZ: AZ \"zone-a\" is missing endpoint with ordinal 0",
		},
		"error: AZ count != RF (too few AZs)": {
			inputEndpoints:           []Endpoint{ep0a, ep1a},
			replicationFactor:        2,
			sectionsPerNode:          SectionsPerNode,
			expectConstructorError:   true,
			constructorErrorContains: "number of AZs (1) must equal replication factor (2)",
		},
		"error: AZ count != RF (too many AZs)": {
			inputEndpoints:           []Endpoint{ep0a, ep1a, ep0b, ep1b, ep0c, ep1c},
			replicationFactor:        2,
			sectionsPerNode:          SectionsPerNode,
			expectConstructorError:   true,
			constructorErrorContains: "number of AZs (3) must equal replication factor (2)",
		},
		"constructor success with unbalanced AZs (uses common subset)": {
			inputEndpoints:         []Endpoint{ep0a, ep1a, ep0b},
			replicationFactor:      2,
			sectionsPerNode:        SectionsPerNode,
			expectConstructorError: false,
		},
		"error: GetN index out of bounds (n >= numEndpoints)": {
			inputEndpoints:         []Endpoint{ep1a, ep0b, ep0a, ep1b},
			replicationFactor:      2,
			sectionsPerNode:        SectionsPerNode,
			tenant:                 "tenant1",
			ts:                     tsForReplicaTest,
			n:                      4,
			expectConstructorError: false,
			expectGetNError:        true,
			getNErrorContains:      "insufficient nodes; have 4, want 5",
		},
	}

	for tcName, tc := range testCases {
		t.Run(tcName, func(t *testing.T) {
			hashRing, err := newAlignedKetamaHashring(tc.inputEndpoints, tc.sectionsPerNode, tc.replicationFactor)

			if tc.expectConstructorError {
				require.Error(t, err, "Expected constructor error")
				require.Nil(t, hashRing, "Hashring should be nil on constructor error")
				if tc.constructorErrorContains != "" {
					require.Contains(t, err.Error(), tc.constructorErrorContains, "Constructor error message mismatch")
				}
				return
			}

			require.NoError(t, err, "Expected constructor to succeed")
			require.NotNil(t, hashRing, "Hashring should not be nil on successful construction")

			if tc.ts == nil && !tc.expectGetNError {
				return
			}
			if tc.ts == nil && tc.expectGetNError {
				tc.ts = &prompb.TimeSeries{Labels: []labelpb.ZLabel{{Name: "dummy", Value: "dummy"}}}
			}

			result, getNErr := hashRing.GetN(tc.tenant, tc.ts, tc.n)
			if tc.expectGetNError {
				require.Error(t, getNErr, "Expected GetN error")
				if tc.getNErrorContains != "" {
					require.Contains(t, getNErr.Error(), tc.getNErrorContains, "GetN error message mismatch")
				}
			} else {
				require.NoError(t, getNErr, "Expected GetN to succeed")
				testutil.Equals(t, tc.expectedEndpoint, result, "GetN returned unexpected endpoint")
			}
		})
	}
}

func TestAlignedKetamaHashringReplicaOrdinals(t *testing.T) {
	t.Parallel()

	endpoints := []Endpoint{
		{Address: podDNS("pod", 0), AZ: "zone-a"},
		{Address: podDNS("pod", 1), AZ: "zone-a"},
		{Address: podDNS("pod", 2), AZ: "zone-a"},
		{Address: podDNS("pod", 0), AZ: "zone-b"},
		{Address: podDNS("pod", 1), AZ: "zone-b"},
		{Address: podDNS("pod", 2), AZ: "zone-b"},
		{Address: podDNS("pod", 0), AZ: "zone-c"},
		{Address: podDNS("pod", 1), AZ: "zone-c"},
		{Address: podDNS("pod", 2), AZ: "zone-c"},
	}
	replicationFactor := uint64(3)
	sectionsPerNode := 10

	hashRing, err := newAlignedKetamaHashring(endpoints, sectionsPerNode, replicationFactor)
	require.NoError(t, err, "Aligned hashring constructor failed")
	require.NotNil(t, hashRing, "Hashring should not be nil")
	require.NotEmpty(t, hashRing.sections, "Hashring should contain sections")

	// Verify that all replicas within a section have the same ordinal.
	for i, s := range hashRing.sections {
		if len(s.replicas) == 0 {
			continue
		}

		expectedOrdinal := -1

		for replicaNum, replicaIndex := range s.replicas {
			require.Less(t, int(replicaIndex), len(hashRing.endpoints),
				"Section %d (hash %d), Replica %d: index %d out of bounds for endpoints list (len %d)",
				i, s.hash, replicaNum, replicaIndex, len(hashRing.endpoints))

			endpoint := hashRing.endpoints[replicaIndex]
			ordinal, err := strutil.ExtractPodOrdinal(endpoint.Address)
			require.NoError(t, err,
				"Section %d (hash %d), Replica %d: failed to extract ordinal from address %s",
				i, s.hash, replicaNum, endpoint.Address)

			if expectedOrdinal == -1 {
				expectedOrdinal = ordinal
			} else {
				require.Equal(t, expectedOrdinal, ordinal,
					"Section %d (hash %d), Replica %d (%s): Mismatched ordinal. Expected %d, got %d. Replicas in section: %v",
					i, s.hash, replicaNum, endpoint.Address, expectedOrdinal, ordinal, s.replicas)
			}
		}
		if len(s.replicas) > 0 {
			require.NotEqual(t, -1, expectedOrdinal, "Section %d (hash %d): Failed to determine expected ordinal for replicas %v", i, s.hash, s.replicas)
		}
	}
}

func TestKetamaHashringBadConfigIsRejected(t *testing.T) {
	t.Parallel()

	_, err := newKetamaHashring([]Endpoint{{Address: "node-1"}}, 1, 2)
	require.Error(t, err)
}

func TestKetamaHashringConsistency(t *testing.T) {
	t.Parallel()

	series := makeSeries()

	ringA := []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}}
	a1, err := assignSeries(series, ringA)
	require.NoError(t, err)

	ringB := []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}}
	a2, err := assignSeries(series, ringB)
	require.NoError(t, err)

	for node, ts := range a1 {
		require.Len(t, a2[node], len(ts), "node %s has an inconsistent number of series", node)
	}

	for node, ts := range a2 {
		require.Len(t, a1[node], len(ts), "node %s has an inconsistent number of series", node)
	}
}

func TestKetamaHashringIncreaseAtEnd(t *testing.T) {
	t.Parallel()

	series := makeSeries()

	initialRing := []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}}
	initialAssignments, err := assignSeries(series, initialRing)
	require.NoError(t, err)

	resizedRing := []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}, {Address: "node-4"}, {Address: "node-5"}}
	reassignments, err := assignSeries(series, resizedRing)
	require.NoError(t, err)

	// Assert that the initial nodes have no new keys after increasing the ring size
	for _, node := range initialRing {
		for _, ts := range reassignments[node.Address] {
			foundInInitialAssignment := findSeries(initialAssignments, node.Address, ts)
			require.True(t, foundInInitialAssignment, "node %s contains new series after resizing", node)
		}
	}
}

func TestKetamaHashringIncreaseInMiddle(t *testing.T) {
	t.Parallel()

	series := makeSeries()

	initialRing := []Endpoint{{Address: "node-1"}, {Address: "node-3"}}
	initialAssignments, err := assignSeries(series, initialRing)
	require.NoError(t, err)

	resizedRing := []Endpoint{{Address: "node-1"}, {Address: "node-2"}, {Address: "node-3"}}
	reassignments, err := assignSeries(series, resizedRing)
	require.NoError(t, err)

	// Assert that the initial nodes have no new keys after increasing the ring size
	for _, node := range initialRing {
		for _, ts := range reassignments[node.Address] {
			foundInInitialAssignment := findSeries(initialAssignments, node.Address, ts)
			require.True(t, foundInInitialAssignment, "node %s contains new series after resizing", node)
		}
	}
}

func TestKetamaHashringReplicationConsistency(t *testing.T) {
	t.Parallel()

	series := makeSeries()

	initialRing := []Endpoint{{Address: "node-1"}, {Address: "node-4"}, {Address: "node-5"}}
	initialAssignments, err := assignReplicatedSeries(series, initialRing, 2)
	require.NoError(t, err)

	resizedRing := []Endpoint{{Address: "node-4"}, {Address: "node-3"}, {Address: "node-1"}, {Address: "node-2"}, {Address: "node-5"}}
	reassignments, err := assignReplicatedSeries(series, resizedRing, 2)
	require.NoError(t, err)

	// Assert that the initial nodes have no new keys after increasing the ring size
	for _, node := range initialRing {
		for _, ts := range reassignments[node.Address] {
			foundInInitialAssignment := findSeries(initialAssignments, node.Address, ts)
			require.True(t, foundInInitialAssignment, "node %s contains new series after resizing", node)
		}
	}
}

func TestKetamaHashringReplicationConsistencyWithAZs(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		initialRing []Endpoint
		resizedRing []Endpoint
		replicas    uint64
	}{
		{
			initialRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}},
			resizedRing: []Endpoint{{Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}, {Address: "a", AZ: "1"}, {Address: "d", AZ: "2"}, {Address: "e", AZ: "4"}},
			replicas:    3,
		},
		{
			initialRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}},
			resizedRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}, {Address: "d", AZ: "1"}, {Address: "e", AZ: "2"}, {Address: "f", AZ: "3"}},
			replicas:    3,
		},
		{
			initialRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}},
			resizedRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}, {Address: "d", AZ: "4"}, {Address: "e", AZ: "5"}, {Address: "f", AZ: "6"}},
			replicas:    3,
		},
		{
			initialRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}},
			resizedRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}, {Address: "c", AZ: "3"}, {Address: "d", AZ: "4"}, {Address: "e", AZ: "5"}, {Address: "f", AZ: "6"}},
			replicas:    2,
		},
		{
			initialRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "c", AZ: "2"}, {Address: "f", AZ: "3"}},
			resizedRing: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "1"}, {Address: "c", AZ: "2"}, {Address: "d", AZ: "2"}, {Address: "f", AZ: "3"}},
			replicas:    2,
		},
	} {
		t.Run("", func(t *testing.T) {
			series := makeSeries()

			initialAssignments, err := assignReplicatedSeries(series, tt.initialRing, tt.replicas)
			require.NoError(t, err)

			reassignments, err := assignReplicatedSeries(series, tt.resizedRing, tt.replicas)
			require.NoError(t, err)

			// Assert that the initial nodes have no new keys after increasing the ring size
			for _, node := range tt.initialRing {
				for _, ts := range reassignments[node.Address] {
					foundInInitialAssignment := findSeries(initialAssignments, node.Address, ts)
					require.True(t, foundInInitialAssignment, "node %s contains new series after resizing", node)
				}
			}
		})
	}
}

func TestKetamaHashringEvenAZSpread(t *testing.T) {
	t.Parallel()

	tenant := "default-tenant"
	ts := &prompb.TimeSeries{
		Labels:  labelpb.ZLabelsFromPromLabels(labels.FromStrings("foo", "bar")),
		Samples: []prompb.Sample{{Value: 1, Timestamp: 0}},
	}

	for _, tt := range []struct {
		nodes    []Endpoint
		replicas uint64
	}{
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "1"},
				{Address: "d", AZ: "2"},
			},
			replicas: 1,
		},
		{
			nodes:    []Endpoint{{Address: "a"}, {Address: "b"}, {Address: "c"}, {Address: "d"}},
			replicas: 1,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "1"},
				{Address: "d", AZ: "2"},
			},
			replicas: 2,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "3"},
				{Address: "d", AZ: "1"},
				{Address: "e", AZ: "2"},
				{Address: "f", AZ: "3"},
			},
			replicas: 3,
		},
		{
			nodes:    []Endpoint{{Address: "a"}, {Address: "b"}, {Address: "c"}, {Address: "d"}, {Address: "e"}, {Address: "f"}, {Address: "g"}},
			replicas: 3,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "3"},
				{Address: "d", AZ: "1"},
				{Address: "e", AZ: "2"},
				{Address: "f", AZ: "3"},
				{Address: "g", AZ: "4"},
				{Address: "h", AZ: "4"},
				{Address: "i", AZ: "4"},
				{Address: "j", AZ: "5"},
				{Address: "k", AZ: "5"},
				{Address: "l", AZ: "5"},
			},
			replicas: 10,
		},
	} {
		t.Run("", func(t *testing.T) {
			hashRing, err := newKetamaHashring(tt.nodes, SectionsPerNode, tt.replicas)
			testutil.Ok(t, err)

			availableAzs := make(map[string]int64)
			for _, endpoint := range tt.nodes {
				availableAzs[endpoint.AZ] = 0
			}

			azSpread := make(map[string]int64)
			for i := 0; i < int(tt.replicas); i++ {
				r, err := hashRing.GetN(tenant, ts, uint64(i))
				testutil.Ok(t, err)

				for _, n := range tt.nodes {
					if !strings.HasPrefix(n.Address, r.Address) {
						continue
					}
					azSpread[n.AZ]++
				}

			}

			expectedAzSpreadLength := int(tt.replicas)
			if int(tt.replicas) > len(availableAzs) {
				expectedAzSpreadLength = len(availableAzs)
			}
			testutil.Equals(t, len(azSpread), expectedAzSpreadLength)

			for _, writeToAz := range azSpread {
				minAz := sizeOfLeastOccupiedAZ(azSpread)
				testutil.Assert(t, math.Abs(float64(writeToAz-minAz)) <= 1.0)
			}
		})
	}
}

func TestKetamaHashringEvenNodeSpread(t *testing.T) {
	t.Parallel()

	tenant := "default-tenant"

	for _, tt := range []struct {
		nodes     []Endpoint
		replicas  uint64
		numSeries uint64
	}{
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "1"},
				{Address: "d", AZ: "2"},
			},
			replicas:  2,
			numSeries: 1000,
		},
		{
			nodes:     []Endpoint{{Address: "a"}, {Address: "b"}, {Address: "c"}, {Address: "d"}},
			replicas:  2,
			numSeries: 1000,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "3"},
				{Address: "d", AZ: "2"},
				{Address: "e", AZ: "1"},
				{Address: "f", AZ: "3"},
			},
			replicas:  3,
			numSeries: 10000,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "3"},
				{Address: "d", AZ: "2"},
				{Address: "e", AZ: "1"},
				{Address: "f", AZ: "3"},
				{Address: "g", AZ: "1"},
				{Address: "h", AZ: "2"},
				{Address: "i", AZ: "3"},
			},
			replicas:  2,
			numSeries: 10000,
		},
		{
			nodes: []Endpoint{
				{Address: "a", AZ: "1"},
				{Address: "b", AZ: "2"},
				{Address: "c", AZ: "3"},
				{Address: "d", AZ: "2"},
				{Address: "e", AZ: "1"},
				{Address: "f", AZ: "3"},
				{Address: "g", AZ: "1"},
				{Address: "h", AZ: "2"},
				{Address: "i", AZ: "3"},
			},
			replicas:  9,
			numSeries: 10000,
		},
	} {
		t.Run("", func(t *testing.T) {
			hashRing, err := newKetamaHashring(tt.nodes, SectionsPerNode, tt.replicas)
			testutil.Ok(t, err)
			optimalSpread := int(tt.numSeries*tt.replicas) / len(tt.nodes)
			nodeSpread := make(map[string]int)
			for i := 0; i < int(tt.numSeries); i++ {
				ts := &prompb.TimeSeries{
					Labels:  labelpb.ZLabelsFromPromLabels(labels.FromStrings("foo", fmt.Sprintf("%d", i))),
					Samples: []prompb.Sample{{Value: 1, Timestamp: 0}},
				}
				for j := 0; j < int(tt.replicas); j++ {
					r, err := hashRing.GetN(tenant, ts, uint64(j))
					testutil.Ok(t, err)

					nodeSpread[r.Address]++
				}
			}
			for _, node := range nodeSpread {
				diff := math.Abs(float64(node) - float64(optimalSpread))
				testutil.Assert(t, diff/float64(optimalSpread) < 0.1)
			}
		})
	}
}

func TestInvalidAZHashringCfg(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		cfg           []HashringConfig
		replicas      uint64
		algorithm     HashringAlgorithm
		expectedError string
	}{
		{
			cfg:           []HashringConfig{{Endpoints: []Endpoint{{Address: "a", AZ: "1"}, {Address: "b", AZ: "2"}}}},
			replicas:      2,
			expectedError: "Hashmod algorithm does not support AZ aware hashring configuration. Either use Ketama or remove AZ configuration.",
		},
	} {
		t.Run("", func(t *testing.T) {
			_, err := NewMultiHashring(tt.algorithm, tt.replicas, tt.cfg)
			require.EqualError(t, err, tt.expectedError)
		})
	}
}

// podDNS creates a DNS-like string for testing endpoint addresses.
func podDNS(name string, ordinal int) string {
	return name + "-" + strconv.Itoa(ordinal) + ".test-svc.test-namespace.svc.cluster.local"
}

func TestGroupByAZ(t *testing.T) {
	// Test setup endpoints.
	ep0a := Endpoint{Address: podDNS("pod", 0), AZ: "zone-a"}
	ep1a := Endpoint{Address: podDNS("pod", 1), AZ: "zone-a"}
	ep2a := Endpoint{Address: podDNS("pod", 2), AZ: "zone-a"}
	ep0b := Endpoint{Address: podDNS("pod", 0), AZ: "zone-b"}
	ep1b := Endpoint{Address: podDNS("pod", 1), AZ: "zone-b"}
	ep0c := Endpoint{Address: podDNS("pod", 0), AZ: "zone-c"}
	ep1c := Endpoint{Address: podDNS("pod", 1), AZ: "zone-c"}
	invalidEp := Endpoint{Address: "invalid-address-format", AZ: "zone-a"}
	duplicateEp0a := Endpoint{Address: podDNS("anotherpod", 0), AZ: "zone-a"} // Same ordinal (0) as ep0a in zone-a.

	testCases := map[string]struct {
		inputEndpoints []Endpoint
		expectedResult [][]Endpoint
		expectError    bool
		errorContains  string
	}{
		"error on empty input": {
			inputEndpoints: []Endpoint{},
			expectedResult: nil,
			expectError:    true,
			errorContains:  "no endpoints provided",
		},
		"single AZ, multiple endpoints": {
			inputEndpoints: []Endpoint{ep1a, ep0a, ep2a},
			expectedResult: [][]Endpoint{
				{ep0a, ep1a, ep2a},
			},
			expectError: false,
		},
		"multiple AZs, balanced and ordered": {
			inputEndpoints: []Endpoint{ep1a, ep0b, ep0a, ep1b},
			expectedResult: [][]Endpoint{
				{ep0a, ep1a},
				{ep0b, ep1b},
			},
			expectError: false,
		},
		"multiple AZs, different counts, stops at first missing ordinal > 0": {
			inputEndpoints: []Endpoint{ep1a, ep0b, ep0a, ep1b, ep2a, ep0c},
			expectedResult: [][]Endpoint{
				{ep0a},
				{ep0b},
				{ep0c},
			},
			expectError: false,
		},
		"error if ordinal 0 missing in any AZ": {
			inputEndpoints: []Endpoint{ep1a, ep2a, ep1b},
			expectedResult: nil,
			expectError:    true,
			errorContains:  "missing endpoint with ordinal 0",
		},
		"error if ordinal 0 missing in only one AZ": {
			inputEndpoints: []Endpoint{ep0a, ep1a, ep1b},
			expectedResult: nil,
			expectError:    true,
			errorContains:  `AZ "zone-b" is missing endpoint with ordinal 0`,
		},
		"error on invalid address format": {
			inputEndpoints: []Endpoint{ep0a, invalidEp, ep0b},
			expectedResult: nil,
			expectError:    true,
			errorContains:  "failed to extract ordinal from address invalid-address-format",
		},
		"error on duplicate ordinal within an AZ": {
			inputEndpoints: []Endpoint{ep0a, ep1a, ep0b, duplicateEp0a},
			expectedResult: nil,
			expectError:    true,
			errorContains:  "duplicate endpoint ordinal 0 for address " + duplicateEp0a.Address + " in AZ zone-a",
		},
		"AZ sorting check": {
			inputEndpoints: []Endpoint{ep0b, ep0c, ep0a},
			expectedResult: [][]Endpoint{
				{ep0a},
				{ep0b},
				{ep0c},
			},
			expectError: false,
		},
		"multiple AZs, stops correctly when next ordinal missing everywhere": {
			inputEndpoints: []Endpoint{ep1a, ep0b, ep0a, ep1b, ep0c, ep1c},
			expectedResult: [][]Endpoint{
				{ep0a, ep1a},
				{ep0b, ep1b},
				{ep0c, ep1c},
			},
			expectError: false,
		},
	}

	for tcName, tc := range testCases {
		t.Run(tcName, func(t *testing.T) {
			result, err := groupByAZ(tc.inputEndpoints)

			if tc.expectError {
				testutil.NotOk(t, err)
				if tc.errorContains != "" {
					testutil.Assert(t, strings.Contains(err.Error(), tc.errorContains), "Expected error message to contain '%s', but got: %v", tc.errorContains, err)
				}
				testutil.Assert(t, result == nil, "Expected nil result on error, got: %v", result)
			} else {
				testutil.Ok(t, err)
				testutil.Equals(t, tc.expectedResult, result)

				// Verify outer slice (AZs) is sorted alphabetically.
				if err == nil && len(result) > 1 {
					azOrderCorrect := sort.SliceIsSorted(result, func(i, j int) bool {
						return result[i][0].AZ < result[j][0].AZ
					})
					testutil.Assert(t, azOrderCorrect, "Outer slice is not sorted by AZ")
				}
			}
		})
	}
}

func makeSeries() []prompb.TimeSeries {
	numSeries := 10000
	series := make([]prompb.TimeSeries, numSeries)
	for i := 0; i < numSeries; i++ {
		series[i] = prompb.TimeSeries{
			Labels: []labelpb.ZLabel{
				{
					Name:  "pod",
					Value: fmt.Sprintf("nginx-%d", i),
				},
			},
		}
	}
	return series
}

func findSeries(initialAssignments map[string][]prompb.TimeSeries, node string, newSeries prompb.TimeSeries) bool {
	for _, oldSeries := range initialAssignments[node] {
		l1 := labelpb.ZLabelsToPromLabels(newSeries.Labels)
		l2 := labelpb.ZLabelsToPromLabels(oldSeries.Labels)
		if labels.Equal(l1, l2) {
			return true
		}
	}

	return false
}

func assignSeries(series []prompb.TimeSeries, nodes []Endpoint) (map[string][]prompb.TimeSeries, error) {
	return assignReplicatedSeries(series, nodes, 0)
}

func assignReplicatedSeries(series []prompb.TimeSeries, nodes []Endpoint, replicas uint64) (map[string][]prompb.TimeSeries, error) {
	hashRing, err := newKetamaHashring(nodes, SectionsPerNode, replicas)
	if err != nil {
		return nil, err
	}
	assignments := make(map[string][]prompb.TimeSeries)
	for i := uint64(0); i < replicas; i++ {
		for _, ts := range series {
			result, err := hashRing.GetN("tenant", &ts, i)
			if err != nil {
				return nil, err
			}
			assignments[result.Address] = append(assignments[result.Address], ts)

		}
	}

	return assignments, nil
}
