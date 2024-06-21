// Copyright (c) The Cortex Authors.
// Licensed under the Apache License 2.0.

// Package utils Monitoring platform team helper resources for frontend
package utils

import (
	"errors"
	"net/url"
	"testing"
)

func TestNewFailedQueryCache(t *testing.T) {
	cache, err := NewFailedQueryCache(2)
	if cache == nil {
		t.Fatalf("Expected cache to be created, but got nil")
	}
	if err != nil {
		t.Fatalf("Expected no error message, but got: %s", err.Error())
	}
}

func TestUpdateFailedQueryCache(t *testing.T) {
	cache, _ := NewFailedQueryCache(2)

	tests := []struct {
		name            string
		err             error
		query           url.Values
		expectedResult  bool
		expectedMessage string
	}{
		{
			name: "No error code in error message",
			err:  errors.New("no error code here"),
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"test_query"},
			},
			expectedResult:  false,
			expectedMessage: "msg: String regex conversion error, normalized query: test_query, query range seconds: 100, updating cache for error: no error code here",
		},
		{
			name: "Non-cacheable error code",
			err:  errors.New("serads;ajkvsd( Code(500) code)asd"),
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"test_query"},
			},
			expectedResult:  false,
			expectedMessage: "msg: Query not cached due to non-cacheable error code, normalized query: test_query, query range seconds: 100, updating cache for error: serads;ajkvsd( Code(500) code)asd",
		},
		{
			name: "Cacheable error code",
			err:  errors.New("This is a random error Code(408). It is random."),
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"test_query"},
			},
			expectedResult:  true,
			expectedMessage: "msg: Cached a failed query, normalized query: test_query, range seconds: 100, updating cache for error: This is a random error Code(408). It is random.",
		},

		{
			name: "Adding query with whitespace and ensuring it is normalized",
			err:  errors.New("Adding error with query that has whitespace and tabs Code(408). Let's see what happens."),
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"\n \t tes \t  t query  \n"},
			},
			expectedResult:  true,
			expectedMessage: "msg: Cached a failed query, normalized query:  tes t query , range seconds: 100, updating cache for error: Adding error with query that has whitespace and tabs Code(408). Let's see what happens.",
		},

		{
			name: "Cacheable error code with range of 0, ensuring regex parsing is correct, and updating range length",
			err:  errors.New("error code( Code(408) error.)"),
			query: url.Values{
				"start": {"100"},
				"end":   {"180"},
				"query": {"test_query"},
			},
			expectedResult:  true,
			expectedMessage: "msg: Cached a failed query, normalized query: test_query, range seconds: 80, updating cache for error: error code( Code(408) error.)",
		},

		{
			name: "Successful update to range length",
			err:  errors.New("error code( Code(408) error.)"),
			query: url.Values{
				"start": {"100"},
				"end":   {"100"},
				"query": {"test_query"},
			},
			expectedResult:  true,
			expectedMessage: "msg: Cached a failed query, normalized query: test_query, range seconds: 0, updating cache for error: error code( Code(408) error.)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, message := cache.UpdateFailedQueryCache(tt.err, tt.query)
			if result != tt.expectedResult {
				t.Errorf("expected result %v, got %v", tt.expectedResult, result)
			}
			if message != tt.expectedMessage {
				t.Errorf("expected message to contain %s, got %s", tt.expectedMessage, message)
			}
		})
	}
}

// TestQueryHitCache tests the QueryHitCache method
func TestQueryHitCache(t *testing.T) {
	cache, _ := NewFailedQueryCache(2)
	lruCache := cache.lruCache

	lruCache.Add("test_query", 100)
	lruCache.Add(" tes t query ", 100)

	tests := []struct {
		name            string
		query           url.Values
		expectedResult  bool
		expectedMessage string
	}{
		{
			name: "Cache hit",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"test_query"},
			},
			expectedResult:  true,
			expectedMessage: "msg: Retrieved query from cache, normalized query: test_query, range seconds: 100",
		},
		{
			name: "Cache miss",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {"miss"},
			},
			expectedResult:  false,
			expectedMessage: "",
		},

		{
			name: "Cache miss due to shorter range length",
			query: url.Values{
				"start": {"100"},
				"end":   {"150"},
				"query": {"test_query"},
			},
			expectedResult:  false,
			expectedMessage: "",
		},

		{
			name: "Cache hit whitespace",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {" \n\ttes \tt \n   query \t\n  "},
			},
			expectedResult:  true,
			expectedMessage: "msg: Retrieved query from cache, normalized query:  tes t query , range seconds: 100",
		},

		{
			name: "Cache miss whitespace",
			query: url.Values{
				"start": {"100"},
				"end":   {"200"},
				"query": {" \n\tte s \tt \n   query \t\n  "},
			},
			expectedResult:  false,
			expectedMessage: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, message := cache.QueryHitCache(tt.query)
			if result != tt.expectedResult {
				t.Errorf("expected result %v, got %v", tt.expectedResult, result)
			}
			if message != tt.expectedMessage {
				t.Errorf("expected message to contain %s, got %s", tt.expectedMessage, message)
			}
		})
	}
}
