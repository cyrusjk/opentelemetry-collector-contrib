// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sqlserverreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/sqlserverreceiver"
// This is a prototype context that can be shared across all the scrapers within this receiver.
// The initial use case for this is sharing attributes from the metrics scraper that can be picked up by
// the logs scraper and used to emit data specific to the shared values.

import "sync"

const (
	// query_and_plan_hash_collector_key is the key used to store the query and plan hash collector in the shared context
	query_and_plan_hash_collector_key = "QUERY_AND_PLAN_HASH_COLLECTOR"
)

type ScraperContext struct {
	// use a sync map to maintain safety between threads using it.
	contextMap sync.Map
}

var sharedContext *ScraperContext

func GetContext() *ScraperContext {
	if sharedContext != nil {
		return sharedContext
	}
	sharedContext = &ScraperContext{}
	return sharedContext
}

func (sc *ScraperContext) Get(key string) *any {
	retval, _ := sc.contextMap.Load(key)
	return &retval
}

func (sc *ScraperContext) Add(key string, value any) {
	sc.contextMap.Store(key, value)
}
