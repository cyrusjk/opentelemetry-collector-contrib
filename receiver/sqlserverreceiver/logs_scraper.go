// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sqlserverreceiver // Package sqlserverreceiver import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/sqlserverreceiver"
// This is a Log-specific scraper that is used to pull query and query plan text values and publish them as
// Open Telemetry logs.
import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/sqlquery"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/sqlserverreceiver/internal/metadata"
)

var (
	keySet = make(map[string]any, 200)
	logs   = plog.NewLogs()
)

type sqlServerLogsScraperHelper struct {
	id                 component.ID
	sqlQuery           string
	instanceName       string
	scrapeCfg          scraperhelper.ControllerConfig
	clientProviderFunc sqlquery.ClientProviderFunc
	dbProviderFunc     sqlquery.DbProviderFunc
	logger             *zap.Logger
	telemetryConfig    sqlquery.TelemetryConfig
	client             sqlquery.DbClient
	db                 *sql.DB
	metricsBuilder     *metadata.MetricsBuilder
	sharedSubject      *Subject
	// internal fields
	dbClient     sqlquery.DbClient
	cache        *lru.Cache[string, bool]
	computerName string
	sqlInstance  string
}

// Start establish DB connection at startup
func (s *sqlServerLogsScraperHelper) Start(context.Context, component.Host) error {
	var err error
	s.db, err = s.dbProviderFunc()
	if err != nil {
		return fmt.Errorf("failed to open Db connection: %w", err)
	}
	s.client = s.clientProviderFunc(sqlquery.DbWrapper{Db: s.db}, s.sqlQuery, s.logger, s.telemetryConfig)

	input := sharedSubject.Subscribe()
	go func() {
		for {
			s.scrapeAsync(input)
			if sharedSubject.closed {
				break
			}
		}
	}()

	return nil
}

// Shutdown Close the DB connection
func (s *sqlServerLogsScraperHelper) Shutdown(_ context.Context) error {
	s.sharedSubject.Close()
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *sqlServerLogsScraperHelper) scrapeAsync(input <-chan any) {
	val := <-input

	switch val := val.(type) {
	case string:
		_ = s.scrapeFor(val)
	default:
		fmt.Printf("Unknown type %T", val)
	}
}

func (s *sqlServerLogsScraperHelper) initLogs() plog.Logs {
	retval := plog.NewLogs()
	resourceLogs := retval.ResourceLogs().AppendEmpty()
	resource := resourceLogs.Resource()
	resource.Attributes().PutStr("computer_name", s.computerName)
	resource.Attributes().PutStr("sql_instance", s.sqlInstance)

	return retval
}

func (s *sqlServerLogsScraperHelper) scrapeFor(key string) error {
	// check to see if we have already queried for this hash
	hit := s.cache.Contains(key)
	if hit {
		// we have already queried for this hash, so skip it
		return nil
	}
	// split the key into query and plan hashes; the creation of this key should be a shared function to ensure consistency
	keys := strings.Split(key, "-")
	queryHash := keys[0]
	planHash := keys[1]

	// Do we need a new context every call?
	asyncContext := context.Background()
	endFunc := func() {}
	if s.scrapeCfg.Timeout == 0 {
		asyncContext, endFunc = context.WithCancel(asyncContext)
	} else {
		asyncContext, endFunc = context.WithTimeout(asyncContext, s.scrapeCfg.Timeout)
	}

	// dbClient only exposes QueryRows, though q query for a single row is more appropriate here.
	rows, err := s.dbClient.QueryRows(asyncContext, queryHash, planHash)
	endFunc()
	if err != nil {
		s.logger.Error("Failed to query for query text", zap.Error(err))
		return nil
	}
	// We only expect one row, so we check for empty or grab 0.
	if len(rows) == 0 {
		s.logger.Warn("No query text found for key", zap.String("key", key))
		return nil
	}
	// We are going to create the ResourceLogs and related OTel objects manually since the MetricsBuilder is
	// focused just on the metrics objects. Hopefully there will be a more generic way to do this in the future.
	resourceLogs := logs.ResourceLogs().At(0)
	resource := resourceLogs.Resource()
	// There is a spot for the hashes at the resource level, but it is not clear if that is the correct place.
	// Create a Log record for the query with the query_hash as an attribute.
	record := resourceLogs.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	record.Attributes().PutStr("query_hash", queryHash)
	record.Body().SetStr(rows[0]["text"])

	// Create another Log record for the query plan with the query_plan_hash as an attribute.
	record = resourceLogs.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	record.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	record.Attributes().PutStr("query_plan_hash", planHash)
	record.Body().SetStr(rows[0]["query_plan"])
	// Now that we have processed the row, we can add it to the cache to make sure we skip it next time and emit the log.
	s.cache.Add(key, true)
	s.metricsBuilder.Emit(metadata.WithResource(resource))
	return nil
}

// ScrapeLogs  queries the database and returns the logs
// We need the provided Context ot interact with the db, otherwise this work could be done as keys arrive.
// BUT the context is only cancellable and has a configured timeout, which COULD be done independently, maning that this
// function could only serve to emit the already collected logs.
func (s *sqlServerLogsScraperHelper) ScrapeLogs(ctx context.Context) (plog.Logs, error) {
	/*
		newCount := 0
		logs := plog.NewLogs()
		for key := range keySet {
			// check to see if we have already queried for this hash
			hit := s.cache.Contains(key)
			if hit {
				// remove the key from the queue
				delete(keySet, key)
				// we have already queried for this hash, so skip it
				continue
			}
			// split the key into query and plan hashes; the creation of this key should be a shared function to ensure consistency
			keys := strings.Split(key, "-")
			queryHash := keys[0]
			planHash := keys[1]
			// dbClient only exposes QueryRows, though q query for a single row is more appropriate here.
			rows, err := s.dbClient.QueryRows(ctx, queryHash, planHash)
			if err != nil {
				s.logger.Error("Failed to query for query text", zap.Error(err))
				continue
			}
			// We only expect one row, so we check for empty or grab 0.
			if len(rows) == 0 {
				s.logger.Warn("No query text found for key", zap.String("key", key))
				continue
			}
			// We are going to create the ResourceLogs and related OTel objects manually since the MetricsBuilder is
			// focused just on the metrics objects. Hopefully there will be a more generic way to do this in the future.
			resourceLogs := logs.ResourceLogs().AppendEmpty()
			resource := resourceLogs.Resource()
			resource.Attributes().PutStr("computer_name", rows[0]["computer_name"])
			resource.Attributes().PutStr("sql_instance", rows[0]["sql_instance"])
			// There is a spot for the hashes at the resource level, but it is not clear if that is the correct place.
			// Create a Log record for the query with the query_hash as an attribute.
			record := resourceLogs.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			record.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			record.Attributes().PutStr("query_hash", queryHash)
			record.Body().SetStr(rows[0]["text"])

			// Create another Log record for the query plan with the query_plan_hash as an attribute.
			record = resourceLogs.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
			record.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
			record.Attributes().PutStr("query_plan_hash", planHash)
			record.Body().SetStr(rows[0]["query_plan"])
			// Now that we have processed the row, we can add it to the cache to make sure we skip it next time and emit the log.
			s.cache.Add(key, true)
			s.metricsBuilder.Emit(metadata.WithResource(resource))
			newCount++
			delete(keySet, key)
		}
		if newCount > 0 {
			s.logger.Info("Exported query text",
				zap.Int("queue_size", len(keySet)),
				zap.Int("new_count", newCount),
				zap.Int("cache_size", s.cache.Len()))
		}
	*/
	emitMe := logs
	logs = s.initLogs()
	return emitMe, nil
}

// getLogQueries returns the queries for logs from queries.go
func getLogQueries() []string {
	var queries []string

	queries = append(queries, queryForQueryAndPlanText)

	return queries
}

// Sub-constructor to initialize the scraper
func (s *sqlServerLogsScraperHelper) init(cfg *Config) {
	db, err := sql.Open("sqlserver", getDBConnectionString(cfg))
	if err != nil {
		s.logger.Error("Failed to open DB connection", zap.Error(err))
		return
	}
	cache, err := lru.New[string, bool](int(cfg.MaxQuerySampleCount))
	if err != nil {
		s.logger.Error("Failed to create LRU cache", zap.Error(err))
		return
	}
	s.cache = cache
	s.dbClient = sqlquery.NewDbClient(sqlquery.DbWrapper{Db: db},
		getSQLServerQueryTextAndPlanQuery(cfg.InstanceName, cfg.MaxQuerySampleCount, cfg.Granularity),
		s.logger, sqlquery.TelemetryConfig{})
}

// setupSQLServerLogsScrapers creates the scrapers for logs similar to setupSQLServerScrapers
func newSQLServerLogsScraperHelper(
	id component.ID,
	sqlQuery string,
	cfg *Config,
	logger *zap.Logger,
	telemetryConfig sqlquery.TelemetryConfig,
	dbProviderFunc sqlquery.DbProviderFunc,
	clientProviderFunc sqlquery.ClientProviderFunc,
	metricsBuilder *metadata.MetricsBuilder,
	sharedSubject *Subject,
) *sqlServerLogsScraperHelper {
	retval := &sqlServerLogsScraperHelper{
		id:                 id,
		sqlQuery:           sqlQuery,
		instanceName:       cfg.InstanceName,
		scrapeCfg:          cfg.ControllerConfig,
		logger:             logger,
		telemetryConfig:    telemetryConfig,
		dbProviderFunc:     dbProviderFunc,
		clientProviderFunc: clientProviderFunc,
		metricsBuilder:     metricsBuilder,
		sharedSubject:      sharedSubject,
		computerName:       cfg.ComputerName,
		sqlInstance:        cfg.InstanceName,
	}
	retval.init(cfg)
	return retval
}
