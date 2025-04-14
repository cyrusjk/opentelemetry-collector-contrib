// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package mysqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/mysqlreceiver"

import (
	"time"

	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/mysqlreceiver/internal/metadata"
)

const (
	defaultStatementEventsDigestTextLimit = 120
	defaultStatementEventsLimit           = 250
	defaultStatementEventsTimeLimit       = 24 * time.Hour
)

type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	Username                       string              `mapstructure:"username,omitempty"`
	Password                       configopaque.String `mapstructure:"password,omitempty"`
	Database                       string              `mapstructure:"database,omitempty"`
	AllowNativePasswords           bool                `mapstructure:"allow_native_passwords,omitempty"`
	confignet.AddrConfig           `mapstructure:",squash"`
	TLS                            configtls.ClientConfig        `mapstructure:"tls,omitempty"`
	MetricsBuilderConfig           metadata.MetricsBuilderConfig `mapstructure:",squash"`
	StatementEvents                StatementEventsConfig         `mapstructure:"statement_events"`
	QueryMetricsAsLogs             bool                          `mapstructure:"query_metrics_as_logs"`
	TopQueryCollection             TopQueryCollection            `mapstructure:"top_query_collection"`
}

type StatementEventsConfig struct {
	DigestTextLimit int           `mapstructure:"digest_text_limit"`
	Limit           int           `mapstructure:"limit"`
	TimeLimit       time.Duration `mapstructure:"time_limit"`
}

type TopQueryCollection struct {
	// Enabled enables the collection of the top queries by the execution time.
	// It will collect the top N queries based on totalElapsedTimeDiffs during the last collection interval.
	// The query statement will also be reported, hence, it is not ideal to send it as a metric. Hence
	// we are reporting them as logs.
	// The `N` is configured via `TopQueryCount`
	Enabled             bool `mapstructure:"enabled"`
	LookbackTime        uint `mapstructure:"lookback_time"`
	MaxQuerySampleCount uint `mapstructure:"max_query_sample_count"`
	TopQueryCount       uint `mapstructure:"top_query_count"`
}

func (cfg *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil {
		// Nothing to do if there is no config given.
		return nil
	}

	// Change the default to Insecure = true as we don't want to break
	// existing deployments which does not use TLS by default.
	if !componentParser.IsSet("tls") {
		cfg.TLS = configtls.ClientConfig{}
		cfg.TLS.Insecure = true
	}
	return componentParser.Unmarshal(cfg)
}
