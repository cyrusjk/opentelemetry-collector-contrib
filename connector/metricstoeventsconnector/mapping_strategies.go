package metricstoeventsconnector

import (
	"fmt"
	"strings"

	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"sort"
)

type scopeKey struct {
	Attributes map[string]string
	TypeName   string
	UnitName   string
}

type scopeKeySlice []scopeKey

func (s scopeKey) String() string {
	// Sort the keys of the attributes map
	keys := maps.Keys(s.Attributes)
	sort.Strings(keys)

	// Create a string representation of the attributes
	var sb strings.Builder
	for _, key := range keys {
		sb.WriteString(fmt.Sprintf("%s:%v,", key, s.Attributes[key]))
	}

	// Remove the trailing comma and return the string
	return sb.String()
}

func (s scopeKeySlice) Len() int {
	return len(s)
}
func (s scopeKeySlice) Less(i, j int) bool {
	return s[i].String() < s[j].String()
}
func (s scopeKeySlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s scopeKey) Equal(ss scopeKey) bool {
	if len(s.Attributes) != len(ss.Attributes) {
		return false
	}
	for k, v := range s.Attributes {
		if v != ss.Attributes[k] {
			return false
		}
	}
	if s.TypeName != ss.TypeName {
		return false
	}
	if s.UnitName != ss.UnitName {
		return false
	}
	return true
}

const (
// LogsStability    = component.StabilityLevelDevelopment
// MetricsStability = component.StabilityLevelBeta
)

var (
	mappingStrategyNames = []string{"simple", "attributes_to_scope"}
)

func MappingStratgyNames() []string {
	return mappingStrategyNames
}

type MappingStrategy struct {
	config *Config
	logger *zap.Logger
}

/* Simple mapping of Metric DataPoints to Log Attributes. This strategy aggregates all data points for a given metric
into a single attribute value and discards any Metric Attributes. Optionally, it may add the type and unit of measure as
additional attributes with those titles appended to the metric name. For example, a metric named "cpu.temperature" would have
a "cpu.temperature_type" attribute with the value "gauge" and a "cpu.temperature_unit" attribute with the value "Celsius". This
strategy is useful for quickly converting metric data to log data without losing any information, but it may not be
suitable for all use cases.
*/

func (ms *MappingStrategy) SimpleMappingStrategy(metrics pmetric.Metrics) (plog.Logs, error) {
	logs := plog.NewLogs()
	for a := 0; a < metrics.ResourceMetrics().Len(); a++ {
		resMets := metrics.ResourceMetrics().At(a)

		recLogs := logs.ResourceLogs().AppendEmpty()
		// copy the resource
		metrics.ResourceMetrics().At(a).Resource().CopyTo(recLogs.Resource())
		recLogs.Resource().Attributes().PutStr("EventSource", "metrics")
		recLogs.Resource().Attributes().PutStr("GUID", ulid.Make().String()) // kafka exporter uses resource attributes as key, so give it something to break that up
		scopeLogs := recLogs.ScopeLogs().AppendEmpty()
		scope := scopeLogs.Scope()
		if resMets.ScopeMetrics().Len() == 1 {
			scopeMets := resMets.ScopeMetrics().At(0)
			scope.SetName(scopeMets.Scope().Name())
			scope.SetVersion(scopeMets.Scope().Version())
		} else {
			scope.SetName("github.com/open-telemetry/opentelemetry-collector-contrib/connector/metricstoeventsconnector")
			scope.SetVersion("development")
			scope.SetName("metricstoeventsconnector")
			scope.SetVersion("0.0.1")
		}
		//conn.mapToLogAttributes(resMets.ScopeMetrics(), scopeLogs.LogRecords())
		logRecordSlice := scopeLogs.LogRecords()
		ms.copyToLogAttributes(resMets.ScopeMetrics(), &logRecordSlice)
	}
	return logs, nil
}
func (ms *MappingStrategy) copyToLogAttributes(metrics pmetric.ScopeMetricsSlice, record *plog.LogRecordSlice) {
	for b := 0; b < metrics.Len(); b++ {
		scopeMets := metrics.At(b)
		logRec := record.AppendEmpty()
		atts := logRec.Attributes()
		for c := 0; c < scopeMets.Metrics().Len(); c++ {
			metric := scopeMets.Metrics().At(c)
			metric.Gauge().DataPoints().At(0).Attributes()
			key := metric.Name()
			ms.logger.Info(fmt.Sprintf("Copying %s to Log Attributes", key))
			var input []any = []any{metric}

			err := atts.PutEmpty(key).FromRaw(input)
			if err != nil {
				ms.logger.Error(fmt.Sprintf("Error copying %s to Log Attributes: %s", key, err.Error()))
			}
		}
	}
}

/*
	This is a more complex mapping strategy that attempts to group metrics with the same Metric attributes under the same

Scope, with that Scope's Attributes holding the relevant Metric attributes. This is useful for grouping metrics that
are related to the same entity, such as a host or a process.
*/
func (ms *MappingStrategy) AttributesToScopeMappingStrategy(metrics pmetric.Metrics) (plog.Logs, error) {
	logs := plog.NewLogs()
	for a := 0; a < metrics.ResourceMetrics().Len(); a++ {
		resMets := metrics.ResourceMetrics().At(a)

		recLogs := logs.ResourceLogs().AppendEmpty()
		// copy the resource
		metrics.ResourceMetrics().At(a).Resource().CopyTo(recLogs.Resource())
		recLogs.Resource().Attributes().PutStr("EventSource", "metrics")
		recLogs.Resource().Attributes().PutStr("GUID", ulid.Make().String())
		// Spin through the datapoints to figure out the scopes we need to create

		scopeLogs := recLogs.ScopeLogs().AppendEmpty()

		scope := scopeLogs.Scope()
		if resMets.ScopeMetrics().Len() == 1 {
			scopeMets := resMets.ScopeMetrics().At(0)
			scope.SetName(scopeMets.Scope().Name())
			scope.SetVersion(scopeMets.Scope().Version())
		} else {
			scope.SetName("github.com/open-telemetry/opentelemetry-collector-contrib/connector/metricstoeventsconnector")
			scope.SetVersion("development")
			scope.SetName("metricstoeventsconnector")
			scope.SetVersion("0.0.1")
		}
		//conn.mapToLogAttributes(resMets.ScopeMetrics(), scopeLogs.LogRecords())
		logRecordSlice := scopeLogs.LogRecords()
		ms.copyToLogAttributes(resMets.ScopeMetrics(), &logRecordSlice)
	}
	return logs, nil
}

func deriveScopeKeys(metrics *pmetric.Metrics) []scopeKey {
	scopeKeys := make(map[scopeKey][]float64, 0)
	for a := 0; a < metrics.ResourceMetrics().Len(); a++ {
		resMets := metrics.ResourceMetrics().At(a)
		for b := 0; b < resMets.ScopeMetrics().Len(); b++ {
			scopeMets := resMets.ScopeMetrics().At(b)
			for c := 0; c < scopeMets.Metrics().Len(); c++ {
				metric := scopeMets.Metrics().At(c)
			}
		}
	}
	return scopeKeys
}

func mapAllAttributesForMetric(metric pmetric.Metric) []scopeKey {
	attributes := make([]scopeKey, 0)
	var datapoints pmetric.NumberDataPointSlice
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		datapoints = metric.Gauge().DataPoints()
	case pmetric.MetricTypeSum:
		datapoints = metric.Sum().DataPoints()
	default:
		datapoints = pmetric.NumberDataPointSlice{}
	}

	for i := 0; i < datapoints.Len(); i++ {
		dp := datapoints.At(i)
		scopeKey := scopeKey{
			Attributes: dp.Attributes().AsRaw(),
			TypeName:   metric.Type().String(),
			UnitName:   metric.Unit(),
		}
		attributes = append(attributes, scopeKey)
	}
	return attributes
}
