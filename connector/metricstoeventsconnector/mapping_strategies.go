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
		This strategy simply serializes the metrics as JSON to the log body. This is more in compliance with the OpenTelemetry
	    Event format. It is useful for debugging and testing, but it may not be suitable for production use cases.
*/
func (ms *MappingStrategy) MetricsToLogBody(metrics pmetric.Metrics) (plog.Logs, error) {
	marshaler := &pmetric.JSONMarshaler{}
	logs := plog.NewLogs()
	reclogs := logs.ResourceLogs()
	recLogs := reclogs.AppendEmpty()
	// copy the resource
	metrics.ResourceMetrics().At(0).Resource().CopyTo(recLogs.Resource())
	recLogs.Resource().Attributes().PutStr("EventSource", "metrics")
	recLogs.Resource().Attributes().PutStr("GUID", ulid.Make().String()) // kafka exporter uses resource attributes as key, so give it something to break that up
	scopeLogs := recLogs.ScopeLogs().AppendEmpty()
	logRec := scopeLogs.LogRecords().AppendEmpty()
	jsonMetrics, err := marshaler.MarshalMetrics(metrics)
	if err != nil {
		ms.logger.Error(fmt.Sprintf("Error marshaling metrics to JSON: %s", err.Error()))
		return logs, err
	}
	logRec.Body().SetStr(string(jsonMetrics))

	return logs, nil
}
