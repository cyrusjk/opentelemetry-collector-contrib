package metricstoeventsconnector // import "github.com/open-telemetry/opentelemetry-collector-contrib/connector/metricstoeventconnector"

/*

This is an attempt to reconcile the differences in how the Open Telemetry model is applied and how the data it generates
is submitted to the SignalFx backend. The Open Telemetry model is based on separate pipelines for Spans, Metrics, Logs
and Traces. The SignalFx API is based on the concept of an Open Telemetry "Event" which is a combination of Metrics and
Logs into a single Logs record, applying the Metrics as Attributes to the Logs record. This connector is intended to
bridge the gap between the Open Telemetry model and the SignalFx API by converting Metrics data into Logs data and
submitting it to the Logs pipeline.

In the use cases this is designed for, Metrics data is derived form RDBMS query performance data, and Logs data contains
corresponding query and query plan text. The Metrics and Log data are emitted on their respective pipelines, but the
Logs data is emitted based on what is selected to emit from the Metrics data. This connector is intended to map the
Metrics data to the Logs data, and emit the Logs data to the Logs pipeline.

There are a few approaches to this, but the most straightforward is to cache the Metrics data and use it to map to the
Logs data. This is because the Metrics data will be emitted before the Logs data, so the Logs data can be emitted based
on the cached Metrics data. The Metrics data will be cached based on a hash value that is emitted with the Metrics data
and used to map to the Logs data.

Another approach is to not cache anything and emit Log data as-is and map metrics data to its own Log record. This avoids
the need for a cache and sends data as fast as possible.


*/

import (
	"context"
	"fmt"
	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"
	"time"

	"github.com/patrickmn/go-cache"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"golang.org/x/exp/maps"
)

var hashCache = cache.New(6*time.Hour, 12*time.Hour)

type metricsToLogsConnector struct {
	config          Config
	metricsConsumer consumer.Logs // the next component in the pipeline to ingest metrics after connector
	logger          *zap.Logger
	component.StartFunc
	component.ShutdownFunc
}

func newMetricsToLogsConnector(logger *zap.Logger, config component.Config) *metricsToLogsConnector {
	cfg, _ := config.(*Config)

	return &metricsToLogsConnector{
		config: *cfg,
		logger: logger,
	}
}

// Capabilities implements the consumer interface.
func (c *metricsToLogsConnector) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeMetrics Consume Metrics and map them to a Log record, emitting them to the Logs pipeline
// This will need to either cache the metrics data or rely on cached logs data to map to logs with this metrics data.
// Given that Logs data will arrive after Metrics data, the former approach is likely the best.
func (conn *metricsToLogsConnector) ConsumeMetrics(ctx context.Context, ms pmetric.Metrics) error {

	logs := plog.NewLogs()
	for a := 0; a < ms.ResourceMetrics().Len(); a++ {
		resMets := ms.ResourceMetrics().At(a)

		recLogs := logs.ResourceLogs().AppendEmpty()
		// copy the resource
		ms.ResourceMetrics().At(a).Resource().CopyTo(recLogs.Resource())
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
		// Map Metrics to Logs Attributes.
		// Note that significant fidelity is lost here as all data for a metric is reduced to a single value; dimensions
		// and other Attributes are not preserved. Any work to improve upon this needs to be balanced against the
		// savings of actually handling the data emitted in the original Metrics format.
		//
		// TODO This adaptation is effectively a tumor, and work used to improve it takes away from work to fix the root cause in the SignalFX API and backend
		for b := 0; b < resMets.ScopeMetrics().Len(); b++ {
			scopeMets := resMets.ScopeMetrics().At(b)
			minTime, maxTime := pcommon.NewTimestampFromTime(time.Now().Add(time.Hour)), pcommon.NewTimestampFromTime(time.Now().Add(time.Hour*-1))
			// skip scope metrics, we aren't using them
			// but we ARE using a Log to map the metrics to
			logRec := scopeLogs.LogRecords().AppendEmpty()
			atts := logRec.Attributes()
			conn.logger.Info(fmt.Sprintf("Mapping %d metrics", scopeMets.Metrics().Len()))
			for c := 0; c < scopeMets.Metrics().Len(); c++ {
				metric := scopeMets.Metrics().At(c)
				var minT = pcommon.NewTimestampFromTime(time.Now().Add(time.Hour))
				var maxT = pcommon.NewTimestampFromTime(time.Now().Add(time.Hour * -1))
				var attributesMap map[string]any = nil
				var dbl float64 = 0
				switch metric.Type() {
				case pmetric.MetricTypeSum:
					// Do we care if it's a counter or not?
					dps := metric.Sum().DataPoints()
					dbl, minT, maxT = conn.appendMetricDataPoints(&dps)
					attributesMap = extractAttributes(&dps)
				case pmetric.MetricTypeGauge:
					dps := metric.Gauge().DataPoints()
					dbl, minT, maxT = conn.appendMetricDataPoints(&dps)
					attributesMap = extractAttributes(&dps)
				case pmetric.MetricTypeSummary:
					dps := metric.Summary().DataPoints()
					for d := 0; d < dps.Len(); d++ {
						dp := dps.At(d)
						dbl = dbl + dp.Sum()
						if minT > dp.Timestamp() {
							minT = dp.Timestamp()
						}
						if maxT < dp.Timestamp() {
							maxT = dp.Timestamp()
						}
					}
					attributesMap = extractSummaryAttributes(&dps)
				default:
					conn.logger.Debug(fmt.Sprintf("Skipping mapping of %s;  %t metric type is not yet supported", metric.Name(), metric.Type()))
					continue
				}
				if minTime >= minT {
					minTime = minT
				}
				if maxTime <= maxT {
					maxTime = maxT
				}
				atts.PutDouble(metric.Name(), dbl)
				// This is one possible way to retain Metric Attributes in the Log record, and other strategies may be added as options later
				if attributesMap != nil {
					for k, v := range attributesMap {
						atts.PutStr(metric.Name()+"."+k, v.(pcommon.Value).Str())
					}
				}
				atts.PutStr(metric.Name()+".metric.type", metric.Type().String())
				atts.PutStr(metric.Name()+".metric.unit", metric.Unit())
			}
			// Not sure what to do with/about different time stamps in metrics. It is assumed that all timestamps will be the same for a metrics set, but that is in no way guaranteed, and we need to acknowledge that there may be a gap
			logRec.SetTimestamp(minTime)
			if minTime < maxTime {
				conn.logger.Warn(fmt.Sprintf("Metrics with differing times observed in the same metrics scope; diff of %d ms.", maxTime.AsTime().UnixMilli()-minTime.AsTime().UnixMilli()))
			}
		}
	}
	return conn.metricsConsumer.ConsumeLogs(ctx, logs)
}

func extractAttributes(dps *pmetric.NumberDataPointSlice) map[string]any {
	atts := make(map[string]any)
	for i := 0; i < dps.Len(); i++ {
		keys := maps.Keys(dps.At(i).Attributes().AsRaw())
		for _, k := range keys {
			atts[k], _ = dps.At(i).Attributes().Get(k)
		}
	}
	return atts
}

func extractSummaryAttributes(dps *pmetric.SummaryDataPointSlice) map[string]any {
	atts := make(map[string]any)
	for i := 0; i < dps.Len(); i++ {
		keys := maps.Keys(dps.At(i).Attributes().AsRaw())
		for _, k := range keys {
			atts[k], _ = dps.At(i).Attributes().Get(k)
		}
	}
	return atts
}

// appendMetricDataPoints this takes all the provided data point in the provided slice and adds up the values while also determining the
// min and max timestamps. All three are then returned.
func (conn *metricsToLogsConnector) appendMetricDataPoints(dps *pmetric.NumberDataPointSlice) (float64, pcommon.Timestamp, pcommon.Timestamp) {

	minT, maxT := pcommon.NewTimestampFromTime(time.Now().Add(time.Hour)), pcommon.NewTimestampFromTime(time.Now().Add(time.Hour*-1))

	dbl := float64(0)
	for d := 0; d < dps.Len(); d++ {
		dp := dps.At(d)
		if minT < dp.Timestamp() {
			minT = dp.Timestamp()
		}
		if maxT > dp.Timestamp() {
			maxT = dp.Timestamp()
		}
		dbl = dbl + dp.DoubleValue()
	}
	return dbl, minT, maxT
}

func addToCache(key string, metrics pmetric.ResourceMetrics) {
	mets, success := hashCache.Get(key)
	if !success {
		mets = make([]pmetric.ResourceMetrics, 0, 10)
		hashCache.Set(key, mets, 60*60*12) // 12 hours
	}
	mets = append(mets.([]pmetric.ResourceMetrics), metrics)
	hashCache.Set(key, mets, 60*60*12) // 12 hours
}
