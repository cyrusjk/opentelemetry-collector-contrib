//go:generate mdatagen metadata.yaml
package metricstoeventsconnector

import (
	"context"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

const (
	typeStr = "metricstoeventsconnector"
)

func NewFactory() connector.Factory {
	myType, _ := component.NewType(typeStr)
	return connector.NewFactory(
		myType,
		CreateDefaultConfig,
		connector.WithMetricsToLogs(createMetricsToLogsConnector, component.StabilityLevelAlpha),
	)
}

func CreateDefaultConfig() component.Config {
	return &Config{}

}

func createMetricsToLogsConnector(_ context.Context, params connector.Settings, cfg component.Config, nextConsumer consumer.Logs) (connector.Metrics, error) {
	conn := newMetricsToLogsConnector(params.Logger, cfg)
	conn.metricsConsumer = nextConsumer
	return conn, nil
}
