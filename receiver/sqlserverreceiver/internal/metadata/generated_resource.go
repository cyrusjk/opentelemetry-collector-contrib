// Code generated by mdatagen. DO NOT EDIT.

package metadata

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// ResourceBuilder is a helper struct to build resources predefined in metadata.yaml.
// The ResourceBuilder is not thread-safe and must not to be used in multiple goroutines.
type ResourceBuilder struct {
	config ResourceAttributesConfig
	res    pcommon.Resource
}

// NewResourceBuilder creates a new ResourceBuilder. This method should be called on the start of the application.
func NewResourceBuilder(rac ResourceAttributesConfig) *ResourceBuilder {
	return &ResourceBuilder{
		config: rac,
		res:    pcommon.NewResource(),
	}
}

// SetServerAddress sets provided value as "server.address" attribute.
func (rb *ResourceBuilder) SetServerAddress(val string) {
	if rb.config.ServerAddress.Enabled {
		rb.res.Attributes().PutStr("server.address", val)
	}
}

// SetServerPort sets provided value as "server.port" attribute.
func (rb *ResourceBuilder) SetServerPort(val int64) {
	if rb.config.ServerPort.Enabled {
		rb.res.Attributes().PutInt("server.port", val)
	}
}

// SetSqlserverComputerName sets provided value as "sqlserver.computer.name" attribute.
func (rb *ResourceBuilder) SetSqlserverComputerName(val string) {
	if rb.config.SqlserverComputerName.Enabled {
		rb.res.Attributes().PutStr("sqlserver.computer.name", val)
	}
}

// SetSqlserverDatabaseName sets provided value as "sqlserver.database.name" attribute.
func (rb *ResourceBuilder) SetSqlserverDatabaseName(val string) {
	if rb.config.SqlserverDatabaseName.Enabled {
		rb.res.Attributes().PutStr("sqlserver.database.name", val)
	}
}

// SetSqlserverInstanceName sets provided value as "sqlserver.instance.name" attribute.
func (rb *ResourceBuilder) SetSqlserverInstanceName(val string) {
	if rb.config.SqlserverInstanceName.Enabled {
		rb.res.Attributes().PutStr("sqlserver.instance.name", val)
	}
}

// SetSqlserverQueryHash sets provided value as "sqlserver.query.hash" attribute.
func (rb *ResourceBuilder) SetSqlserverQueryHash(val string) {
	if rb.config.SqlserverQueryHash.Enabled {
		rb.res.Attributes().PutStr("sqlserver.query.hash", val)
	}
}

// SetSqlserverQueryPlanHash sets provided value as "sqlserver.query_plan.hash" attribute.
func (rb *ResourceBuilder) SetSqlserverQueryPlanHash(val string) {
	if rb.config.SqlserverQueryPlanHash.Enabled {
		rb.res.Attributes().PutStr("sqlserver.query_plan.hash", val)
	}
}

// Emit returns the built resource and resets the internal builder state.
func (rb *ResourceBuilder) Emit() pcommon.Resource {
	r := rb.res
	rb.res = pcommon.NewResource()
	return r
}
