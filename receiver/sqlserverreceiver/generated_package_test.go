// Code generated by mdatagen. DO NOT EDIT.

package sqlserverreceiver

import (
	"go.uber.org/goleak"
	"testing"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreAnyFunction("github.com/godbus/dbus.(*Conn).inWorker"))
}
