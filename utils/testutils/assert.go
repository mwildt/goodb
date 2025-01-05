package testutils

import (
	"runtime"
	"testing"
)

func AssertNoError(t *testing.T, err error, template string, args ...any) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		argv := append([]any{file, line, err}, args...)
		t.Errorf("file://%s:%d [%w] "+template, argv...)
	}
}

func Assert(t *testing.T, condition bool, template string, args ...any) {

	if !condition {
		_, file, line, _ := runtime.Caller(1)
		argv := append([]any{file, line}, args...)
		t.Errorf("file://%s:%d "+template, argv...)
	}

}
