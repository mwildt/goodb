package messagelog

import (
	"context"
	"github.com/mwildt/goodb/utils/testutils"

	"path"
	"testing"
)

func TestMessageLog_Append(t *testing.T) {

	testutils.RunWithTempDir("testdata", func(dir string) {
		if log, err := NewMessageLog[string](path.Join(dir, "testlog.data")); err != nil {
			t.Fatalf(err.Error())
		} else {
			readCount, err := log.Open(func(_ context.Context, message string) error {
				t.Errorf("consumer should not be called in the 1st time")
				return nil
			})
			testutils.AssertNoError(t, err, "fehler beim öffnen")
			testutils.Assert(t, readCount == 0, "expected read count to be 0, but was %d", readCount)
			log.Append(context.Background(), "Hello")
			log.Append(context.Background(), "World")
			log.Close()
		}

		if log, err := NewMessageLog[string](path.Join(dir, "testlog.data")); err != nil {
			t.Fatalf(err.Error())
		} else {
			messages := make([]string, 0)
			readCount, _ := log.Open(func(_ context.Context, message string) error {
				messages = append(messages, message)
				return nil
			})

			testutils.AssertNoError(t, err, "fehler beim öffnen")
			testutils.Assert(t, readCount == 2, "expected read count to be 2, but was %d", readCount)
			testutils.Assert(t, len(messages) == 2, "keine 2 Nachrichten")
			testutils.Assert(t, string(messages[0]) == "Hello", "Hello erwartet")
			testutils.Assert(t, string(messages[1]) == "World", "World erwartet")

			log.Close()
		}
	})

}
