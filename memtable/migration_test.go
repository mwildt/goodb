package memtable

import (
	"context"
	"github.com/mwildt/goodb/codecs"
	"github.com/mwildt/goodb/utils/testutils"
	"testing"
)

type DataV1 struct {
	Name string
}

type DataV2 struct {
	Name   string
	Length int
}

type DataV3 struct {
	Name   string
	Length int
	Double int
}

func TestNewMigrationManager(t *testing.T) {
	testutils.RunWithTempDir("TestNewMigrationManager", func(dir string) {
		collection := "testmt"
		mt, err := CreateMemtable[int, DataV1](collection, WithDatadir(dir))
		testutils.AssertNoError(t, err, "Fehler beim erstellen der memtable")

		mt.Set(context.Background(), 1, DataV1{Name: "eins"})
		mt.Set(context.Background(), 2, DataV1{Name: "eins."})
		mt.Set(context.Background(), 3, DataV1{Name: "eins.."})
		mt.Close()

		migman, err := NewMigrationManager[int, map[string]interface{}](collection, mt.frs, codecs.NewJsonCodec[map[string]interface{}](), Migration[map[string]interface{}]{
			Name:    "demo",
			Version: "V__1",
			Handler: func(obj map[string]interface{}) (map[string]interface{}, error) {
				obj["Length"] = len(obj["Name"].(string))
				return obj, nil
			},
		}, Migration[map[string]interface{}]{
			Name:    "demo-2",
			Version: "V__2",
			Handler: func(obj map[string]interface{}) (map[string]interface{}, error) {
				obj["Double"] = (obj["Length"].(int)) * 2
				return obj, nil
			},
		})
		testutils.AssertNoError(t, err, "Fehler beim erstellen des migrationmanager")
		migman.migrate(context.Background())

		reopend, err := CreateMemtable[int, DataV3](collection, WithDatadir(dir))
		testutils.AssertNoError(t, err, "Fehler beim erstellen der memtable")
		testutils.Assert(t, 3 == mt.Size(), "wrong size, expected 3, but got %d", mt.Size())
		v3, found := reopend.Get(3)
		testutils.Assert(t, found, "key 3 was not found found")
		testutils.Assert(t, v3.Length == 6, "Length is expected to be 6, but was %d", v3.Length)
		testutils.Assert(t, v3.Double == 12, "Double is expected to be  6, but was %d", v3.Length)

		reopend.Close()
	})
}
