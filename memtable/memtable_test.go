package memtable

import (
	"context"
	"github.com/mwildt/goodb/utils/testutils"
	"testing"
)

func TestCreateMemtable(t *testing.T) {
	testutils.RunWithTempDir("TestCreateMemtable", func(dir string) {
		mt, err := CreateMemtable[int, string](dir, "testmt")
		testutils.AssertNoError(t, err, "Fehler beim erstellen der memtable")
		_, found := mt.Get(1)
		testutils.Assert(t, !found, "key 1 found")

		mt.Set(context.Background(), 1, "eins")
		mt.Set(context.Background(), 2, "zwei")
		mt.Set(context.Background(), 5, "fünf")
		testutils.Assert(t, 3 == mt.Size(), "falsche größe")

		v1, found := mt.Get(1)
		testutils.Assert(t, found, "key 1 found")
		testutils.Assert(t, v1 == "eins", "value 1 is not eins")

		del, err := mt.Delete(context.Background(), 0)
		testutils.AssertNoError(t, err, "Fehler beim löschen von 0")
		testutils.Assert(t, !del, "item deleted obwohl nicht vorhanden")
		testutils.Assert(t, 3 == mt.Size(), "falsche größe")

		del, err = mt.Delete(context.Background(), 2)
		testutils.Assert(t, del, "item anscheinend deleted")
		testutils.Assert(t, 2 == mt.Size(), "größe nicht reduziert")
		mt.Close()

		reopend, err := CreateMemtable[int, string](dir, "testmt")
		testutils.AssertNoError(t, err, "Fehler beim erstellen der memtable")
		testutils.Assert(t, 2 == mt.Size(), "größe nicht reduziert")

		v1, found = reopend.Get(1)
		testutils.Assert(t, found, "key 1 found")
		testutils.Assert(t, v1 == "eins", "value 1 is not eins")
		reopend.Close()
	})
}

func TestUpdateLastElement(t *testing.T) {
	testutils.RunWithTempDir("TestUpdateLastElement", func(dir string) {
		mt, err := CreateMemtable[int, string](dir, "testmt")
		testutils.AssertNoError(t, err, "Fehler beim erstellen der memtable")
		mt.Set(context.Background(), 1, "A 1")
		mt.Set(context.Background(), 2, "A 2")
		mt.Set(context.Background(), 2, "B 2")
		mt.Close()
	})
}

func TestRepoensMemtable(t *testing.T) {
	testutils.RunWithTempDir("TestRepoensMemtable", func(dir string) {
		mt, err := CreateMemtable[int, string](dir, "testmt")
		testutils.AssertNoError(t, err, "Fehler beim erstellen der memtable")

		mt.Set(context.Background(), 1, "A 1")
		mt.Set(context.Background(), 2, "A 2")
		mt.Set(context.Background(), 99, "A 99")
		mt.Set(context.Background(), 5, "A 5")
		mt.Delete(context.Background(), 1)
		mt.Delete(context.Background(), 2)
		mt.Set(context.Background(), 5, "B 5")
		mt.Set(context.Background(), 1, "D 1")
		mt.Set(context.Background(), 99, "B 99")
		mt.Set(context.Background(), 2, "D 2")
		mt.Delete(context.Background(), 5)
		mt.Set(context.Background(), 99, "C 99")
		mt.Close()

		reopend, err := CreateMemtable[int, string](dir, "testmt")
		value, _ := reopend.Get(1)
		testutils.Assert(t, value == "D 1", "value 1 is not D 1, but %s", value)

		value, _ = reopend.Get(2)
		testutils.Assert(t, value == "D 2", "value 2 is not D 2, but %s", value)

		value, found := reopend.Get(5)
		testutils.Assert(t, !found, "found unexpected key 5")

		value, _ = reopend.Get(99)
		testutils.Assert(t, value == "C 99", "value 99 is not C 99, but %s", value)
		reopend.Close()
	})

}
