package skiplist

import (
	"github.com/mwildt/goodb/utils/testutils"
	"testing"
)

func TestAutoadjust(t *testing.T) {
	sl := NewSkipList[int, string]()
	for i := 0; i < 511; i++ {
		sl.Set(i, "xx")
	}
	testutils.Assert(t, sl.level == 8, "höhe sollte jetzt 8 sein, aber war %d", sl.level)

	sl.Set(512, "xx")
	testutils.Assert(t, sl.level == 9, "höhe sollte jetzt 9 sein, aber war %d", sl.level)

	for i := 0; i < 255; i++ {
		sl.Delete(i)
	}
	testutils.Assert(t, sl.level == 9, "höhe sollte jetzt noch 9 sein, aber war %d", sl.level)

	sl.Delete(256)
	testutils.Assert(t, sl.level == 8, "höhe sollte jetzt 8 sein, aber war %d", sl.level)

}

func TestCrud_Smoketest(t *testing.T) {
	sl := NewSkipList[int, string]()

	sl.Set(1, "A 1")
	sl.Set(90, "A 90")
	sl.Set(0, "A 0")
	sl.Set(20, "A 20")
	sl.Set(30, "A 30")
	sl.Set(50, "A 50")
	sl.Set(50, "A 50")
	sl.Set(50, "A 50")
	sl.Set(60, "A 60")
	sl.Set(70, "A 70")
	sl.Set(80, "A 80")
	sl.Set(0, "A 0")
}

func TestGetOnEmty(t *testing.T) {
	sl := NewSkipList[int, string]()

	i := 0
	for range sl.Keys() {
		i++
	}

	v := 0
	for range sl.Values() {
		v++
	}
	if v > 0 || i > 0 {
		t.Errorf("values oder keys nicht leer")
	}
	if _, found := sl.Get(0); found {
		t.Errorf("get auch nicht leer ")
	}
}

func TestGet(t *testing.T) {
	sl := NewSkipList[int, string]()
	sl.Set(10, "10")
	sl.Set(20, "20")

	_, found := sl.Get(0)
	testutils.Assert(t, !found, "found absent key")

	value, found := sl.Get(10)
	testutils.Assert(t, found, "not found existing key (10)")
	testutils.Assert(t, value == "10", "go wrong value, expected 1ß but got %s", value)

	value, found = sl.Get(20)
	testutils.Assert(t, found, "not found existing key (20)")
	testutils.Assert(t, value == "20", "go wrong value, expected 20 but got %s", value)

	_, found = sl.Get(21)
	testutils.Assert(t, !found, "found absent key (21)")
	_, found = sl.Get(15)
	testutils.Assert(t, !found, "found absent key (21)")

	sl.Delete(10)
	_, found = sl.Get(10)
	testutils.Assert(t, !found, "found deleted key (21)")
}

func TestSetDoesNotDuplicate(t *testing.T) {
	sl := NewSkipList[int, string]()
	sl.Set(10, "AA")
	sl.Set(10, "BB")

	value, found := sl.Get(10)
	testutils.Assert(t, found, "not found existing key (10)")
	testutils.Assert(t, value == "BB", "go wrong value, expected BB but got %s", value)
	testutils.Assert(t, sl.Size() == 1, "is not sigleton, but has site %d", sl.size)

}

func TestSkipList_Insert(t *testing.T) {
	sl := NewSkipList[int, string]()
	sl.Set(10, "10")
	sl.Set(20, "20")
	sl.Set(15, "15")
	sl.Set(30, "30")
	sl.Set(5, "5")
	sl.Set(80, "80")
	sl.Set(-1, "-1")
	sl.Set(0, "0")

	testutils.Assert(t, sl.level == 3, "level nicht korrekt, was %d (%d)", sl.level)
	testutils.Assert(t, len(sl.head.next) == 3, "level in erstem node nicht korrekt, was %d (%d)", len(sl.head.next))

	expected := []int{-1, 0, 5, 10, 15, 20, 30, 80}
	i := 0
	for key := range sl.Keys() {
		testutils.Assert(t, expected[i] == key, "falsche reihenfolge an stelle %d. %d erwartet, aber %d bekommen", i, expected[i], key)
		i++
	}

	expectedValues := []string{"-1", "0", "5", "10", "15", "20", "30", "80"}
	i = 0
	for value := range sl.Values() {
		testutils.Assert(t, expectedValues[i] == value, "falsche reihenfolge an stelle %d. %s erwartet, aber %s bekommen", i, expectedValues[i], value)
		i++
	}
}

func TestSkipList_find(t *testing.T) {
	sl := NewSkipList[int, string]()
	sl.Set(10, "ZEHN")
	sl.Set(20, "ZWANZIG")
	sl.Set(15, "fünfzehn")
	sl.Set(30, "3zig")
	sl.Set(40, "4zig")
	sl.Set(5, "fünf")
	sl.Set(80, "80")
	sl.Set(-1, "neg")
	sl.Set(0, "ZERO")
	if v, found := sl.Get(30); !found || v != "3zig" {
		t.Errorf("30 nicht gefunden")
	}

	if _, found := sl.Get(31); found {
		t.Errorf("31 gefunden")
	}
}

func TestSkipList_Delete(t *testing.T) {
	sl := NewSkipList[int, string]()
	sl.Set(10, "10")
	sl.Set(20, "20")
	sl.Set(15, "15")
	sl.Delete(100)
	sl.Set(30, "30")
	sl.Set(5, "5")
	sl.Set(80, "80")
	sl.Delete(30)
	sl.Set(90, "90")
	sl.Delete(15)
	sl.Set(-1, "-1")
	sl.Set(0, "0")

	testutils.Assert(t, len(sl.head.next) == 2, "level in erstem node nicht korrekt, was %d", len(sl.head.next))
	expected := []int{-1, 0, 5, 10, 20, 80, 90}
	i := 0
	for key := range sl.Keys() {
		if expected[i] != key {
			t.Errorf("falsche reihenfolge an stelle %d. %d erwartet, aber %d bekommen", i, expected[i], key)
		}
		i++
	}

	expectedValues := []string{"-1", "0", "5", "10", "20", "80", "90"}
	i = 0
	for value := range sl.Values() {
		if expectedValues[i] != value {
			t.Errorf("falsche reihenfolge an stelle %d. %s erwartet, aber %s bekommen", i, expectedValues[i], value)
		}
		i++
	}
}

func TestSkipList_Delete_Only(t *testing.T) {
	sl := NewSkipList[int, string]()
	sl.Set(10, "10")
	sl.Delete(10)

	if sl.head != nil {
		t.Errorf("fehler")
	}
}

func TestSkipList_Delete_First(t *testing.T) {
	sl := NewSkipList[int, string]()
	sl.Set(20, "20")
	sl.Set(10, "10")
	sl.Delete(10)

	if sl.head == nil {
		t.Errorf("fehler empty")
	}
	if sl.head.key != 20 {
		t.Errorf("fehler value")
	}
}

func TestSkipList_Delete_First2(t *testing.T) {
	sl := NewSkipList[int, string]()
	sl.Set(10, "10")
	sl.Set(20, "20")
	sl.Delete(10)

	if sl.head == nil {
		t.Errorf("fehler empty")
	}
	if sl.head.key != 20 {
		t.Errorf("fehler value")
	}
}
