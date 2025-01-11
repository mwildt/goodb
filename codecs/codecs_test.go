package codecs

import (
	"github.com/mwildt/goodb/utils/testutils"
	"testing"
)

func TestBase64JsonCodec(t *testing.T) {

	codec := NewBase64JsonCodec[string]()
	enc, err := codec.Encode("TEST")
	testutils.AssertNoError(t, err, "FEHLER")
	testutils.Assert(t, string(enc) == "IlRFU1Qi", "expected base 64 ecodev value, bu got %s", string(enc))

	decoded, err := codec.Decode(enc)
	testutils.AssertNoError(t, err, "FEHLER")
	testutils.Assert(t, decoded == "TEST", "expected decoded value test, bu got %s", decoded)

}
