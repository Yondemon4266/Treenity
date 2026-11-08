package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
)

// ---- Text mode: key:body (subject VIII.2) ----

func TestParseTextMessage(t *testing.T) {
	cases := []struct {
		name    string
		line    string
		key     string
		value   string
		wantErr bool
	}{
		{"with newline", "user.create:{\"age\":25}\n", "user.create", "{\"age\":25}", false},
		{"without newline (last line)", "k:v", "k", "v", false},
		{"windows newline", "k:v\r\n", "k", "v", false},
		{"colon inside the body", "k:a:b", "k", "a:b", false},
		{"exactly 1024 bytes", "k:" + strings.Repeat("v", MaxPayload-1), "k", strings.Repeat("v", MaxPayload-1), false},
		{"1025 bytes", "k:" + strings.Repeat("v", MaxPayload), "", "", true},
		{"no colon", "just text\n", "", "", true},
		{"empty key", ":v\n", "", "", true},
		{"empty body", "k:\n", "", "", true},
		{"empty line", "\n", "", "", true},
	}
	for _, c := range cases {
		key, value, err := ParseTextMessage(c.line)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: err = %v, wantErr %v", c.name, err, c.wantErr)
			continue
		}
		if !c.wantErr && (string(key) != c.key || string(value) != c.value) {
			t.Errorf("%s: got %q:%q, want %q:%q", c.name, key, value, c.key, c.value)
		}
	}
}

func TestFormatTextMessage(t *testing.T) {
	if got := FormatTextMessage([]byte("user.create"), []byte(`{"age":25}`)); got != "user.create:{\"age\":25}\n" {
		t.Errorf("got %q", got)
	}
	// What the consumer prints, the producer can read back.
	key, value, err := ParseTextMessage(FormatTextMessage([]byte("k"), []byte("a:b")))
	if err != nil || string(key) != "k" || string(value) != "a:b" {
		t.Errorf("round trip: %q:%q, %v", key, value, err)
	}
}

// ---- Raw mode (subject VIII.2) ----
// Producer input:  [keysize:int32][key][valuesize:int32][value]
// Consumer output: [offset:int32][keysize:int32][key][valuesize:int32][value]
// Sizes are 4-byte little-endian integers.

// le builds the little-endian bytes of an int32, the way the subject describes them.
func le(n int32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(n))
	return b
}

func join(parts ...[]byte) []byte {
	return bytes.Join(parts, nil)
}

func TestWriteRawProducerBytes(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteRawMessage(&buf, Delivery{Key: []byte("ab"), Value: []byte("xyz")}, false); err != nil {
		t.Fatal(err)
	}
	want := join(le(2), []byte("ab"), le(3), []byte("xyz"))
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("got  % x\nwant % x", buf.Bytes(), want)
	}
}

func TestWriteRawConsumerBytes(t *testing.T) {
	var buf bytes.Buffer
	msg := Delivery{Offset: 258, Key: []byte("k"), Value: []byte("v")}
	if err := WriteRawMessage(&buf, msg, true); err != nil {
		t.Fatal(err)
	}
	// 258 = 0x0102: little-endian puts the low byte first.
	want := join([]byte{0x02, 0x01, 0, 0}, le(1), []byte("k"), le(1), []byte("v"))
	if !bytes.Equal(buf.Bytes(), want) {
		t.Errorf("got  % x\nwant % x", buf.Bytes(), want)
	}
}

func TestWriteRawSizeLimit(t *testing.T) {
	var buf bytes.Buffer
	ok := Delivery{Key: []byte("k"), Value: bytes.Repeat([]byte("v"), MaxPayload-1)}
	if err := WriteRawMessage(&buf, ok, false); err != nil {
		t.Errorf("1024 bytes refused: %v", err)
	}
	buf.Reset()
	tooBig := Delivery{Key: []byte("k"), Value: bytes.Repeat([]byte("v"), MaxPayload)}
	if err := WriteRawMessage(&buf, tooBig, false); err == nil {
		t.Error("1025 bytes accepted")
	}
	if buf.Len() != 0 {
		t.Errorf("a refused message still wrote %d bytes", buf.Len())
	}
}

func TestReadRawRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		msg  Delivery
	}{
		{"text", Delivery{Offset: 7, Key: []byte("user.create"), Value: []byte(`{"age":25}`)}},
		{"binary bytes", Delivery{Offset: 0, Key: []byte{0, 255, '\n'}, Value: []byte{':', 0}}},
		{"empty key and value", Delivery{Offset: 1, Key: []byte{}, Value: []byte{}}},
	}
	for _, c := range cases {
		for _, withOffset := range []bool{false, true} {
			var buf bytes.Buffer
			if err := WriteRawMessage(&buf, c.msg, withOffset); err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
			got, err := ReadRawMessage(&buf, withOffset)
			if err != nil {
				t.Errorf("%s (offset=%v): %v", c.name, withOffset, err)
				continue
			}
			wantOffset := int32(0)
			if withOffset {
				wantOffset = c.msg.Offset
			}
			if got.Type != DeliveryMessage || got.Offset != wantOffset ||
				!bytes.Equal(got.Key, c.msg.Key) || !bytes.Equal(got.Value, c.msg.Value) {
				t.Errorf("%s (offset=%v): got %+v", c.name, withOffset, got)
			}
		}
	}
}

// Records are back-to-back, with no separator; a clean EOF after the last
// record is io.EOF.
func TestReadRawBackToBack(t *testing.T) {
	keys := []string{"a", "bb", "ccc"}
	var buf bytes.Buffer
	for _, k := range keys {
		if err := WriteRawMessage(&buf, Delivery{Key: []byte(k), Value: []byte("v")}, false); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range keys {
		msg, err := ReadRawMessage(&buf, false)
		if err != nil || string(msg.Key) != want {
			t.Fatalf("got %q, %v; want %q", msg.Key, err, want)
		}
	}
	if _, err := ReadRawMessage(&buf, false); err != io.EOF {
		t.Errorf("after the last record: got %v, want io.EOF", err)
	}
}

// A partial record at EOF is a producer error (subject VIII.2): ReadRawMessage
// must not answer io.EOF, which the producer takes for the normal end of input.
func TestReadRawPartialRecord(t *testing.T) {
	full := join(le(3), []byte("key"), le(5), []byte("value"))
	withOffset := append(le(9), full...)

	cases := []struct {
		name       string
		data       []byte
		withOffset bool
	}{
		{"half of keysize", full[:2], false},
		{"keysize, then EOF", full[:4], false},
		{"half of the key", full[:5], false},
		{"key, then EOF before valuesize", full[:7], false},
		{"half of valuesize", full[:9], false},
		{"valuesize, then EOF", full[:11], false},
		{"half of the value", full[:13], false},
		{"consumer: half of offset", withOffset[:2], true},
		{"consumer: offset, then EOF", withOffset[:4], true},
	}
	for _, c := range cases {
		_, err := ReadRawMessage(bytes.NewReader(c.data), c.withOffset)
		if err == nil || errors.Is(err, io.EOF) {
			t.Errorf("%s (%d bytes): got %v, want an error that is not io.EOF", c.name, len(c.data), err)
		}
	}
}

// Sizes come from stdin: a negative one must give an error, not a panic.
func TestReadRawNegativeSize(t *testing.T) {
	cases := []struct {
		name string
		data []byte
	}{
		{"negative keysize", join(le(-1), []byte("key"), le(1), []byte("v"))},
		{"negative valuesize", join(le(1), []byte("k"), le(-5), []byte("value"))},
	}
	for _, c := range cases {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: panic: %v", c.name, r)
				}
			}()
			if _, err := ReadRawMessage(bytes.NewReader(c.data), false); err == nil {
				t.Errorf("%s: no error", c.name)
			}
		}()
	}
}
