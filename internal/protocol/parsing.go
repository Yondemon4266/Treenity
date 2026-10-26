package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"strings"
)

// ==========================================
// 1. TEXT MODE (Default format: key:body\n)
// ==========================================

// ParseTextMessage analyzes a raw line and extracts key and value as bytes.
func ParseTextMessage(line string) ([]byte, []byte, error) {
	line = strings.TrimRight(line, "\r\n")

	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return nil, nil, errors.New("invalid text format, expected: 'key:body'")
	}
	key := []byte(parts[0])
	value := []byte(parts[1])
	if len(key)+len(value) > MaxPayload {
		return nil, nil, errors.New("key+value is larger than 1024 bytes")
	}

	return key, value, nil
}

// FormatTextMessage converts key and value into text.
func FormatTextMessage(key, value []byte) string {
	return string(key) + ":" + string(value) + "\n"
}

// ==========================================
// 2. BINARY MODE (--raw)
// ==========================================

// WriteRawMessage serializes and write a message.
// - If Offset is asked, offset is written first (for consumer).
// - Else (producer) only [keysize][key][valuesize][value] are written.
func WriteRawMessage(w io.Writer, msg Delivery, includeOffset bool) error {
	if len(msg.Key)+len(msg.Value) > MaxPayload {
		return errors.New("key+value is larger than 1024 bytes")
	}
	if includeOffset {
		if err := binary.Write(w, binary.LittleEndian, msg.Offset); err != nil {
			return err
		}
	}

	keySize := int32(len(msg.Key))
	if err := binary.Write(w, binary.LittleEndian, keySize); err != nil {
		return err
	}

	if keySize > 0 {
		if _, err := w.Write(msg.Key); err != nil {
			return err
		}
	}

	valueSize := int32(len(msg.Value))
	if err := binary.Write(w, binary.LittleEndian, valueSize); err != nil {
		return err
	}

	if valueSize > 0 {
		if _, err := w.Write(msg.Value); err != nil {
			return err
		}
	}

	return nil
}

// ReadRawMessage reads and deserializes binary message.
// At each Read, the cursor increments of the size of the data size
func ReadRawMessage(r io.Reader, includeOffset bool) (Delivery, error) {
	msg := Delivery{
		Type: DeliveryMessage,
	}

	if includeOffset {
		if err := binary.Read(r, binary.LittleEndian, &msg.Offset); err != nil {
			return msg, err
		}
	}

	var keySize int32
	if err := binary.Read(r, binary.LittleEndian, &keySize); err != nil {
		return msg, err
	}

	msg.Key = make([]byte, keySize)
	if keySize > 0 {
		if _, err := io.ReadFull(r, msg.Key); err != nil {
			return msg, err
		}
	}

	var valueSize int32
	if err := binary.Read(r, binary.LittleEndian, &valueSize); err != nil {
		return msg, err
	}

	msg.Value = make([]byte, valueSize)
	if valueSize > 0 {
		if _, err := io.ReadFull(r, msg.Value); err != nil {
			return msg, err
		}
	}

	return msg, nil
}
