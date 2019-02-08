package wirelatency

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
)

func TestCassandraQuery(t *testing.T) {
	f := &CassandraQuery{
		Version:    1,
		ReceivedAt: time.Now(),
		CQL:        "foo",
		Args:       [][]byte{[]byte("bar"), []byte("baz")},
	}

	output := bytes.NewBuffer(nil)
	buf := proto.NewBuffer(nil)
	lengthBuf := make([]byte, 4)

	n := 42
	for i := 0; i < n; i++ {
		buf.Reset()

		err := f.Encode(buf)
		if err != nil {
			t.Fatalf("failed to encode buffer: %v", err)
		}

		bytes := buf.Bytes()

		binary.LittleEndian.PutUint32(lengthBuf[:], uint32(len(bytes)))
		if _, err := output.Write(lengthBuf[:]); err != nil {
			t.Fatalf("failed to write output size: %v", err)
		}

		if _, err := output.Write(buf.Bytes()); err != nil {
			t.Fatalf("failed to write output size: %v", err)
		}
	}

	// New buffer with the contents of the current buffer
	var reuseableBuf []byte
	reader := io.Reader(output)
	for i := 0; i < n; i++ {
		d := &CassandraQuery{}

		// Read the size of the message
		_, err := io.ReadFull(reader, lengthBuf[:])
		if err != nil {
			t.Fatalf("failed to read length: %v", err)
		}

		size := binary.LittleEndian.Uint32(lengthBuf[:])
		if cap(reuseableBuf) < int(size) {
			reuseableBuf = make([]byte, size)
		} else {
			reuseableBuf = reuseableBuf[:size]
		}

		_, err = io.ReadFull(reader, reuseableBuf)
		if err != nil {
			t.Fatalf("failed to read buffer: %v", err)
		}

		// Reset the proto buffer to use the fully read message
		buf.SetBuf(reuseableBuf)

		err = d.Decode(buf)
		if err != nil {
			t.Fatalf("failed to decode buffer: %v", err)
		}

		if d.Version != f.Version {
			t.Fatalf("invalid version: %v", d.Version)
		}

		if !d.ReceivedAt.Equal(f.ReceivedAt) {
			t.Fatalf("invalid received at: %v", d.ReceivedAt.String())
		}

		if d.CQL != f.CQL {
			t.Fatalf("invalid cql: %v", d.CQL)
		}

		if len(d.Args) != len(f.Args) {
			t.Fatalf("invalid len args: %v", len(d.Args))
		}
		for i, arg := range d.Args {
			if !bytes.Equal(arg, f.Args[i]) {
				t.Fatalf("invalid arg at index %d: %v", i, arg)
			}
		}
	}
}
