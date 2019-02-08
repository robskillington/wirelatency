package wirelatency

import (
	"bytes"
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

	buf := proto.NewBuffer(nil)
	n := 42
	for i := 0; i < n; i++ {
		err := f.Encode(buf)
		if err != nil {
			t.Fatalf("failed to encode buffer: %v", err)
		}
	}

	// New buffer with the contents of the current buffer
	buf = proto.NewBuffer(buf.Bytes())
	for i := 0; i < n; i++ {
		d := &CassandraQuery{}
		err := d.Decode(buf)
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
