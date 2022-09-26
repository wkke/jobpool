package structs

import (
	"bytes"
	"github.com/hashicorp/go-msgpack/codec"
	"reflect"
	"time"
	"yunli.com/jobpool/core/constant"
)



// DrainSpec describes a Node's desired drain behavior.
type DrainSpec struct {
	// Deadline is the duration after StartTime when the remaining
	// allocations on a draining Node should be told to stop.
	Deadline time.Duration

	// IgnoreSystemJobs allows systems jobs to remain on the node even though it
	// has been marked for draining.
	IgnoreSystemJobs bool
}

// DrainStrategy describes a Node's drain behavior.
type DrainStrategy struct {
	// DrainSpec is the user declared drain specification
	DrainSpec

	// ForceDeadline is the deadline time for the drain after which drains will
	// be forced
	ForceDeadline time.Time

	// StartedAt is the time the drain process started
	StartedAt time.Time
}

func (d *DrainStrategy) Copy() *DrainStrategy {
	if d == nil {
		return nil
	}

	nd := new(DrainStrategy)
	*nd = *d
	return nd
}

// DeadlineTime returns a boolean whether the drain strategy allows an infinite
// duration or otherwise the deadline time. The force drain is captured by the
// deadline time being in the past.
func (d *DrainStrategy) DeadlineTime() (infinite bool, deadline time.Time) {
	// Treat the nil case as a force drain so during an upgrade where a node may
	// not have a drain strategy but has Drain set to true, it is treated as a
	// force to mimick old behavior.
	if d == nil {
		return false, time.Time{}
	}

	ns := d.Deadline.Nanoseconds()
	switch {
	case ns < 0: // Force
		return false, time.Time{}
	case ns == 0: // Infinite
		return true, time.Time{}
	default:
		return false, d.ForceDeadline
	}
}

func (d *DrainStrategy) Equal(o *DrainStrategy) bool {
	if d == nil && o == nil {
		return true
	} else if o != nil && d == nil {
		return false
	} else if d != nil && o == nil {
		return false
	}

	// Compare values
	if d.ForceDeadline != o.ForceDeadline {
		return false
	} else if d.Deadline != o.Deadline {
		return false
	} else if d.IgnoreSystemJobs != o.IgnoreSystemJobs {
		return false
	}

	return true
}

// msgpackHandle is a shared handle for encoding/decoding of structs
var MsgpackHandle = func() *codec.MsgpackHandle {
	h := &codec.MsgpackHandle{}
	h.RawToString = true

	// maintain binary format from time prior to upgrading latest ugorji
	h.BasicHandle.TimeNotBuiltin = true

	// Sets the default type for decoding a map into a nil interface{}.
	// This is necessary in particular because we store the driver configs as a
	// nil interface{}.
	h.MapType = reflect.TypeOf(map[string]interface{}(nil))

	// only review struct codec tags
	h.TypeInfos = codec.NewTypeInfos([]string{"codec"})

	return h
}()

// Decode is used to decode a MsgPack encoded object
func Decode(buf []byte, out interface{}) error {
	return codec.NewDecoder(bytes.NewReader(buf), MsgpackHandle).Decode(out)
}

// Encode is used to encode a MsgPack object with type prefix
func Encode(t constant.MessageType, msg interface{}) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(uint8(t))
	err := codec.NewEncoder(&buf, MsgpackHandle).Encode(msg)
	return buf.Bytes(), err
}
