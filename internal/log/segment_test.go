package log

import (
	api "github.com/Lyr-a-Brode/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, err := ioutil.TempDir("", "segment-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	const baseOffset = uint64(16)

	s, err := newSegment(dir, baseOffset, c)
	require.NoError(t, err)
	require.Equal(t, baseOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, baseOffset+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	require.True(t, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, baseOffset, c)
	require.NoError(t, err)

	require.True(t, s.IsMaxed())

	require.NoError(t, s.Remove())
	s, err = newSegment(dir, baseOffset, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
