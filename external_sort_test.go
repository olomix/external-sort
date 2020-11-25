package external_sort

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"math/rand"
	"sort"
	"testing"

	go_fast_sort "github.com/olomix/go-fast-sort"
	"github.com/stretchr/testify/require"
)

func TestOne(t *testing.T) {
	b1 := make([]byte, 3)
	b2 := []byte{1, 2, 3, 4}
	x := copy(b1, b2)
	t.Logf("%v %v %v", x, len(b1), b1)
}

func TestFlush(t *testing.T) {
	r := require.New(t)
	buf := make([]byte, defaultBufSize)
	n, err := rand.Read(buf)
	r.NoError(err)
	r.Equal(n, defaultBufSize)
	s := &Sorter{
		buf:    buf,
		bufIdx: len(buf),
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	}()
	r.Equal(0, s.sizeWritten)
	if err = s.flush(); err != nil {
		if s.tempFile != nil {
			t.Fatalf("failed to flush to file %v: %v", s.tempFile.Name(), err)
		} else {
			t.Fatal(err)
		}
	}
	r.Equal(defaultBufSize, s.sizeWritten)

	offset, err := s.tempFile.Seek(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0 {
		t.Fatalf("expected temp file position is 0, got %v", offset)
	}
	buf2, err := ioutil.ReadAll(s.tempFile)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, buf2) {
		t.Fatal("data in file and in buffer are different")
	}

	if s.bufIdx != 0 {
		t.Fatalf("expected buf pointer 0, got %v", s.bufIdx)
	}
}

func TestSort(t *testing.T) {
	in := []byte{
		0x03, 0x11,
		0x01, 0x12,
		0x03, 0x13,
		0x02, 0x14,
		0x29, 0x28, 0x27, 0x26, // garbage should not be touched
	}
	expected := []byte{
		0x01, 0x12,
		0x02, 0x14,
		0x03, 0x11,
		0x03, 0x13,
		0x29, 0x28, 0x27, 0x26, // garbage should not be touched
	}
	s := &Sorter{
		itemSize: 2,
		lessFn:   func(a, b []byte) bool { return a[0] < b[0] },
		buf:      in,
		bufIdx:   8,
	}
	s.sort()
	if !bytes.Equal(s.buf, expected) {
		t.Fatal(s.buf)
	}
}

type tReaderAtExpectation struct {
	data []byte
	err  error
}

type tReaderCall struct {
	bufLn  int
	offset int64
}

type tReaderAt struct {
	expectations []tReaderAtExpectation
	idx          int
	calls        []tReaderCall
}

func (t *tReaderAt) ReadAt(b []byte, off int64) (int, error) {
	if t.idx >= len(t.expectations) {
		return 0, errors.New("unexpected call to ReadAt")
	}
	t.calls = append(t.calls, tReaderCall{bufLn: len(b), offset: off})
	n := copy(b, t.expectations[t.idx].data)
	err := t.expectations[t.idx].err
	t.idx++
	return n, err
}

func TestBBFill(t *testing.T) {
	tra := &tReaderAt{expectations: []tReaderAtExpectation{
		{data: []byte{1, 2, 3, 4, 5, 6}},
		{data: []byte{7, 8, 9}}, {data: []byte{10, 11, 12}},
		{data: []byte{13, 14}},
	}}
	b, err := newBB(make([]byte, 6), 2, tra, 100, 114)
	r := require.New(t)
	r.NoError(err)
	r.Equal([]tReaderCall{{bufLn: 6, offset: 100}}, tra.calls)
	r.Equal([]byte{1, 2}, b.item())
	r.Equal([]byte{1, 2, 3, 4, 5, 6}, b.buf)
	r.Equal(3, b.bufCap)
	r.Equal(0, b.idx)
	r.Equal(2, b.itemSz)
	r.Equal(106, b.bufStart)
	r.Equal(114, b.bufEnd)

	hasMore, err := b.pop()
	r.NoError(err)
	r.True(hasMore)
	r.Equal([]tReaderCall{{bufLn: 6, offset: 100}}, tra.calls)
	r.Equal([]byte{3, 4}, b.item())
	r.Equal([]byte{1, 2, 3, 4, 5, 6}, b.buf)
	r.Equal(3, b.bufCap)
	r.Equal(1, b.idx)
	r.Equal(2, b.itemSz)
	r.Equal(106, b.bufStart)
	r.Equal(114, b.bufEnd)

	hasMore, err = b.pop()
	r.NoError(err)
	r.True(hasMore)
	r.Equal([]tReaderCall{{bufLn: 6, offset: 100}}, tra.calls)
	r.Equal([]byte{5, 6}, b.item())
	r.Equal([]byte{1, 2, 3, 4, 5, 6}, b.buf)
	r.Equal(3, b.bufCap)
	r.Equal(2, b.idx)
	r.Equal(2, b.itemSz)
	r.Equal(106, b.bufStart)
	r.Equal(114, b.bufEnd)

	hasMore, err = b.pop()
	r.NoError(err)
	r.True(hasMore)
	r.Equal(
		[]tReaderCall{
			{bufLn: 6, offset: 100},
			{bufLn: 6, offset: 106},
			{bufLn: 3, offset: 109},
		},
		tra.calls,
	)
	r.Equal([]byte{7, 8}, b.item())
	r.Equal([]byte{7, 8, 9, 10, 11, 12}, b.buf)
	r.Equal(3, b.bufCap)
	r.Equal(0, b.idx)
	r.Equal(2, b.itemSz)
	r.Equal(112, b.bufStart)
	r.Equal(114, b.bufEnd)

	hasMore, err = b.pop()
	r.NoError(err)
	r.True(hasMore)
	r.Equal(
		[]tReaderCall{
			{bufLn: 6, offset: 100},
			{bufLn: 6, offset: 106},
			{bufLn: 3, offset: 109},
		},
		tra.calls,
	)
	r.Equal([]byte{9, 10}, b.item())
	r.Equal([]byte{7, 8, 9, 10, 11, 12}, b.buf)
	r.Equal(3, b.bufCap)
	r.Equal(1, b.idx)
	r.Equal(2, b.itemSz)
	r.Equal(112, b.bufStart)
	r.Equal(114, b.bufEnd)

	hasMore, err = b.pop()
	r.NoError(err)
	r.True(hasMore)
	r.Equal(
		[]tReaderCall{
			{bufLn: 6, offset: 100},
			{bufLn: 6, offset: 106},
			{bufLn: 3, offset: 109},
		},
		tra.calls,
	)
	r.Equal([]byte{11, 12}, b.item())
	r.Equal([]byte{7, 8, 9, 10, 11, 12}, b.buf)
	r.Equal(3, b.bufCap)
	r.Equal(2, b.idx)
	r.Equal(2, b.itemSz)
	r.Equal(112, b.bufStart)
	r.Equal(114, b.bufEnd)

	hasMore, err = b.pop()
	r.NoError(err)
	r.True(hasMore)
	r.Equal(
		[]tReaderCall{
			{bufLn: 6, offset: 100},
			{bufLn: 6, offset: 106},
			{bufLn: 3, offset: 109},
			{bufLn: 2, offset: 112},
		},
		tra.calls,
	)
	r.Equal([]byte{13, 14}, b.item())
	r.Equal([]byte{13, 14, 9, 10, 11, 12}, b.buf)
	r.Equal(1, b.bufCap)
	r.Equal(0, b.idx)
	r.Equal(2, b.itemSz)
	r.Equal(114, b.bufStart)
	r.Equal(114, b.bufEnd)

	hasMore, err = b.pop()
	r.NoError(err)
	r.False(hasMore)
	hasMore, err = b.pop()
	r.NoError(err)
	r.False(hasMore)
}

func TestSort2(t *testing.T) {
	itemSize := 4*1024 - 1
	r := require.New(t)
	srt, err := New(itemSize, func(a, b []byte) bool {
		return a[0] < b[0]
	}, make([]byte, itemSize*4))
	r.NoError(err)

	// TODO increase this greater then 9 and ensure we can pre-merge
	//      parts to use not very large buffer to merge file parts
	itemsToWrite := 9

	var data [][]byte
	for i := 0; i < itemsToWrite; i++ {
		item := make([]byte, itemSize)
		rand.Read(item)
		data = append(data, item)
		n, err := srt.Write(item)
		r.NoError(err)
		r.Equal(itemSize, n)
	}

	sortedData := bytes.NewBuffer(nil)
	n, err := srt.WriteTo(sortedData)
	r.NoError(err)
	r.Equal(n, int64(itemsToWrite*itemSize))

	sort.SliceStable(data, func(i, j int) bool {
		return data[i][0] < data[j][0]
	})

	flatData := make([]byte, 0, itemsToWrite*itemSize)
	for i := range data {
		flatData = append(flatData, data[i]...)
	}

	r.Equal(flatData, sortedData.Bytes())
}

func TestSort3(t *testing.T) {
	testCases := []struct {
		title        string
		itemSize     int
		buf          []byte
		itemsToWrite int
	}{
		{
			title:        "general",
			itemSize:     4*1024 - 1,
			buf:          make([]byte, (4*1024-1)*4),
			itemsToWrite: 9,
		},
		{
			title:        "large data",
			itemSize:     16,
			buf:          nil,
			itemsToWrite: 27525120 / 10000,
		},
	}
	for i := range testCases {
		tc := testCases[i]
		t.Run(tc.title, func(t *testing.T) {
			r := require.New(t)
			srt, err := New(tc.itemSize, func(a, b []byte) bool {
				return a[0] < b[0]
			}, tc.buf)
			r.NoError(err)
			defer func() {
				if err := srt.Close(); err != nil {
					t.Error(err)
				}
			}()

			var data [][]byte
			for i := 0; i < tc.itemsToWrite; i++ {
				item := make([]byte, tc.itemSize)
				n, err := rand.Read(item)
				r.NoError(err)
				r.Equal(len(item), n)
				data = append(data, item)
				n, err = srt.Write(item)
				r.NoError(err)
				r.Equal(tc.itemSize, n)
			}

			sortedData := bytes.NewBuffer(nil)
			n, err := srt.WriteTo(sortedData)
			r.NoError(err)
			r.Equal(n, int64(tc.itemsToWrite*tc.itemSize))

			sort.SliceStable(data, func(i, j int) bool {
				return data[i][0] < data[j][0]
			})

			flatData := make([]byte, 0, tc.itemsToWrite*tc.itemSize)
			for i := range data {
				flatData = append(flatData, data[i]...)
			}

			r.Equal(flatData, sortedData.Bytes())
		})
	}
}

func ui64(in []byte) uint64 {
	return binary.LittleEndian.Uint64(in)
}
func TestSort4(t *testing.T) {
	itemSize := 16
	itemsToWrite := 27_525_120 / 1
	// itemsToWrite = 6553600 // 100M
	itemsToWrite = 6_553_600 / 100
	r := require.New(t)
	srt, err := New(itemSize, func(a, b []byte) bool {
		return ui64(a) < ui64(b)
	}, nil)
	r.NoError(err)
	defer func() {
		if err := srt.Close(); err != nil {
			t.Error(err)
		}
	}()

	rndData := make([]byte, itemsToWrite*itemSize)
	n, err := rand.Read(rndData)
	r.NoError(err)
	r.Equal(itemsToWrite*itemSize, n)
	for i := 0; i < itemsToWrite; i++ {
		//r.NoError(err)
		//r.Equal(len(item), n)
		n, err = srt.Write(rndData[i*itemSize : (i+1)*itemSize])
		_ = err
		_ = n
		//r.NoError(err)
		//r.Equal(itemSize, n)
	}

	sortedData := bytes.NewBuffer(nil)
	n2, err := srt.WriteTo(sortedData)
	r.NoError(err)
	r.Equal(n2, int64(itemsToWrite*itemSize))

	//sort.SliceStable(data, func(i, j int) bool {
	//	return data[i][0] < data[j][0]
	//})

	//
	//flatData := make([]byte, 0, itemsToWrite*itemSize)
	//for i := range data {
	//	flatData = append(flatData, data[i]...)
	//}
	//
	//r.Equal(flatData, sortedData.Bytes())
}

func TestSortIsStable(t *testing.T) {
	r := require.New(t)

	itemSize := 16
	itemsToWrite := 27_525_120 / 1
	//itemsToWrite = 6_553_600 / 1

	lt := func(a, b []byte) bool { return ui64(a) < ui64(b) }
	srt, err := New(itemSize, lt, nil)
	r.NoError(err)
	defer func() {
		if err := srt.Close(); err != nil {
			t.Error(err)
		}
	}()

	rndData := make([]byte, itemsToWrite*itemSize)
	// to guarantee equal records (for testing stable), make max
	// key value less then total number of keys
	maxInt := int64(float64(itemsToWrite) * 0.9)
	for i := 0; i < itemsToWrite; i++ {
		key := uint64(rand.Int63n(maxInt))
		binary.LittleEndian.PutUint64(rndData[i*itemSize:], key)
		binary.LittleEndian.PutUint64(rndData[(i*itemSize)+8:], uint64(i))
	}

	var n int
	for i := 0; i < itemsToWrite; i++ {
		n, err = srt.Write(rndData[i*itemSize : (i+1)*itemSize])
		if err != nil {
			t.Fatal(err)
		}
		if n != itemSize {
			t.Fatal(n)
		}
	}

	sortedData := bytes.NewBuffer(nil)
	n2, err := srt.WriteTo(sortedData)
	r.NoError(err)
	r.Equal(n2, int64(itemsToWrite*itemSize))

	sortedBuf := sortedData.Bytes()
	prevKey := ui64(sortedBuf)
	prevVal := ui64(sortedBuf[8:])

	for i := 1; i < itemsToWrite; i++ {
		key := ui64(sortedBuf[i*itemSize:])
		val := ui64(sortedBuf[i*itemSize+8:])
		if key < prevKey {
			t.Fatalf(
				"at %v key %v less then previous key %v", i, key, prevKey,
			)
		} else if key == prevKey && key < prevKey {
			t.Fatalf(
				"at %v eys are are equal, "+
					"but current val %v is less then previous %v",
				i, val, prevVal,
			)
		}
		prevKey = key
		prevVal = val
	}
	t.Logf("successfuly checked buf of %v bytes", len(sortedBuf))
}

func TestStableSortTime(t *testing.T) {
	itemSize := 16
	itemsToWrite := 27_525_120 / 1
	// itemsToWrite = 6553600 // 100M
	itemsToWrite = 6_553_600 / 1
	r := require.New(t)

	rndData := make([]byte, itemsToWrite*itemSize)
	n, err := rand.Read(rndData)
	r.NoError(err)
	r.Equal(itemsToWrite*itemSize, n)

	t.Logf("the length of rndData is %v", len(rndData))

	sort.Stable(bytesSortUInt64{
		arr:   rndData,
		elmSz: 16,
		tmp:   make([]byte, 16),
	})
}

func btsLt(a, b []byte) bool {
	return ui64(a) < ui64(b)
}

func TestTimsortTime(t *testing.T) {
	itemSize := 16
	itemsToWrite := 27_525_120 / 1
	// itemsToWrite = 6553600 // 100M
	itemsToWrite = 6_553_600 / 1
	r := require.New(t)

	rndData := make([]byte, itemsToWrite*itemSize)
	n, err := rand.Read(rndData)
	r.NoError(err)
	r.Equal(itemsToWrite*itemSize, n)

	t.Logf("the length of rndData is %v", len(rndData))

	ts2 := go_fast_sort.NewBytesTimSorter(
		rndData, itemSize, btsLt, make([]byte, len(rndData)/2),
	)

	go_fast_sort.TimSort2(ts2)
}

type bytesSortUInt64 struct {
	arr   []byte
	elmSz int
	tmp   []byte
}

func (b bytesSortUInt64) Len() int {
	return len(b.arr) / b.elmSz
}

func (b bytesSortUInt64) Less(i, j int) bool {
	return ui64(b.arr[i*b.elmSz:]) < ui64(b.arr[j*b.elmSz:])
}

func (b bytesSortUInt64) Swap(i, j int) {
	copy(b.tmp, b.arr[j*b.elmSz:])
	copy(b.arr[j*b.elmSz:(j+1)*b.elmSz], b.arr[i*b.elmSz:])
	copy(b.arr[i*b.elmSz:], b.tmp)
}

func TestEmptySorting(t *testing.T) {
	sorter, err := New(33, func(a, b []byte) bool {
		return a[0] < b[0]
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	buf := &bytes.Buffer{}
	w := bufio.NewWriter(buf)
	n, err := sorter.WriteTo(w)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal(n)
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), []byte{}) {
		t.Fatal(buf.Bytes())
	}
}
