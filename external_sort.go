package external_sort

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	go_fast_sort "github.com/olomix/go-fast-sort"
)

type Sorter struct {
	itemSize    int
	TempDir     string
	lessFn      func(a, b []byte) bool
	buf         []byte
	bufCap      int    // full capacity of buf we can use for merging
	tmBuf       []byte // buffer for tim-sort algorithm
	bufIdx      int
	err         error
	sizeWritten int
	tempFile    *os.File
}

const defaultBufSize = 100 * 1024 * 1024

// buf may be nil. In this case we allocate default buf of size ~100MB dividable
// by itemSize
func New(
	itemSize int,
	lessFn func(a, b []byte) bool,
	buf []byte,
) (*Sorter, error) {
	if itemSize <= 0 {
		return nil, errors.New("item size should be greater then zero")
	}

	var timSortBufItems int
	var bufItems int
	var bufCap int
	if buf != nil {

		itemsInBuf := len(buf) / itemSize
		if itemsInBuf < 3 {
			return nil, errors.New(
				"buf len is too small, should be 3*itemSize at least",
			)
		}

		timSortBufItems = itemsInBuf / 3
		if timSortBufItems*3 > itemsInBuf {
			timSortBufItems++
		}

		bufItems = itemsInBuf - timSortBufItems
		bufCap = bufItems * itemSize

	} else {
		if 3*itemSize > defaultBufSize {
			return nil, errors.New(
				"allocated buffer required for such a big items",
			)
		}

		timSortBufItems = (defaultBufSize / itemSize) / 3
		bufItems = timSortBufItems * 2
		bufCap = (bufItems + timSortBufItems) * itemSize
		buf = make([]byte, bufCap)
	}

	return &Sorter{
		buf:      buf[:bufItems*itemSize],
		tmBuf:    buf[bufItems*itemSize : (bufItems+timSortBufItems)*itemSize],
		itemSize: itemSize,
		lessFn:   lessFn,
		bufCap:   bufCap,
	}, nil
}

func (s *Sorter) Write(in []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}

	bytesWritten := 0
	for bytesWritten < len(in) {
		n := copy(s.buf[s.bufIdx:], in[bytesWritten:])
		bytesWritten += n
		s.bufIdx += n

		if s.bufIdx == len(s.buf) {
			if s.err = s.sortAndFlush(); s.err != nil {
				return 0, s.err
			}
		}
	}

	return bytesWritten, nil
}

// Write sorted data to writer. After call WriteTo, buffers are flushed
// and temp file is truncated and ready so sort other data.
func (s *Sorter) WriteTo(w io.Writer) (int64, error) {
	if s.err != nil {
		return 0, s.err
	}

	if s.bufIdx != 0 {
		if s.err = s.sortAndFlush(); s.err != nil {
			return 0, s.err
		}
	}

	if s.sizeWritten%s.itemSize != 0 {
		s.err = errors.New("size written is not dividable by item size")
		return 0, s.err
	}

	// TODO if there is too many parts, pre-merge into bigger chunks.
	s.err = s.merge(w)
	if s.err != nil {
		return 0, s.err
	}

	n := s.sizeWritten
	s.sizeWritten = 0
	if s.tempFile != nil {
		s.err = s.tempFile.Truncate(0)
		if s.err != nil {
			return 0, s.err
		}
	}
	return int64(n), nil
}

// Use the same buf for new sort
func (s *Sorter) Reset() error {
	if s.err != nil {
		return s.err
	}
	if s.tempFile != nil {
		_, s.err = s.tempFile.Seek(0, 0)
		if s.err != nil {
			return s.err
		}
	}
	s.bufIdx = 0
	s.sizeWritten = 0
	return nil
}

func (s *Sorter) Close() error {
	if s.tempFile != nil {
		if s.err = s.tempFile.Close(); s.err != nil {
			return s.err
		}
		if s.err = os.Remove(s.tempFile.Name()); s.err != nil {
			return s.err
		}
		s.tempFile = nil
	}
	return s.Reset()
}

func (s *Sorter) flush() error {
	if s.err != nil {
		return s.err
	}

	// If nothing to flush, exit
	if s.bufIdx == 0 {
		return nil
	}

	if s.tempFile == nil {
		s.tempFile, s.err = ioutil.TempFile(s.TempDir, "")
		if s.err != nil {
			return s.err
		}
	}

	var nn int
	for nn < s.bufIdx {
		var n int
		n, s.err = s.tempFile.Write(s.buf[nn:s.bufIdx])
		if s.err != nil {
			return s.err
		}
		nn += n
	}

	s.sizeWritten += nn
	s.bufIdx = 0
	return nil
}

func (s *Sorter) sort() {
	ts2 := go_fast_sort.NewBytesTimSorter(
		s.buf[:s.bufIdx], s.itemSize, s.lessFn, s.tmBuf,
	)
	go_fast_sort.TimSort2(ts2)
}

func (s *Sorter) sortAndFlush() error {
	s.sort()
	return s.flush()
}

func (s *Sorter) merge(w io.Writer) error {
	// TODO if only one chunk in file, just write it to output writer.
	//      following code expects minimum two chunks if file

	if s.err != nil {
		return s.err
	}

	if s.sizeWritten%s.itemSize != 0 {
		s.err = errors.New("size written is not dividable by item size")
		return s.err
	}

	// nothing was written
	if s.sizeWritten == 0 {
		return nil
	}

	buf := s.buf[:s.bufCap]
	bufsNum := (s.sizeWritten + (len(s.buf) - 1)) / len(s.buf)

	minBufSize := 4 * 1024
	if s.itemSize > minBufSize {
		minBufSize = s.itemSize
	} else {
		minBufSize = (minBufSize / s.itemSize) * s.itemSize
	}

	if len(buf)/bufsNum < minBufSize {
		minSize := minBufSize * bufsNum
		return fmt.Errorf(
			"buffer is too small to merge data, at least it should be %v "+
				"bytes lenght", minSize,
		)
	}

	bufSize := (len(buf) / bufsNum / s.itemSize) * s.itemSize
	if bufSize <= 0 {
		return fmt.Errorf("[assertion] unexpected bufSize: %v", bufSize)
	}

	bufs := make([]*bb, bufsNum)
	for i := range bufs {
		fEnd := len(s.buf) * (i + 1)
		if fEnd > s.sizeWritten {
			fEnd = s.sizeWritten
		}
		bufs[i], s.err = newBB(
			buf[i*bufSize:(i+1)*bufSize],
			s.itemSize,
			s.tempFile,
			len(s.buf)*i,
			fEnd,
		)
		if s.err != nil {
			return s.err
		}
	}

	for {
		if len(bufs) == 1 {
			// just copy everything from buf to output writer
			for {
				if err := writeAll(w, bufs[0].item()); err != nil {
					return err
				}
				hasMore, err := bufs[0].pop()
				if err != nil {
					return err
				}
				if !hasMore {
					return nil
				}
			}
		}

		minIdx := 0

		for i := 1; i < len(bufs); i++ {
			if s.lessFn(bufs[i].item(), bufs[minIdx].item()) {
				minIdx = i
			}
		}

		if err := writeAll(w, bufs[minIdx].item()); err != nil {
			return err
		}

		hasMore, err := bufs[minIdx].pop()
		if err != nil {
			return err
		}
		if !hasMore {
			// delete empty buffer
			bufs = append(bufs[:minIdx], bufs[minIdx+1:]...)
		}
	}
}

func writeAll(w io.Writer, v []byte) error {
	dataWritten := 0
	for dataWritten < len(v) {
		n, err := w.Write(v[dataWritten:])
		if err != nil {
			return err
		}
		dataWritten += n
	}
	return nil
}

// Assertion: (bufEnd - bufStart) % itemSz == 0
// Assertion: len(buf) % itemSz == 0
type bb struct {
	buf      []byte
	bufCap   int // number of items in buf
	idx      int // current item number
	itemSz   int // item length in bytes
	f        io.ReaderAt
	bufStart int // offset in file f where chunk starts
	bufEnd   int // offset+1 where buf ends
}

func newBB(
	buf []byte, itemSz int, f io.ReaderAt, fStart, fEnd int,
) (*bb, error) {
	if itemSz <= 0 {
		return nil, errors.New("item size should be greater then zero")
	}
	if len(buf) <= 0 {
		return nil, errors.New("buffer length should be greater the zero")
	}
	if len(buf)%itemSz != 0 {
		return nil, errors.New("buffer len should be dividable by item size")
	}
	if f == nil {
		return nil, errors.New("input file is nil")
	}
	if fStart < 0 || fEnd < 0 || fEnd <= fStart {
		return nil, errors.New(
			"incorrect file offsets, end should be greater then start",
		)
	}
	if (fEnd-fStart)%itemSz != 0 {
		return nil, errors.New("offsets should be dividable by item size")
	}
	b := &bb{
		buf:      buf,
		bufCap:   0,
		idx:      0,
		itemSz:   itemSz,
		f:        f,
		bufStart: fStart,
		bufEnd:   fEnd,
	}

	var err error
	b.bufCap, err = b.fill()
	if err != nil {
		return nil, err
	}
	if b.bufCap <= 0 {
		return nil, errors.New("[assertion] n should not be zero")
	}
	return b, nil
}

// current current item, return errBbEmpty if buffer is empty and no data in
// file left
func (bb *bb) item() []byte {
	return bb.buf[bb.idx*bb.itemSz : bb.idx*bb.itemSz+bb.itemSz]
}

// fill buffer with data, return number of items filled
func (bb *bb) fill() (int, error) {
	if bb.bufEnd-bb.bufStart == 0 {
		return 0, nil
	}

	buf := bb.buf
	if len(buf) > bb.bufEnd-bb.bufStart {
		buf = buf[:bb.bufEnd-bb.bufStart]
	}
	bytesToRead := len(buf)
	bytesRead := 0
	for bytesRead < bytesToRead {
		n, err := bb.f.ReadAt(buf[bytesRead:], int64(bb.bufStart))
		if err != nil {
			return 0, err
		}
		bytesRead += n
		bb.bufStart += n
	}
	return bytesRead / bb.itemSz, nil
}

// pop remove head element from buffer, if buffer is empty, read more from file,
// if nothing to read, return false. Else return true and following item()
// will return data.
func (bb *bb) pop() (bool, error) {
	if bb.bufStart == bb.bufEnd && bb.idx == bb.bufCap {
		return false, nil
	}

	bb.idx++
	if bb.idx < bb.bufCap {
		// we still have more items in buffer
		return true, nil
	}

	n, err := bb.fill()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	bb.bufCap = n
	bb.idx = 0
	return true, nil
}
