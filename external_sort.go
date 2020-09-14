package external_sort

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
)

type Sorter struct {
	itemSize    int
	itemBuf     []byte
	TempDir     string
	lessFn      func(a, b []byte) bool
	buf         []byte
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

	if buf == nil {
		if itemSize < defaultBufSize {
			buf = make([]byte, defaultBufSize/itemSize*itemSize)
		} else {
			buf = make([]byte, itemSize)
		}
	}

	if len(buf) < 0 {
		return nil, errors.New("buf size should be at least item size")
	}

	if len(buf)%itemSize != 0 {
		return nil, errors.New("buf size should be dividable by item size")
	}

	return &Sorter{
		buf:      buf,
		itemSize: itemSize,
		lessFn:   lessFn,
		itemBuf:  make([]byte, itemSize),
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
	s.err = s.tempFile.Truncate(0)
	if s.err != nil {
		return 0, s.err
	}
	return int64(n), nil

	panic("not implemented")
}

// Use the same buf for new sort
func (s *Sorter) Reset() {
	panic("not implemented")
}

func (s *Sorter) Close() error {
	if s.tempFile != nil {
		if s.err = s.tempFile.Close(); s.err != nil {
			return s.err
		}
		if s.err = os.Remove(s.tempFile.Name()); s.err != nil {
			return s.err
		}
	}
	return nil
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
	sort.Stable(sortedBuf{s})
}

func (s *Sorter) sortAndFlush() error {
	s.sort()
	return s.flush()
}

type sortedBuf struct {
	s *Sorter
}

func (s sortedBuf) Len() int {
	return s.s.bufIdx / s.s.itemSize
}

func (s sortedBuf) Less(i, j int) bool {
	a := s.s.buf[i*s.s.itemSize : (i+1)*s.s.itemSize]
	b := s.s.buf[j*s.s.itemSize : (j+1)*s.s.itemSize]
	return s.s.lessFn(a, b)
}

func (s sortedBuf) Swap(i, j int) {
	copy(s.s.itemBuf, s.s.buf[i*s.s.itemSize:(i+1)*s.s.itemSize])
	copy(
		s.s.buf[i*s.s.itemSize:(i+1)*s.s.itemSize],
		s.s.buf[j*s.s.itemSize:(j+1)*s.s.itemSize],
	)
	copy(s.s.buf[j*s.s.itemSize:(j+1)*s.s.itemSize], s.s.itemBuf)
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

	bufsNum := (s.sizeWritten + (len(s.buf) - 1)) / len(s.buf)

	minBufSize := 4 * 1024
	if s.itemSize > minBufSize {
		minBufSize = s.itemSize
	} else {
		minBufSize = (minBufSize / s.itemSize) * s.itemSize
	}

	if len(s.buf)/bufsNum < minBufSize {
		minSize := minBufSize * bufsNum
		return fmt.Errorf(
			"buffer is too small to merge data, at least it should be %v "+
				"bytes lenght", minSize,
		)
	}

	bufSize := (len(s.buf) / bufsNum / s.itemSize) * s.itemSize
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
			s.buf[i*bufSize:(i+1)*bufSize],
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
