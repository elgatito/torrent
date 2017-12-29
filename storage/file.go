package storage

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/anacrolix/missinggo"

	"github.com/anacrolix/torrent/metainfo"
)

// File-based storage for torrents, that isn't yet bound to a particular
// torrent.
type fileClientImpl struct {
	baseDir   string
	pathMaker func(baseDir string, info *metainfo.Info, infoHash metainfo.Hash) string
	pc        PieceCompletion
}

// The Default path maker just returns the current path
func defaultPathMaker(baseDir string, info *metainfo.Info, infoHash metainfo.Hash) string {
	return baseDir
}

func infoHashPathMaker(baseDir string, info *metainfo.Info, infoHash metainfo.Hash) string {
	return filepath.Join(baseDir, infoHash.HexString())
}

// All Torrent data stored in this baseDir
func NewFile(baseDir string) ClientImpl {
	return NewFileWithCompletion(baseDir, pieceCompletionForDir(baseDir))
}

func NewFileWithCompletion(baseDir string, completion PieceCompletion) ClientImpl {
	return newFileWithCustomPathMakerAndCompletion(baseDir, nil, completion)
}

// All Torrent data stored in subdirectorys by infohash
func NewFileByInfoHash(baseDir string) ClientImpl {
	return NewFileWithCustomPathMaker(baseDir, infoHashPathMaker)
}

// Allows passing a function to determine the path for storing torrent data
func NewFileWithCustomPathMaker(baseDir string, pathMaker func(baseDir string, info *metainfo.Info, infoHash metainfo.Hash) string) ClientImpl {
	return newFileWithCustomPathMakerAndCompletion(baseDir, pathMaker, pieceCompletionForDir(baseDir))
}

func newFileWithCustomPathMakerAndCompletion(baseDir string, pathMaker func(baseDir string, info *metainfo.Info, infoHash metainfo.Hash) string, completion PieceCompletion) ClientImpl {
	if pathMaker == nil {
		pathMaker = defaultPathMaker
	}
	return &fileClientImpl{
		baseDir:   baseDir,
		pathMaker: pathMaker,
		pc:        completion,
	}
}

func (me *fileClientImpl) Close() error {
	return me.pc.Close()
}

func (fs *fileClientImpl) OpenTorrent(info *metainfo.Info, infoHash metainfo.Hash) (TorrentImpl, error) {
	dir := fs.pathMaker(fs.baseDir, info, infoHash)
	err := CreateNativeZeroLengthFiles(info, dir)
	if err != nil {
		return nil, err
	}
	return &fileTorrentImpl{
		dir,
		info,
		infoHash,
		fs.pc,
		map[string]*fileTorrentHandle{},
	}, nil
}

type fileTorrentImpl struct {
	dir        string
	info       *metainfo.Info
	infoHash   metainfo.Hash
	completion PieceCompletion
	handles    map[string]*fileTorrentHandle
}

func (fts *fileTorrentImpl) Piece(p metainfo.Piece) PieceImpl {
	// Create a view onto the file-based torrent storage.
	_io := fileTorrentImplIO{fts}
	// Return the appropriate segments of this.
	return &filePieceImpl{
		fts,
		p,
		missinggo.NewSectionWriter(_io, p.Offset(), p.Length()),
		io.NewSectionReader(_io, p.Offset(), p.Length()),
	}
}

func (fs *fileTorrentImpl) OpenFile(fi metainfo.FileInfo, creatable bool) (*fileTorrentHandle, error) {
	filename := fs.fileInfoName(fi)
	if h, ok := fs.handles[filename]; ok {
		return h, nil
	}

	if _, err := os.Stat(filename); os.IsNotExist(err) && !creatable {
		return nil, io.EOF
	}

	os.MkdirAll(filepath.Dir(filename), 0770)
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}

	fs.handles[filename] = &fileTorrentHandle{f, filename, &sync.Mutex{}}
	return fs.handles[filename], nil
}

func (fs *fileTorrentImpl) Close() error {
	for _, h := range fs.handles {
		if h != nil && h.f != nil {
			err := h.f.Close()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// CreateNativeZeroLengthFiles Creates natives files for any zero-length file
// entries in the info. This is a helper for file-based storages, which
// don't address or write to zero-length files because they have
// no corresponding pieces.
func CreateNativeZeroLengthFiles(info *metainfo.Info, dir string) (err error) {
	for _, fi := range info.UpvertedFiles() {
		if fi.Length != 0 {
			continue
		}
		name := filepath.Join(append([]string{dir, info.Name}, fi.Path...)...)
		os.MkdirAll(filepath.Dir(name), 0770)
		var f *os.File
		f, err = os.Create(name)
		if err != nil {
			break
		}
		f.Close()
	}
	return
}

// Exposes file-based storage of a torrent, as one big ReadWriterAt.
type fileTorrentImplIO struct {
	fts *fileTorrentImpl
}

// Returns EOF on short or missing file.
func (fst *fileTorrentImplIO) readFileAt(fi metainfo.FileInfo, b []byte, off int64) (n int, err error) {
	var h *fileTorrentHandle
	h, err = fst.fts.OpenFile(fi, false)
	if err != nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// Limit the read to within the expected bounds of this file.
	if int64(len(b)) > fi.Length-off {
		b = b[:fi.Length-off]
	}
	for off < fi.Length && len(b) != 0 {
		n1, err1 := h.f.ReadAt(b, off)
		b = b[n1:]
		n += n1
		off += int64(n1)
		if n1 == 0 {
			err = err1
			break
		}
	}
	return
}

// Only returns EOF at the end of the torrent. Premature EOF is ErrUnexpectedEOF.
func (fst fileTorrentImplIO) ReadAt(b []byte, off int64) (n int, err error) {
	for _, fi := range fst.fts.info.UpvertedFiles() {
		for off < fi.Length {
			n1, err1 := fst.readFileAt(fi, b, off)
			n += n1
			off += int64(n1)
			b = b[n1:]
			if len(b) == 0 {
				// Got what we need.
				return
			}
			if n1 != 0 {
				// Made progress.
				continue
			}
			err = err1
			if err == io.EOF {
				// Lies.
				err = io.ErrUnexpectedEOF
			}
			return
		}
		off -= fi.Length
	}
	err = io.EOF
	return
}

func (fst fileTorrentImplIO) WriteAt(p []byte, off int64) (n int, err error) {
	for _, fi := range fst.fts.info.UpvertedFiles() {
		if off >= fi.Length {
			off -= fi.Length
			continue
		}
		n1 := len(p)
		if int64(n1) > fi.Length-off {
			n1 = int(fi.Length - off)
		}
		var h *fileTorrentHandle
		h, err = fst.fts.OpenFile(fi, true)
		if err != nil {
			return
		}
		h.mu.Lock()
		n1, err = h.f.WriteAt(p[:n1], off)
		// TODO: On some systems, write errors can be delayed until the Close.
		h.mu.Unlock()
		if err != nil {
			return
		}
		n += n1
		off = 0
		p = p[n1:]
		if len(p) == 0 {
			break
		}
	}
	return
}

func (fts *fileTorrentImpl) fileInfoName(fi metainfo.FileInfo) string {
	return filepath.Join(append([]string{fts.dir, fts.info.Name}, fi.Path...)...)
}

type fileTorrentHandle struct {
	f    *os.File
	path string
	mu   *sync.Mutex
}
