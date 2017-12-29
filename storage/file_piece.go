package storage

import (
	"io"

	"github.com/anacrolix/torrent/metainfo"
)

type filePieceImpl struct {
	*fileTorrentImpl
	p metainfo.Piece
	io.WriterAt
	io.ReaderAt
}

var _ PieceImpl = (*filePieceImpl)(nil)

func (me *filePieceImpl) pieceKey() metainfo.PieceKey {
	return metainfo.PieceKey{
		InfoHash: me.infoHash,
		Index:    me.p.Index(),
	}
}

func (fs *filePieceImpl) Completion() Completion {
	c, err := fs.completion.Get(fs.pieceKey())
	if err != nil || !c.Ok {
		return Completion{Ok: false}
	}
	// If it's allegedly complete, check that its constituent files have the
	// necessary length.
	for _, fi := range extentCompleteRequiredLengths(fs.p.Info, fs.p.Offset(), fs.p.Length()) {
		h, errOpen := fs.fileTorrentImpl.OpenFile(fi, false)
		if errOpen != nil {
			c.Complete = false
			break
		}

		h.mu.Lock()
		s, err := h.f.Stat()
		h.mu.Unlock()

		if err != nil || s == nil || s.Size() < fi.Length {
			c.Complete = false
			break
		}
	}
	if !c.Complete {
		// The completion was wrong, fix it.
		fs.completion.Set(fs.pieceKey(), false)
	}
	return c
}

func (fs *filePieceImpl) MarkComplete() error {
	return fs.completion.Set(fs.pieceKey(), true)
}

func (fs *filePieceImpl) MarkNotComplete() error {
	return fs.completion.Set(fs.pieceKey(), false)
}
