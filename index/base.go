/*
Copyright © 2022 Antonin Portelli <antonin.portelli@me.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/
package index

import (
	"database/sql"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
)

type IndexerStats struct {
	NFiles    uint64
	TotalSize uint64
}

type FileIndexer struct {
	db             *sql.DB
	insertFileStmt *sql.Stmt
	insertTreeStmt *sql.Stmt
	maxId          uint64
	batchSize      uint
	stats          IndexerStats
	dirCache       sync.Map
	nWorkers       uint
}

func NewFileIndexer(path string, resetDb bool) (*FileIndexer, error) {
	s := new(FileIndexer)
	s.batchSize = 10000
	s.nWorkers = (uint)(2 * runtime.NumCPU())
	var err error
	if resetDb {
		err = os.RemoveAll(path)
		if err != nil {
			return nil, err
		}
	}
	s.db, err = sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	err = s.initDb()
	return s, err
}

func (s *FileIndexer) resetStats() {
	s.stats = IndexerStats{NFiles: 0, TotalSize: 0}
}

func (s *FileIndexer) Stats() IndexerStats {
	return s.stats
}

func (s *FileIndexer) getDirId(path string, newId uint64) (uint64, uint64) {
	dirId, loaded := s.dirCache.LoadOrStore(path, newId)
	if loaded {
		return dirId.(uint64), newId
	} else {
		return dirId.(uint64), s.newId()
	}
}

func (s *FileIndexer) newId() uint64 {
	return atomic.AddUint64(&s.maxId, 1)
}
