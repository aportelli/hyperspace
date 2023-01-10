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
	"sync"

	"github.com/aportelli/hyperspace/index/db"
)

type IndexerStats struct {
	NFiles         uint64
	TotalSize      uint64
	ActiveWorkers  int32
	QueuingWorkers int32
}

type FileIndexer struct {
	Db         *db.IndexDb
	stats      IndexerStats
	NumWorkers uint
	quitScan   chan int
	indexWg    sync.WaitGroup
}

func NewFileIndexer(d *db.IndexDb, numWorkers uint) *FileIndexer {
	s := new(FileIndexer)
	s.NumWorkers = numWorkers
	s.Db = d
	return s
}

func (s *FileIndexer) resetStats() {
	s.stats = IndexerStats{}
}

func (s *FileIndexer) Stats() IndexerStats {
	return s.stats
}

func (s *FileIndexer) Interrupt() {
	if s.quitScan != nil {
		s.quitScan <- 1
		s.indexWg.Wait()
	}
}
