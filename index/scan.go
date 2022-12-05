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
	"os"
	"path/filepath"
	"sync/atomic"

	log "github.com/aportelli/golog"
)

type indexChan struct {
	File           chan FileEntry
	Tree           chan TreeEntry
	StopInsert     chan struct{}
	InsertFinished chan struct{}
	Error          chan error
}

func (s *FileIndexer) IndexDir(dir string) error {
	c := indexChan{
		File:           make(chan FileEntry),
		Tree:           make(chan TreeEntry),
		StopInsert:     make(chan struct{}),
		InsertFinished: make(chan struct{}),
		Error:          make(chan error),
	}
	s.maxId = 0
	s.resetStats()
	go s.insertData(c)
	d, err := os.Stat(dir)
	log.ErrorCheck(err, "")
	c.File <- FileEntry{Id: 0, Path: dir}
	c.Tree <- TreeEntry{Id: 0, ParentId: nil, Size: d.Size()}
	s.wg.Add(1)
	go s.indexDir(dir, 0, c)
	log.Dbg.Println("FileIndexer: Indexer started")
	s.wg.Wait()
	log.Dbg.Println("FileIndexer: Indexer finished")
	close(c.StopInsert)
	<-c.InsertFinished
	select {
	case err := <-c.Error:
		return err
	default:
		return nil
	}
}

func (s *FileIndexer) indexDir(dir string, id uint64, c indexChan) {
	defer s.wg.Done()
	visit := func(path string, d os.DirEntry, err error) error {
		var err2 error
		if err != nil {
			return nil
		}
		i, err2 := d.Info()
		if err2 != nil {
			return nil
		}
		if d.IsDir() && path != dir {
			newId := atomic.AddUint64(&s.maxId, 1)
			c.File <- FileEntry{Id: newId, Path: path}
			c.Tree <- TreeEntry{Id: newId, ParentId: id, Size: i.Size()}
			s.wg.Add(1)
			go s.indexDir(path, newId, c)
			return filepath.SkipDir
		} else if !i.IsDir() {
			newId := atomic.AddUint64(&s.maxId, 1)
			c.File <- FileEntry{Id: newId, Path: path}
			c.Tree <- TreeEntry{Id: newId, ParentId: id, Size: i.Size()}
		}
		return nil
	}
	filepath.WalkDir(dir, visit)
}
