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
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	log "github.com/aportelli/golog"
	"github.com/aportelli/hyperspace/index/db"
	"github.com/aportelli/hyperspace/index/hash"
)

type InterruptError struct{}

func (e *InterruptError) Error() string {
	return "indexing interrupted"
}

type scanChan struct {
	entries chan<- *db.FileEntry
	errors  chan<- error
	guard   chan struct{}
}

type dirData struct {
	Path     string
	TreePath string
	HashPath string
	Depth    uint
	Id       int64
}

func (s *FileIndexer) IndexDir(dir string) error {
	var status int
	s.resetStats()
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	s.Db.SetValue("root_input", dir)
	s.Db.SetValue("root_abs", root)
	centries := make(chan *db.FileEntry)
	cerrors := make(chan error)
	cquit := make(chan struct{})
	cguard := make(chan struct{}, s.NumWorkers)
	quitScan := make(chan int)
	s.quitScan = quitScan
	sc := scanChan{entries: centries, errors: cerrors, guard: cguard}
	ic := db.InsertChan{Entries: centries, Quit: cquit, Errors: cerrors}
	var swg sync.WaitGroup
	s.indexWg.Add(1)
	go s.Db.InsertData(ic, &s.indexWg)
	go func() {
		log.Dbg.Printf("FileIndexer: Scanner starting")
		id, err := hash.PathHash("")
		if err != nil {
			cerrors <- err
		}
		centries <- &db.FileEntry{
			Id:       id,
			ParentId: nil,
			Path:     "",
			Depth:    0,
			Name:     "",
			Type:     "d",
			Size:     info.Size(),
		}
		swg.Add(1)
		cguard <- struct{}{}
		go s.scanDirectory(dirData{Path: dir, TreePath: "", HashPath: "", Id: id}, sc, &swg)
		swg.Wait()
		quitScan <- 0
	}()
out:
	for {
		select {
		case status = <-quitScan:
			close(cquit)
			s.quitScan = nil
			break out
		case err := <-cerrors:
			close(cquit)
			s.quitScan = nil
			return err
		}
	}
	s.indexWg.Wait()
	if status == 1 {
		return &InterruptError{}
	} else {
		return nil
	}
}

func (s *FileIndexer) scanDirectory(dd dirData, c scanChan, wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() { <-c.guard }()

	// registering as active
	atomic.AddInt32(&s.stats.ActiveWorkers, 1)

	// path append function
	pathAppend := func(path string, extra string) string {
		if path != "" {
			return fmt.Sprintf("%s/%s", path, extra)
		} else {
			return extra
		}
	}

	// scan function
	scan := func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		info, err2 := d.Info()
		if err2 != nil {
			return nil
		}
		if d.IsDir() && dd.Path != path {
			newTreePath := pathAppend(dd.TreePath, info.Name())
			newId, err2 := hash.PathHash(newTreePath)
			if err2 != nil {
				return err2
			}
			newHashPath := pathAppend(dd.HashPath, hash.HashToString(newId))
			c.entries <- &db.FileEntry{
				Id:       newId,
				ParentId: dd.Id,
				Path:     newHashPath,
				Depth:    dd.Depth,
				Name:     info.Name(),
				Type:     "d",
				Size:     info.Size(),
			}
			atomic.AddUint64(&s.stats.NFiles, 1)
			atomic.AddUint64(&s.stats.TotalSize, uint64(info.Size()))
			wg.Add(1)
			go func() {
				atomic.AddInt32(&s.stats.QueuingWorkers, 1)
				c.guard <- struct{}{}
				atomic.AddInt32(&s.stats.QueuingWorkers, -1)
				s.scanDirectory(dirData{
					Path:     path,
					TreePath: newTreePath,
					HashPath: newHashPath,
					Depth:    dd.Depth + 1,
					Id:       newId,
				}, c, wg)
			}()
			return filepath.SkipDir
		} else if !d.IsDir() {
			treePath := pathAppend(dd.TreePath, info.Name())
			newId, err2 := hash.PathHash(treePath)
			if err2 != nil {
				return err2
			}
			hashPath := pathAppend(dd.HashPath, hash.HashToString(newId))
			c.entries <- &db.FileEntry{
				Id:       newId,
				ParentId: dd.Id,
				Path:     hashPath,
				Depth:    dd.Depth,
				Name:     info.Name(),
				Type:     "f",
				Size:     info.Size(),
			}
			atomic.AddUint64(&s.stats.NFiles, 1)
			atomic.AddUint64(&s.stats.TotalSize, uint64(info.Size()))
			return nil
		}
		return nil
	}

	// walk the tree
	err := filepath.WalkDir(dd.Path, scan)
	if err != nil {
		c.errors <- err
	}

	// registering as inactive
	atomic.AddInt32(&s.stats.ActiveWorkers, -1)
}
