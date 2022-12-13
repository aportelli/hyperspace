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
	"sync"
	"sync/atomic"

	log "github.com/aportelli/golog"
)

type scanChan struct {
	entries chan<- *fileEntry
	errors  chan<- error
	guard   chan struct{}
}

var signal = struct{}{}

func (s *FileIndexer) IndexDir(dir string) error {
	s.maxId = 0
	s.resetStats()
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	centries := make(chan *fileEntry)
	cerrors := make(chan error)
	cquit := make(chan struct{})
	cguard := make(chan struct{}, s.opt.NumWorkers)
	scanDone := make(chan struct{})
	sc := scanChan{entries: centries, errors: cerrors, guard: cguard}
	ic := insertChan{entries: centries, quit: cquit, errors: cerrors}
	var swg, iwg sync.WaitGroup
	iwg.Add(1)
	go s.insertData(ic, &iwg)
	go func() {
		log.Dbg.Printf("FileIndexer: Scanner starting")
		id := s.newId()
		centries <- &fileEntry{Id: id, ParentId: nil, Path: dir, Size: info.Size()}
		swg.Add(1)
		cguard <- signal
		go s.scanDirectory(dirData{Path: dir, Id: id}, sc, &swg)
		swg.Wait()
		scanDone <- signal
	}()
out:
	for {
		select {
		case <-scanDone:
			close(cquit)
			break out
		case err := <-cerrors:
			log.Err.Fatalln(err)
			close(cquit)
			return err
		}
	}
	iwg.Wait()
	return nil
}

type dirData struct {
	Path string
	Id   any
}

func (s *FileIndexer) scanDirectory(dd dirData, c scanChan, wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() { <-c.guard }()

	// registering as active
	atomic.AddInt32(&s.stats.ActiveWorkers, 1)

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
			newId := s.newId()
			c.entries <- &fileEntry{Id: newId, ParentId: dd.Id, Path: path, Size: info.Size()}
			wg.Add(1)
			go func() {
				atomic.AddInt32(&s.stats.QueuingWorkers, 1)
				c.guard <- signal
				atomic.AddInt32(&s.stats.QueuingWorkers, -1)
				s.scanDirectory(dirData{Path: path, Id: newId}, c, wg)
			}()
			return filepath.SkipDir
		} else if !d.IsDir() {
			c.entries <- &fileEntry{Id: s.newId(), ParentId: dd.Id, Path: path, Size: info.Size()}
			return nil
		}
		return nil
	}

	// walk the tree
	filepath.WalkDir(dd.Path, scan)

	// registering as inactive
	atomic.AddInt32(&s.stats.ActiveWorkers, -1)
}
