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
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	log "github.com/aportelli/golog"
)

type walkChan struct {
	jobs   chan<- *scanJob
	errors chan<- error
	done   chan<- struct{}
}

type scanJob struct {
	path   string
	isRoot bool
}

type scanChan struct {
	jobs    <-chan *scanJob
	entries chan<- *fileEntry
	errors  chan<- error
}

var signal = struct{}{}

func (s *FileIndexer) IndexDir(dir string) error {
	s.maxId = 0
	s.resetStats()
	cjobs := make(chan *scanJob)
	centries := make(chan *fileEntry)
	cerrors := make(chan error)
	cquit := make(chan struct{})
	walkDone := make(chan struct{})
	scanDone := make(chan struct{})
	wc := walkChan{jobs: cjobs, errors: cerrors, done: walkDone}
	sc := scanChan{jobs: cjobs, entries: centries, errors: cerrors}
	ic := insertChan{entries: centries, quit: cquit, errors: cerrors}
	var swg, iwg sync.WaitGroup
	iwg.Add(1)
	go s.insertData(ic, &iwg)
	swg.Add(1)
	go s.directoryProducer(dir, wc, &swg)
	go func() {
		log.Dbg.Printf("FileIndexer: Scanner pool starting (%d workers)", s.nWorkers)
		for i := uint(0); i < s.nWorkers; i++ {
			swg.Add(1)
			go s.directoryScanner(i, sc, &swg)
		}
		swg.Wait()
		scanDone <- signal
	}()
out:
	for {
		select {
		case <-walkDone:
			close(cjobs)
		case <-scanDone:
			close(cquit)
			break out
		case err := <-cerrors:
			log.Err.Fatalln(err)
			close(cjobs)
			close(cquit)
			return err
		}
	}
	iwg.Wait()
	return nil
}

func (s *FileIndexer) directoryProducer(root string, c walkChan, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Dbg.Println("FileIndexer: Walker started")
	c.jobs <- &scanJob{path: root, isRoot: true}
	visit := func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && root != path {
			c.jobs <- &scanJob{path: path, isRoot: false}
		}
		return nil
	}
	err := filepath.WalkDir(root, visit)
	if err != nil {
		c.errors <- err
	}
	c.done <- signal
	log.Dbg.Println("FileIndexer: Walker quitting")
}

func (s *FileIndexer) directoryScanner(workerId uint, c scanChan, wg *sync.WaitGroup) {
	defer wg.Done()
	for j := range c.jobs {
		var parentId any     // will be the id of the parent directory
		var currentId uint64 // will be the id of the current directory
		newId := s.newId()   // next usable id

		// process current directory
		currentId, newId = s.getDirId(j.path, newId)
		if j.isRoot {
			parentId = nil
		} else {
			parent := filepath.Dir(j.path)
			parentId, newId = s.getDirId(parent, newId)
		}
		if j.isRoot {
			i, err := os.Stat(j.path)
			if err != nil {
				c.errors <- err
			}
			c.entries <- &fileEntry{Id: currentId, ParentId: parentId, Path: j.path, Size: i.Size()}
		}

		// process other files in directory
		scan := func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			i, err2 := d.Info()
			if err2 != nil {
				return nil
			}
			if path != j.path {
				var id uint64
				if d.IsDir() {
					id, newId = s.getDirId(path, newId)
				} else {
					id = newId
					newId = s.newId()
				}
				c.entries <- &fileEntry{Id: id, ParentId: currentId, Path: path, Size: i.Size()}
				if d.IsDir() {
					return fs.SkipDir
				}
			}
			return nil
		}
		err := filepath.WalkDir(j.path, scan)
		if err != nil {
			c.errors <- err
		}
	}
}
