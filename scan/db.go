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
package scan

import (
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	log "github.com/aportelli/golog"
	_ "github.com/mattn/go-sqlite3"
)

type ScannerStats struct {
	NFiles    uint64
	TotalSize uint64
}

type FileScanner struct {
	db             *sql.DB
	insertFileStmt *sql.Stmt
	insertTreeStmt *sql.Stmt
	wg             sync.WaitGroup
	maxId          uint64
	batchSize      uint
	stats          ScannerStats
}

func NewFileScanner(path string, resetDb bool) (*FileScanner, error) {
	s := new(FileScanner)
	s.batchSize = 10000
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
	err = s.init()
	return s, err
}

func (s *FileScanner) init() error {
	_, err := s.db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		return err
	}
	_, err = s.db.Exec("PRAGMA synchronous=NORMAL")
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`CREATE TABLE IF NOT EXISTS tree (
		id INT PRIMARY KEY,
    parent_id INT NULL REFERENCES tree (id),
		size INT NOT NULL)`)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("CREATE TABLE IF NOT EXISTS files (id INT PRIMARY KEY, path TEXT NOT NULL)")
	if err != nil {
		return err
	}
	s.insertFileStmt, err = s.db.Prepare("INSERT INTO files VALUES(?,?)")
	if err != nil {
		return err
	}
	s.insertTreeStmt, err = s.db.Prepare("INSERT INTO tree VALUES(?,?,?)")
	return err
}

func (s *FileScanner) resetStats() {
	s.stats = ScannerStats{NFiles: 0, TotalSize: 0}
}

func (s *FileScanner) Stats() ScannerStats {
	return s.stats
}

func (s *FileScanner) Close() error {
	err := s.db.Close()
	return err
}

func (s *FileScanner) begin() error {
	_, err := s.db.Exec("BEGIN")
	return err
}

func (s *FileScanner) commit() error {
	_, err := s.db.Exec("COMMIT")
	return err
}

type FileEntry struct {
	Id   uint64
	Path string
}

type TreeEntry struct {
	Id       uint64
	ParentId any
	Size     int64
}

type scanChan struct {
	File           chan FileEntry
	Tree           chan TreeEntry
	StopInsert     chan struct{}
	InsertFinished chan struct{}
	Error          chan error
}

func (s *FileScanner) insertFile(entry FileEntry) error {
	_, err := s.insertFileStmt.Exec(entry.Id, entry.Path)
	return err
}

func (s *FileScanner) insertTree(entry TreeEntry) error {
	_, err := s.insertTreeStmt.Exec(entry.Id, entry.ParentId, entry.Size)
	return err
}

func (s *FileScanner) ScanDir(dir string) error {
	c := scanChan{
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
	go s.scanDir(dir, 0, c)
	log.Dbg.Println("FileScanner: Scanner started")
	s.wg.Wait()
	log.Dbg.Println("FileScanner: Scanner finished")
	close(c.StopInsert)
	<-c.InsertFinished
	select {
	case err := <-c.Error:
		return err
	default:
		return nil
	}
}

func (s *FileScanner) scanDir(dir string, id uint64, c scanChan) {
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
			go s.scanDir(path, newId, c)
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

func (s *FileScanner) insertData(c scanChan) {
	var err error
	log.Dbg.Println("FileScanner: Inserter started")
	for {
		err = s.begin()
		if err != nil {
			c.Error <- err
		}
		for i := uint(0); i < s.batchSize; i++ {
			select {
			case fileEntry := <-c.File:
				err = s.insertFile(fileEntry)
				if err != nil {
					c.Error <- err
				}
			case treeEntry := <-c.Tree:
				s.insertTree(treeEntry)
				if err != nil {
					c.Error <- err
				}
				s.stats.NFiles++
				s.stats.TotalSize += uint64(treeEntry.Size)
			case <-c.StopInsert:
				err = s.commit()
				if err != nil {
					c.Error <- err
				}
				close(c.InsertFinished)
				log.Dbg.Println("FileScanner: Inserter quitting")
				return
			}
		}
		err = s.commit()
		if err != nil {
			c.Error <- err
		}
	}
}
