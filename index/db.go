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
	"sync"
	"sync/atomic"

	log "github.com/aportelli/golog"
	_ "github.com/mattn/go-sqlite3"
)

func (s *FileIndexer) openDb(path string) error {
	var err error
	s.db, err = sql.Open("sqlite3", path)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		return err
	}
	_, err = s.db.Exec("PRAGMA synchronous=NORMAL")
	if err != nil {
		return err
	}
	_, err = s.db.Exec("PRAGMA case_sensitive_like = ON")
	if err != nil {
		return err
	}
	return nil
}

func (s *FileIndexer) initDb() error {
	_, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS tree (
		id INT PRIMARY KEY,
    parent_id INT NULL REFERENCES tree (id),
		path TEXT NOT NULL,
		name TEXT NOT NULL,
		size INT NOT NULL)`)
	if err != nil {
		return err
	}
	_, err = s.db.Exec("CREATE INDEX IF NOT EXISTS index_path ON tree(path)")
	if err != nil {
		return err
	}
	s.insertTreeStmt, err = s.db.Prepare("INSERT INTO tree VALUES(?,?,?,?,?)")
	return err
}

func (s *FileIndexer) begin() error {
	_, err := s.db.Exec("BEGIN")
	return err
}

func (s *FileIndexer) commit() error {
	_, err := s.db.Exec("COMMIT")
	return err
}

type fileEntry struct {
	Id       uint64
	Name     string
	Path     string
	ParentId any
	Size     int64
}

func (s *FileIndexer) insertTree(entry *fileEntry) error {
	_, err := s.insertTreeStmt.Exec(entry.Id, entry.ParentId, entry.Path, entry.Name, entry.Size)
	atomic.AddUint64(&s.stats.DbInsertions, 1)
	return err
}

type insertChan struct {
	entries <-chan *fileEntry
	quit    <-chan struct{}
	errors  chan<- error
}

func (s *FileIndexer) insertData(c insertChan, wg *sync.WaitGroup) {
	var err error
	defer wg.Done()
	log.Dbg.Println("FileIndexer: Inserter started")
	for {
		err = s.begin()
		if err != nil {
			c.errors <- err
		}
		for i := uint(0); i < s.opt.DbBatchSize; i++ {
			select {
			case fileEntry := <-c.entries:
				err = s.insertTree(fileEntry)
				if err != nil {
					c.errors <- err
				}
				atomic.AddUint64(&s.stats.NFiles, 1)
				atomic.AddUint64(&s.stats.TotalSize, uint64(fileEntry.Size))
			case <-c.quit:
				err = s.commit()
				if err != nil {
					c.errors <- err
				}
				log.Dbg.Println("FileIndexer: Inserter quitting")
				return
			}
		}
		err = s.commit()
		if err != nil {
			c.errors <- err
		}
	}
}
