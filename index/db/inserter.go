/*
Copyright © 2022 Antonin Portelli <antonin.portelli@me.com>

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more detaild.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/
package db

import (
	"sync"
	"sync/atomic"

	log "github.com/aportelli/golog"
	"golang.org/x/text/unicode/norm"
)

type FileEntry struct {
	Id       int64
	ParentId any
	Path     string
	Depth    uint
	Name     string
	Type     string
	Size     int64
}

type InsertChan struct {
	Entries <-chan *FileEntry
	Quit    <-chan struct{}
	Errors  chan<- error
}

func (d *IndexDb) begin() error {
	_, err := d.db.Exec("BEGIN")
	return err
}

func (d *IndexDb) commit() error {
	_, err := d.db.Exec("COMMIT")
	return err
}

func (d *IndexDb) insertTree(entry *FileEntry) error {
	_, err := d.insertTreeStmt.Exec(entry.Id, entry.ParentId, entry.Path, entry.Depth, entry.Name,
		entry.Type, entry.Size)
	atomic.AddUint64(&d.Insertions, 1)
	return err
}

func (d *IndexDb) InsertData(c InsertChan, wg *sync.WaitGroup) {
	var err error
	defer wg.Done()
	log.Dbg.Println("FileIndexer: Inserter started")
	for {
		err = d.begin()
		if err != nil {
			c.Errors <- err
		}
		for i := uint(0); i < d.BatchSize; i++ {
			select {
			case fileEntry := <-c.Entries:
				fileEntry.Name = norm.NFC.String(fileEntry.Name)
				err = d.insertTree(fileEntry)
				if err != nil {
					c.Errors <- err
				}
			case <-c.Quit:
				err = d.commit()
				if err != nil {
					c.Errors <- err
				}
				log.Dbg.Println("FileIndexer: Inserter quitting")
				return
			}
		}
		err = d.commit()
		if err != nil {
			c.Errors <- err
		}
	}
}
