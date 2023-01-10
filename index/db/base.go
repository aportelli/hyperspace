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
	"database/sql"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

type IndexDb struct {
	db             *sql.DB
	insertTreeStmt *sql.Stmt
	insertValStmt  *sql.Stmt
	Insertions     uint64
	BatchSize      uint
}

type IndexDbOpt struct {
	Reset     bool
	BatchSize uint
}

func NewIndexDb(path string, opt IndexDbOpt) (*IndexDb, error) {
	var err error
	d := new(IndexDb)
	if opt.Reset {
		err = os.RemoveAll(path)
		if err != nil {
			return nil, err
		}
	}
	err = d.open(path)
	if err != nil {
		return nil, err
	}
	if opt.Reset {
		err = d.initTables()
		if err != nil {
			return nil, err
		}
	}
	err = d.initStatements()
	if err != nil {
		return nil, err
	}
	d.BatchSize = opt.BatchSize
	return d, err
}

func (d *IndexDb) Close() error {
	err := d.db.Close()
	return err
}

func (d *IndexDb) open(path string) error {
	var err error
	d.db, err = sql.Open("sqlite3", path)
	if err != nil {
		return err
	}
	_, err = d.db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		return err
	}
	_, err = d.db.Exec("PRAGMA synchronous=NORMAL")
	if err != nil {
		return err
	}
	_, err = d.db.Exec("PRAGMA case_sensitive_like = ON")
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	return nil
}

func (d *IndexDb) initTables() error {
	_, err := d.db.Exec(`CREATE TABLE value_map (
		key TEXT PRIMARY KEY,
		value TEXT)`)
	if err != nil {
		return err
	}
	_, err = d.db.Exec(`CREATE TABLE tree (
		id INT PRIMARY KEY,
    parent_id INT NULL REFERENCES tree (id),
		path TEXT NOT NULL,
		depth INT NOT NULL,
		name TEXT NOT NULL,
		type TEXT NOT NULL,
		size INT NOT NULL)`)
	if err != nil {
		return err
	}
	_, err = d.db.Exec(`CREATE VIEW view_tree_hex AS
		SELECT
		  printf("%012x",id) AS id,
			CASE
			  WHEN parent_id NOT NULL THEN printf("%012x",parent_id)
				ELSE NULL
			END parent_id,
			path, depth, name, type, size
		FROM tree`)

	return err
}

func (d *IndexDb) initStatements() error {
	var err error
	d.insertTreeStmt, err = d.db.Prepare("INSERT INTO tree VALUES(?,?,?,?,?,?,?)")
	if err != nil {
		return err
	}
	d.insertValStmt, err = d.db.Prepare("REPLACE INTO value_map VALUES(?,?)")
	if err != nil {
		return err
	}
	return nil
}
