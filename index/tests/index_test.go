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
	"bufio"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/aportelli/hyperspace/index"
	"github.com/aportelli/hyperspace/index/db"
)

var testDir, testRoot string

func init() {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	file, err := os.Open(path.Join(dir, "filelist.txt"))
	if err != nil {
		log.Fatalf("error: %s", err.Error())
	}
	defer file.Close()
	testDir, err = os.MkdirTemp("", "hs_test")
	testRoot = filepath.Join(testDir, "test_root")
	if err != nil {
		log.Fatalf("error: %s", err.Error())
	}
	fileScanner := bufio.NewScanner(file)
	fileScanner.Split(bufio.ScanLines)
	for fileScanner.Scan() {
		path := filepath.Join(testRoot, fileScanner.Text())
		os.MkdirAll(filepath.Dir(path), 0750)
		os.WriteFile(path, []byte{1, 2, 3, 4, 5}, 0640)
	}
}

type pathTest struct {
	path     string
	normPath string
	id       int64
}

func TestIndex(t *testing.T) {
	var d *db.IndexDb

	t.Run("indexing", func(t *testing.T) {
		var err error
		opt := db.IndexDbOpt{Reset: true, BatchSize: 10000}
		d, err = db.NewIndexDb(filepath.Join(testDir, "test.db"), opt)
		if err != nil {
			t.Errorf("Got error %s", err.Error())
		}
		s := index.NewFileIndexer(d, 4)
		err = s.IndexDir(filepath.Join(testDir, "test_root"))
		if err != nil {
			t.Errorf("Got error %s", err.Error())
		}
	})

	paths := []pathTest{
		{"Hôtel/été", "Hôtel/été", 81617519048312},
		{".git/hooks/commit-msg.sample", ".git/hooks/commit-msg.sample", 989360371266},
		{".git/hooks/../hooks/commit-msg.sample", ".git/hooks/commit-msg.sample", 989360371266},
		{filepath.Join(testRoot, "index/tests/index_test.go"), "index/tests/index_test.go", 235148021444510},
	}

	for _, test := range paths {
		t.Run("queryId_"+strings.Replace(test.path, "/", "_", -1), func(t *testing.T) {
			id, err := d.GetId(test.path)
			if err != nil {
				t.Errorf("Got error %s", err.Error())
			}
			if id != test.id {
				t.Errorf("Got id %x, expected %x", id, test.id)
			}
		})
	}
	for _, test := range paths {
		t.Run("queryPath_"+strings.Replace(test.path, "/", "_", -1), func(t *testing.T) {
			path, err := d.GetPath(test.id)
			if err != nil {
				t.Errorf("Got error %s", err.Error())
			}
			if path != test.normPath {
				t.Errorf("Got path %s, expected %s", path, test.normPath)
			}
		})
	}
	for _, test := range paths {
		t.Run("queryName_"+strings.Replace(test.path, "/", "_", -1), func(t *testing.T) {
			name, err := d.GetName(test.id)
			if err != nil {
				t.Errorf("Got error %s", err.Error())
			}
			expName := filepath.Base(test.normPath)
			if name != expName {
				t.Errorf("Got name %s, expected %s", name, expName)
			}
		})
	}
	for _, test := range paths {
		t.Run("queryParentId_"+strings.Replace(test.path, "/", "_", -1), func(t *testing.T) {
			id, err := d.GetId(test.path)
			if err != nil {
				t.Errorf("Got error %s", err.Error())
			}
			parentId, err := d.GetParentId(id)
			if err != nil {
				t.Errorf("Got error %s", err.Error())
			}
			parentName, err := d.GetName(parentId)
			if err != nil {
				t.Errorf("Got error %s", err.Error())
			}
			expParentName := filepath.Base(filepath.Dir(test.normPath))
			if parentName != expParentName {
				t.Errorf("Got parent name %s, expected %s", parentName, expParentName)
			}
		})
	}

	d.Close()
}
