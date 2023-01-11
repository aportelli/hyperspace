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
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aportelli/hyperspace/index/hash"
)

func (d *IndexDb) GetParentId(id int64) (int64, error) {
	var parentId int64
	r := d.db.QueryRow("SELECT parent_id FROM tree WHERE id = ?", id)
	err := r.Scan(&parentId)
	if err != nil {
		return 0, err
	}
	return parentId, nil
}

func (d *IndexDb) GetName(id int64) (string, error) {
	var name string
	r := d.db.QueryRow("SELECT name FROM tree WHERE id = ?", id)
	err := r.Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}

func (d *IndexDb) GetPath(id int64) (string, error) {
	var idPath string
	r := d.db.QueryRow("SELECT path FROM tree WHERE id = ?", id)
	err := r.Scan(&idPath)
	if err != nil {
		return "", err
	}
	split := strings.Split(idPath, "/")
	splitId, err := strconv.ParseInt("0x"+split[0], 0, 64)
	if err != nil {
		return "", err
	}
	path, err := d.GetName(splitId)
	if err != nil {
		return "", err
	}
	for i := 1; i < len(split); i++ {
		splitId, err = strconv.ParseInt("0x"+split[i], 0, 64)
		if err != nil {
			return "", err
		}
		buf, err := d.GetName(splitId)
		if err != nil {
			return "", err
		}
		path += "/" + buf
	}
	return path, nil
}

func (d *IndexDb) GetId(path string) (int64, error) {
	var relPath string
	if filepath.IsAbs(path) {
		root, err := d.GetValue("root_abs")
		if err != nil {
			return 0, err
		}
		relPath, err = filepath.Rel(root.(string), path)
		if err != nil {
			return 0, err
		}
	} else {
		relPath = filepath.Clean(path)
	}
	id, err := hash.PathHash(relPath)
	if err != nil {
		return 0, err
	}
	name, err := d.GetName(id)
	if err != nil {
		return 0, err
	}
	if name != filepath.Base(path) {
		return 0, fmt.Errorf("id %x has name %s, expected %s", id, name, filepath.Base(path))
	}
	return id, nil
}
