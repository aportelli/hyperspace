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

func (d *IndexDb) SetValue(key string, value any) error {
	_, err := d.insertValStmt.Exec(key, value)
	if err != nil {
		return err
	}
	return nil
}

func (d *IndexDb) GetValue(key string) (any, error) {
	var value any
	r := d.db.QueryRow("SELECT value FROM key_value WHERE key = ?", key)
	err := r.Scan(&value)
	if err != nil {
		return "", err
	}
	return value, nil
}
