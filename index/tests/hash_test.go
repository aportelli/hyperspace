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
	"strings"
	"testing"

	"github.com/aportelli/hyperspace/index/hash"
)

type hashTest struct {
	path       string
	intHash    int64
	stringHash string
}

var hashTests = []hashTest{
	{"/dir1/dir2/dir3/dir4", 126142529671152, "72b9d8ac0ff0"},
	{"/usr/bin/bash", 161059760740939, "927ba5d39a4b"},
	{"any string", 47700301343738, "2B6217846BFA"},
	{"UTF string João 👍", 164893783688819, "95f853670673"},
	{"日本語", 12063291817715, "0af8b4393ef3"},
}

func TestHash(t *testing.T) {
	for _, test := range hashTests {
		t.Run(strings.Replace(test.path, "/", "_", -1), func(t *testing.T) {
			intHash := hash.PathHash(test.path)
			if intHash != test.intHash {
				t.Errorf("Got int64 hash %x, expected %x\n", intHash, test.intHash)
			}
		})
	}
}

func TestStringToHash(t *testing.T) {
	for _, test := range hashTests {
		t.Run(strings.Replace(test.path, "/", "_", -1), func(t *testing.T) {
			stringHash := hash.HashToString(test.intHash)
			if !strings.EqualFold(stringHash, test.stringHash) {
				t.Errorf("Got string hash %s, expected %s\n", stringHash, test.stringHash)
			}
		})
	}
}

func TestHashToString(t *testing.T) {
	for _, test := range hashTests {
		t.Run(strings.Replace(test.path, "/", "_", -1), func(t *testing.T) {
			intHash, err := hash.StringToHash(test.stringHash)
			if err != nil {
				t.Errorf("Got error %s", err.Error())
			}
			if intHash != test.intHash {
				t.Errorf("Got hash %x, expected %x\n", intHash, test.intHash)
			}
		})
	}
}
