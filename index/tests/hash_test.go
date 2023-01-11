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
	{"dir1/dir2/dir3/dir4", 275515016816611, "fa9456b527e3"},
	{"usr/bin/bash", 103075467877435, "5dbf20a5dc3b"},
	{"UTF/string/João/👍", 150634961215366, "89006f5a9f86"},
	{"日/本/語", 52573909869918, "2fd0d138e55e"},
}

func TestHash(t *testing.T) {
	for _, test := range hashTests {
		t.Run(strings.Replace(test.path, "/", "_", -1), func(t *testing.T) {
			intHash, err := hash.PathHash(test.path)
			if err != nil {
				t.Errorf("error: %s\n", err.Error())
			}
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
