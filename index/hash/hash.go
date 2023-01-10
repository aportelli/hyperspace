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
package hash

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"golang.org/x/text/unicode/norm"
)

// Return the hash of a path as int64 (SQLite does not support uint64).
// The hash is a 48 bit hash defined as follows
//
//  1. compute the MD5 hash of the path
//  2. take the 8 highest bytes of the MD5 hash
//  3. set the 2 highest bytes to zero
//  4. convert the result to int64 in big endian
func PathHash(path string) int64 {
	fullHash := md5.Sum([]byte(norm.NFC.String(path)))
	copy(fullHash[:2], "\x00\x00")
	hash := int64(binary.BigEndian.Uint64(fullHash[:8]))
	return hash
}

// Convert a path hash to an hexadecimal string
func HashToString(hash int64) string {
	byteHash := make([]byte, 8)
	binary.BigEndian.PutUint64(byteHash, (uint64)(hash))
	byteHash = byteHash[2:]
	return fmt.Sprintf("%x", byteHash)
}

// Convert a path hash hexadecimal string to int64
func StringToHash(hash string) (int64, error) {
	byteHash, err := hex.DecodeString(hash)
	if err != nil {
		return 0, err
	}
	byteHash = append([]byte{0, 0}, byteHash...)
	intHash := int64(binary.BigEndian.Uint64(byteHash))
	return intHash, nil
}
