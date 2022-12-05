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
package cmd

import (
	"os"

	log "github.com/aportelli/golog"
	"github.com/aportelli/hyperspace/index"
	"github.com/spf13/cobra"
)

// scanCmd represents the index command
var indexCmd = &cobra.Command{
	Use:   "index <dir>",
	Short: "Index directory",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		root := args[0]
		dbPath := indexOpt.Db
		if dbPath == "" {
			cacheDir, err := os.UserCacheDir()
			log.ErrorCheck(err, "")
			err = os.MkdirAll(cacheDir+"/hyperspace", 0750)
			dbPath = cacheDir + "/hyperspace/index.db"
			log.ErrorCheck(err, "")
		}
		log.Dbg.Println("using database '" + dbPath + "'")
		fileIndexer, err := index.NewFileIndexer(dbPath, true)
		log.ErrorCheck(err, "could not create database")
		fileIndexer.IndexDir(root)
		err = fileIndexer.Close()
		log.ErrorCheck(err, "could not close database")
		stats := fileIndexer.Stats()
		log.Msg.Printf("Indexed %d file(s), total size %s", stats.NFiles,
			log.SizeString(log.ByteSize(stats.TotalSize)))
	},
}

var indexOpt = struct{ Db string }{Db: ""}

func init() {
	rootCmd.AddCommand(indexCmd)
	indexCmd.Flags().StringVarP(&indexOpt.Db, "db", "d", "", "index database path")
}
