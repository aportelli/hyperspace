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
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"time"

	log "github.com/aportelli/golog"
	"github.com/aportelli/hyperspace/index"
	"github.com/briandowns/spinner"
	"github.com/spf13/cobra"
)

// scanCmd represents the index command
var indexCmd = &cobra.Command{
	Use:   "index <dir>",
	Short: "Index directory",
	Long:  ``,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var status int
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
		fileIndexer, err := index.NewFileIndexer(dbPath, indexOpt.Opt, true)
		log.ErrorCheck(err, "could not create database")
		spin := spinner.New(spinString, 100*time.Millisecond)
		spin.Color("blue")
		log.Msg.Printf("Scanning directory '%s'", root)
		done := make(chan int)
		sigint := make(chan os.Signal)
		signal.Notify(sigint, os.Interrupt)
		ticker := time.NewTicker(500 * time.Millisecond)
		tStart := <-ticker.C
		go func() {
			<-sigint
			if spin.Active() {
				spin.Stop()
			}
			log.Warn.Println("Indexing interrupted")
			fileIndexer.Interrupt()
		}()
		go func() {
			err := fileIndexer.IndexDir(root)
			var e *index.InterruptError
			if errors.As(err, &e) {
				done <- 1
			} else {
				log.ErrorCheck(err, "Indexer encountered an error")
			}
			done <- 0
		}()
	out:
		for {
			select {
			case status = <-done:
				break out
			case t := <-ticker.C:
				if !spin.Active() {
					spin.Start()
				}
				dt := t.Sub(tStart)
				stats := fileIndexer.Stats()
				spin.Suffix = fmt.Sprintf(" %.0f file/s | %d workers | %d queued | %.0f DB insert/s | total %d files, %s",
					float64(stats.NFiles)/dt.Seconds(), stats.ActiveWorkers, stats.QueuingWorkers,
					float64(stats.DbInsertions)/dt.Seconds(), stats.NFiles, log.SizeString(log.ByteSize(stats.TotalSize)))
			}
		}
		err = fileIndexer.Close()
		log.ErrorCheck(err, "could not close database")
		spin.Stop()
		printTotalStats(tStart, fileIndexer)
		if status > 0 {
			quit(status)
		}
	},
}

var indexOpt = struct {
	Db  string
	Opt index.FileIndexerOpt
}{
	Db:  "",
	Opt: index.FileIndexerOpt{NumWorkers: 0, DbBatchSize: 0},
}

func init() {
	rootCmd.AddCommand(indexCmd)
	indexCmd.Flags().StringVarP(&indexOpt.Db, "db", "d", "", "index database path")
	indexCmd.Flags().UintVarP(&indexOpt.Opt.NumWorkers, "jobs", "j", (uint)(runtime.NumCPU()), "number of concurrent scanner tasks")
	indexCmd.Flags().UintVarP(&indexOpt.Opt.DbBatchSize, "db-batch", "b", 10000, "number of insertion per DB transaction")
}

func printTotalStats(tStart time.Time, fileIndexer *index.FileIndexer) {
	dt := time.Since(tStart)
	stats := fileIndexer.Stats()
	log.Msg.Printf("Indexed %d file(s), total size %s, %.0f files/s", stats.NFiles,
		log.SizeString(log.ByteSize(stats.TotalSize)), float64(stats.NFiles)/dt.Seconds())
	log.Msg.Println("Total time", dt.String())
}
