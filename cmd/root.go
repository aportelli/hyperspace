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
	"runtime/pprof"

	log "github.com/aportelli/golog"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "hs",
	Short: "Fast file indexer",
	Long:  ``,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if rootOpt.Profile != "" {
			log.Inf.Printf("Starting profiling (output file '%s')", rootOpt.Profile)
			f, err := os.Create(rootOpt.Profile)
			log.ErrorCheck(err, "cannot create profile file '"+rootOpt.Profile+"'")
			err = pprof.StartCPUProfile(f)
			log.ErrorCheck(err, "cannot start profiling")
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		if rootOpt.Profile != "" {
			log.Inf.Printf("Stopping profiling (output file '%s')", rootOpt.Profile)
			pprof.StopCPUProfile()
		}
	},
}

var rootOpt = struct{ Profile string }{""}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().IntVarP(&log.Level, "verbosity", "v", 0,
		"verbosity level (0: default, 1: info, 2: debug)")
	rootCmd.PersistentFlags().StringVar(&rootOpt.Profile, "profile", "", "save pprof profile")
}
