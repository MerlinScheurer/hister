// SPDX-License-Identifier: AGPL-3.0-or-later

package cmd

import (
	"fmt"

	"github.com/asciimoo/hister/server/model"

	"github.com/spf13/cobra"
)

var recompressCmd = &cobra.Command{
	Use:   "recompress",
	Short: "Convert stored HTML and favicon files to the configured compression",
	Long: `Rewrite stored HTML and favicon data files into the codec set by indexer.compression.

Stop the Hister server before running this command, because it opens the local index.`,
	Args: cobra.NoArgs,
	Run: func(_ *cobra.Command, _ []string) {
		idx := initIndex(model.ReadWrite)
		defer idx.Close()
		stats, err := idx.RecompressData()
		if err != nil {
			exit(1, "Recompress error: "+err.Error())
		}
		fmt.Printf("Converted %d data file(s) to %s\n", stats.Converted, cfg.Indexer.StorageCompression())
		fmt.Printf("Skipped %d data file(s) already in %s\n", stats.Skipped, cfg.Indexer.StorageCompression())
		if stats.Unreadable > 0 {
			fmt.Printf("Left %d unreadable data file(s) in place (see log for paths)\n", stats.Unreadable)
		}
		if stats.Unrecognized > 0 {
			fmt.Printf("Ignored %d data file(s) with an unrecognized path\n", stats.Unrecognized)
		}
	},
}
