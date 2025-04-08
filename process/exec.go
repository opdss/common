package process

import (
	"fmt"
	"github.com/zeebo/errs"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

func init() {
	cobra.MousetrapHelpText = "This is a command line tool.\n\n" +
		"This needs to be run from a Command Prompt.\n"

	// Figure out the executable name.
	exe, err := os.Executable()
	if err == nil {
		cobra.MousetrapHelpText += fmt.Sprintf(
			"Try running \"%s help\" for more information\n", exe)
	}
}

// fileExists checks whether file exists, handle error correctly if it doesn't.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		log.Fatalf("failed to check for file existence: %v", err)
	}
	return true
}

// atomicWriteFile is a helper to atomically write the data to the outfile.
func atomicWriteFile(outfile string, data []byte, _ os.FileMode) (err error) {
	// TODO: provide better atomicity guarantees, like fsyncing the parent
	// directory and, on windows, using MoveFileEx with MOVEFILE_WRITE_THROUGH.

	fh, err := os.CreateTemp(filepath.Dir(outfile), filepath.Base(outfile))
	if err != nil {
		return errs.Wrap(err)
	}
	needsClose, needsRemove := true, true

	defer func() {
		if needsClose {
			err = errs.Combine(err, errs.Wrap(fh.Close()))
		}
		if needsRemove {
			err = errs.Combine(err, errs.Wrap(os.Remove(fh.Name())))
		}
	}()

	if _, err := fh.Write(data); err != nil {
		return errs.Wrap(err)
	}

	needsClose = false
	if err := fh.Close(); err != nil {
		return errs.Wrap(err)
	}

	if err := os.Rename(fh.Name(), outfile); err != nil {
		return errs.Wrap(err)
	}
	needsRemove = false

	return nil
}
