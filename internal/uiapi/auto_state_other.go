//go:build !windows

package uiapi

import "os"

func replaceAutoExecutionStateAtomic(from, to string) error {
	return os.Rename(from, to)
}
