//go:build !windows

package bootstrap

import "os"

func replaceAtomic(source, target string) error { return os.Rename(source, target) }
