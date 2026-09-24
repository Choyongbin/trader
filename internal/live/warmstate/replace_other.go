//go:build !windows

package warmstate

import "os"

func replaceRuntimeAtomic(from, to string) error { return os.Rename(from, to) }
