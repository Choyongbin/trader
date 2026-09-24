//go:build !windows

package main

import "os"

func replaceAtomic(from, to string) error { return os.Rename(from,to) }
