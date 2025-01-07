// Package base for common code
package base

import "golang.org/x/exp/constraints"

// Entry represents a key-value pair
type Entry[K constraints.Ordered, V any] struct {
	Key   K
	Value V
}
