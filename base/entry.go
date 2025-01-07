package base

import "golang.org/x/exp/constraints"

type Entry[K constraints.Ordered, V any] struct {
	Key   K
	Value V
}
