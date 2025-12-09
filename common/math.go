package common

import (
	"golang.org/x/exp/constraints"
)

func Abs[T constraints.Integer | constraints.Float](a T) T {
	if a < 0 {
		return -a
	}
	return a
}

func Max[T constraints.Ordered](a, b T) T {
	if a < b {
		return b
	}
	return a
}

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func IsPowerOfTwo[T constraints.Integer](number T) bool {
	return (number > 1) && (number&(number-1)) == 0
}
