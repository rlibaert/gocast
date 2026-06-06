// Package assert provides assertion helpers for [testing].
package assert

import (
	"cmp"
	"errors"
	"iter"
	"testing"
)

// Expected calls [testing.TB.Error] unless condition is met.
func Expected(tb testing.TB, condition bool, args ...any) {
	tb.Helper()
	if !condition {
		tb.Error(args...)
	}
}

// Expectedf calls [testing.TB.Errorf] unless condition is met.
func Expectedf(tb testing.TB, condition bool, format string, args ...any) {
	tb.Helper()
	if !condition {
		tb.Errorf(format, args...)
	}
}

// Required calls [testing.TB.Fatal] and stops execution unless condition is met.
func Required(tb testing.TB, condition bool, args ...any) {
	tb.Helper()
	if !condition {
		tb.Fatal(args...)
	}
}

// Requiredf calls [testing.TB.Fatalf] and stops execution unless condition is met.
func Requiredf(tb testing.TB, condition bool, format string, args ...any) {
	tb.Helper()
	if !condition {
		tb.Fatalf(format, args...)
	}
}

// ErrNil marks the function as having failed and stops its execution unless err == nil.
func ErrNil(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatalf("got error: %v", err)
	}
}

// ErrIs marks the function as having failed unless [errors.Is] target.
func ErrIs(tb testing.TB, err, target error) {
	tb.Helper()
	if !errors.Is(err, target) {
		tb.Errorf("got error: %v; want: %v", err, target)
	}
}

// SeqContains marks the function as having failed unless haystack contains needle.
func SeqContains[T comparable](tb testing.TB, haystack iter.Seq[T], needle T) {
	s := []T{}
	for v := range haystack {
		if v == needle {
			return
		}
		s = append(s, v)
	}
	tb.Errorf("got: %v; miss: %v", s, needle)
}

// EQ marks the function as having failed unless got == want.
func EQ[T comparable](tb testing.TB, got, want T) {
	tb.Helper()
	if got != want {
		tb.Errorf("got: %v; want: == %v", got, want)
	}
}

// NE marks the function as having failed unless got != want.
func NE[T comparable](tb testing.TB, got, want T) {
	tb.Helper()
	if got == want {
		tb.Errorf("got: %v; want: != %v", got, want)
	}
}

// LT marks the function as having failed unless got < want.
func LT[T cmp.Ordered](tb testing.TB, got, want T) {
	tb.Helper()
	if cmp.Compare(got, want) != -1 {
		tb.Errorf("got: %v; want: < %v", got, want)
	}
}

// GT marks the function as having failed unless got > want.
func GT[T cmp.Ordered](tb testing.TB, got, want T) {
	tb.Helper()
	if cmp.Compare(got, want) != 1 {
		tb.Errorf("got: %v; want: > %v", got, want)
	}
}

// LE marks the function as having failed unless got <= want.
func LE[T cmp.Ordered](tb testing.TB, got, want T) {
	tb.Helper()
	if cmp.Compare(got, want) == 1 {
		tb.Errorf("got: %v; want: <= %v", got, want)
	}
}

// GE marks the function as having failed unless got >= want.
func GE[T cmp.Ordered](tb testing.TB, got, want T) {
	tb.Helper()
	if cmp.Compare(got, want) == -1 {
		tb.Errorf("got: %v; want: >= %v", got, want)
	}
}
