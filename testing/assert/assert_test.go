package assert_test

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/rlibaert/gocast/testing/assert"
)

type tb struct{ testing.TB }

func (tb) Helper()                           {}
func (tb) Errorf(format string, args ...any) { fmt.Fprintf(os.Stdout, "E: "+format+"\n", args...) }
func (tb) Fatalf(format string, args ...any) { fmt.Fprintf(os.Stdout, "F: "+format+"\n", args...) }

func Example() {
	assert.Expectedf(tb{}, 1 == 2, "the condition %s was not met", "1 == 2")
	assert.Requiredf(tb{}, 1 == 2, "the condition %s was not met", "1 == 2")
	assert.ErrNil(tb{}, errors.New("an error"))
	assert.ErrIs(tb{}, nil, errors.New("an error"))
	//Output:
	// E: the condition 1 == 2 was not met
	// F: the condition 1 == 2 was not met
	// F: got error: an error
	// E: got error: <nil>; want: an error
}

func Example_types() {
	assert.NE(tb{}, 123, 123)
	assert.NE(tb{}, "foo", "foo")
	assert.NE(tb{}, 'f', 'f')
	assert.NE(tb{}, true, true)
	assert.LT(tb{}, time.Millisecond, time.Millisecond)
	//Output:
	// E: got: 123; want: != 123
	// E: got: foo; want: != foo
	// E: got: 102; want: != 102
	// E: got: true; want: != true
	// E: got: 1ms; want: < 1ms
}

func ExampleEQ() {
	assert.EQ(tb{}, 1, 2)
	assert.EQ(tb{}, 2, 2)
	assert.EQ(tb{}, 3, 2)
	//Output:
	// E: got: 1; want: == 2
	// E: got: 3; want: == 2
}

func ExampleNE() {
	assert.NE(tb{}, 1, 2)
	assert.NE(tb{}, 2, 2)
	assert.NE(tb{}, 3, 2)
	//Output:
	// E: got: 2; want: != 2
}

func ExampleLT() {
	assert.LT(tb{}, 1, 2)
	assert.LT(tb{}, 2, 2)
	assert.LT(tb{}, 3, 2)
	//Output:
	// E: got: 2; want: < 2
	// E: got: 3; want: < 2
}

func ExampleGT() {
	assert.GT(tb{}, 1, 2)
	assert.GT(tb{}, 2, 2)
	assert.GT(tb{}, 3, 2)
	//Output:
	// E: got: 1; want: > 2
	// E: got: 2; want: > 2
}

func ExampleLE() {
	assert.LE(tb{}, 1, 2)
	assert.LE(tb{}, 2, 2)
	assert.LE(tb{}, 3, 2)
	//Output:
	// E: got: 3; want: <= 2
}

func ExampleGE() {
	assert.GE(tb{}, 1, 2)
	assert.GE(tb{}, 2, 2)
	assert.GE(tb{}, 3, 2)
	//Output:
	// E: got: 1; want: >= 2
}

func ExampleSeqContains() {
	m := map[string]int{"foo": 1, "bar": 2, "baz": 3}
	assert.SeqContains(tb{}, maps.Keys(m), "foo")
	assert.SeqContains(tb{}, maps.Values(m), 3)
	assert.SeqContains(tb{}, slices.Values([]int{1, 2, 3}), 4)
	//Output:
	// E: got: [1 2 3]; miss: 4
}
