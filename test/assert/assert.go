package assert

import (
	"reflect"
	"testing"
)

// Eq checks if two values are equal
func Eq(t *testing.T, exp, act interface{}) {
	if reflect.TypeOf(exp) != reflect.TypeOf(act) {
		t.Errorf("expected to equal (type mismatch) +expected -actual\n-%v(%T)\n+%v(%T)\n", exp, exp, act, act)
		return
	}

	if exp != act {
		t.Errorf("expected to equal +expected -actual\n-%v\n+%v\n", exp, act)
	}
}
