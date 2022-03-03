package clover

import "testing"

func Test_Marshal(t *testing.T) {
	type Array struct {
		Strings []string
	}
	type Test struct {
		Name  string
		Value int
		List  Array
	}
	expected := Test{
		Name:  "abc",
		Value: 23,
		List: Array{
			Strings: []string{"X", "Y", "Z"},
		},
	}
	var got Test

	d := Marshal(expected)
	d.Unmarshal(&got)

	if expected.Name != got.Name {
		t.Errorf("expected %q, got %q", expected.Name, got.Name)
	}
	if expected.Value != got.Value {
		t.Errorf("expected %q, got %q", expected.Value, got.Value)
	}
	for i, v := range expected.List.Strings {
		g := got.List.Strings[i]
		if v != g {
			t.Errorf("expected %q, got %q", v, g)
		}
	}
}
