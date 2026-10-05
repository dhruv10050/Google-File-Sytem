package common

import (
	"reflect"
	"testing"
)

func TestSplitRange(t *testing.T) {
	const cs = 100
	cases := []struct {
		off, n int64
		want   []Piece
	}{
		{0, 0, nil},
		{10, 20, []Piece{{0, 10, 0, 20}}},
		{90, 20, []Piece{{0, 90, 0, 10}, {1, 0, 10, 10}}},
		{100, 100, []Piece{{1, 0, 0, 100}}},
		{50, 300, []Piece{{0, 50, 0, 50}, {1, 0, 50, 100}, {2, 0, 150, 100}, {3, 0, 250, 50}}},
	}
	for _, c := range cases {
		if got := SplitRange(c.off, c.n, cs); !reflect.DeepEqual(got, c.want) {
			t.Errorf("SplitRange(%d, %d) = %v, want %v", c.off, c.n, got, c.want)
		}
	}
}

func TestCleanPath(t *testing.T) {
	for in, want := range map[string]string{"/": "/", "/a/b/": "/a/b", "/a//b/../c": "/a/c"} {
		if got, err := CleanPath(in); err != nil || got != want {
			t.Errorf("CleanPath(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "a/b"} {
		if _, err := CleanPath(bad); err == nil {
			t.Errorf("CleanPath(%q) accepted", bad)
		}
	}
	if got := Ancestors("/a/b/c"); !reflect.DeepEqual(got, []string{"/", "/a", "/a/b"}) {
		t.Errorf("Ancestors = %v", got)
	}
}
