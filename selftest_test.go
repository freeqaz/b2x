package main

import (
	"os"
	"testing"
)

// selftestPrefix is the only seam of selftest that is testable without live
// credentials — the round-trip itself needs a real bucket.
func TestSelftestPrefixDefaultsAndTrims(t *testing.T) {
	cases := []struct {
		name string
		set  bool
		env  string
		want string
	}{
		{name: "unset", set: false, want: "_b2x_selftest"},
		{name: "empty", set: true, env: "", want: "_b2x_selftest"},
		{name: "whitespace", set: true, env: "   ", want: "_b2x_selftest"},
		{name: "bare slash", set: true, env: "/", want: "_b2x_selftest"},
		{name: "plain", set: true, env: "scratch", want: "scratch"},
		{name: "leading and trailing slash", set: true, env: "/jobs/scratch/", want: "jobs/scratch"},
		{name: "trailing slash", set: true, env: "jobs/_b2x_selftest/", want: "jobs/_b2x_selftest"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// t.Setenv first either way: it registers the restore that
			// keeps the unset case from leaking into the next test.
			t.Setenv("B2X_SELFTEST_PREFIX", tc.env)
			if !tc.set {
				os.Unsetenv("B2X_SELFTEST_PREFIX")
			}
			if got := selftestPrefix(); got != tc.want {
				t.Errorf("selftestPrefix() with %q = %q, want %q", tc.env, got, tc.want)
			}
		})
	}
}
