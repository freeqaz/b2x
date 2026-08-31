package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

// A box carries up to three disjoint B2 grants (read bucket-wide, B2_WRITE_*
// over one prefix, B2_PUBLISH_* over checkpoints/) because a B2 application key
// holds exactly ONE namePrefix. b2x knew only the first two, so every push to
// checkpoints/ presented the jobs-scoped key and 403'd — invisibly, since every
// call site falls back to rclone on any b2x failure.
func TestPushCredFollowsTheDestinationPrefix(t *testing.T) {
	c := &config{
		readCred:      credentials{keyID: "read"},
		writeCred:     credentials{keyID: "write"},
		writeScoped:   true,
		publishCred:   credentials{keyID: "publish"},
		publishScoped: true,
	}
	for _, tc := range []struct{ key, want string }{
		{"checkpoints/run-1/adapter.safetensors", "publish"},
		{"checkpoints/", "publish"},
		{"jobs/abc/results.tar", "write"},
		{"base-models/qwen35-9b/config.json", "write"},
		// Not a prefix match: the grant covers "checkpoints/", and a key that
		// merely starts with the word is a different place in the bucket.
		{"checkpoints-scratch/x", "write"},
	} {
		got, _ := c.pushCredFor(tc.key)
		if got.keyID != tc.want {
			t.Errorf("pushCredFor(%q) = %q, want %q", tc.key, got.keyID, tc.want)
		}
	}
}

// Without the grant, fall through to the write key rather than refusing: on a
// box with a bucket-wide key that write SUCCEEDS, and b2x is not where a scope
// decision made by the launcher should be re-litigated.
func TestPushToCheckpointsWithoutPublishGrantFallsThrough(t *testing.T) {
	c := &config{writeCred: credentials{keyID: "write"}, writeScoped: true}
	got, name := c.pushCredFor("checkpoints/run-1/x")
	if got.keyID != "write" {
		t.Errorf("got %q, want the write cred", got.keyID)
	}
	if !strings.Contains(name, "no publish grant") {
		t.Errorf("the label must say a 403 here is the scope, not b2x; got %q", name)
	}
}

func TestPublishCredIsLoadedFromEnv(t *testing.T) {
	for k, v := range map[string]string{
		"B2_BUCKET": "b", "B2_KEY_ID": "r", "B2_APPLICATION_KEY": "rs",
		"B2_PUBLISH_KEY_ID": "p", "B2_PUBLISH_APPLICATION_KEY": "ps",
	} {
		t.Setenv(k, v)
	}
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.publishScoped || cfg.publishCred.keyID != "p" {
		t.Errorf("publish grant not loaded: scoped=%v keyID=%q",
			cfg.publishScoped, cfg.publishCred.keyID)
	}
}

func TestNormKeyStripsEveryRemoteSpelling(t *testing.T) {
	c := &config{bucket: "bkt"}
	for _, pre := range []string{"b2:", "b2w:", "b2p:", "b2eu:"} {
		if got := c.normKey(pre + "bkt/checkpoints/run/x"); got != "checkpoints/run/x" {
			t.Errorf("normKey(%q…) = %q", pre, got)
		}
	}
}

// A 403 must name the grant that was actually presented. A box carries three
// disjoint keys, so a hint pointing at B2_WRITE_* when the push used
// B2_PUBLISH_* sends the reader to re-scope a key that was never involved.
func TestAuthHintNamesTheGrantThatWasUsed(t *testing.T) {
	for _, tc := range []struct {
		grant, scoped, want string
	}{
		{"publish", "y", publishPrefix},
		{"write", "y", "the write key is namePrefix-scoped"},
		{"read", "", "check B2_KEY_ID"},
	} {
		cfg := &config{usedGrant: tc.grant, writeScoped: tc.scoped == "y"}
		err := &httpError{Status: 403, Method: "PUT", Key: "/b/checkpoints/x"}
		out := captureStderr(t, func() { report(err, cfg) })
		if !strings.Contains(out, tc.want) {
			t.Errorf("grant %q hint = %q, want it to mention %q",
				tc.grant, out, tc.want)
		}
	}
}

func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	fn()
	w.Close()
	os.Stderr = old
	b, _ := io.ReadAll(r)
	return string(b)
}

// Root-anchored "/name" filters, the shape every adapter-publish site builds
// (`--include "/$f"`). Replaces TestRootAnchoredIncludeIsNotSupported, which
// pinned the gap that blocked those sites from migrating.
//
// The DEPTH half is the load-bearing one. Unanchored patterns also match the
// basename, so anchoring cannot be implemented as a bare leading-"/" strip:
// that would leave "/adapter_config.json" matching a mid-training
// "checkpoint-40/adapter_config.json" and sweep a stale adapter into the
// published prefix — precisely what the sites use the "/" to prevent.
func TestRootAnchoredFilters(t *testing.T) {
	for _, c := range []struct {
		pat, rel string
		want     bool
	}{
		// anchored: the root-level file, and ONLY the root-level file
		{"/adapter_model.safetensors", "adapter_model.safetensors", true},
		{"/adapter_config.json", "adapter_config.json", true},
		{"/adapter_config.json", "checkpoint-40/adapter_config.json", false},
		{"/adapter_model.safetensors", "sub/adapter_model.safetensors", false},
		{"/adapter_model.safetensors", "a/b/adapter_model.safetensors", false},
		{"/PUBLISHED.json", "PUBLISHED.json", true},
		{"/PUBLISHED.json", "out/PUBLISHED.json", false},
		// anchored globs stay anchored
		{"/*.json", "adapter_config.json", true},
		{"/*.json", "checkpoint-40/adapter_config.json", false},
		{"/checkpoint-*/**", "checkpoint-40/x.bin", true},
		{"/checkpoint-*/**", "out/checkpoint-40/x.bin", false},
		// unanchored behaviour is UNCHANGED (basename fallback intact)
		{"adapter_config.json", "checkpoint-40/adapter_config.json", true},
		{"STATUS", "out/STATUS", true},
	} {
		if got := globMatch(c.pat, c.rel); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.pat, c.rel, got, c.want)
		}
	}
}

// Anchoring applies to --exclude as well, and exclude still beats include.
func TestRootAnchoredIncludeAndExcludeCompose(t *testing.T) {
	inc := []string{"/adapter_model.safetensors", "/adapter_config.json"}
	exc := []string{"/adapter_config.json"}
	for _, c := range []struct {
		rel  string
		want bool
	}{
		{"adapter_model.safetensors", true},
		{"adapter_config.json", false},               // excluded at the root
		{"checkpoint-40/adapter_config.json", false}, // never included
		{"checkpoint-40/adapter_model.safetensors", false},
	} {
		if got := matchFilters(c.rel, inc, exc); got != c.want {
			t.Errorf("matchFilters(%q) = %v, want %v", c.rel, got, c.want)
		}
	}
	// A root-anchored EXCLUDE must not reach into a subdirectory either.
	if !matchFilters("checkpoint-40/STATUS", nil, []string{"/STATUS"}) {
		t.Error(`--exclude "/STATUS" wrongly excluded checkpoint-40/STATUS`)
	}
	if matchFilters("STATUS", nil, []string{"/STATUS"}) {
		t.Error(`--exclude "/STATUS" failed to exclude the root STATUS`)
	}
}
