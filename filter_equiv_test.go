package main

// filter_equiv_test.go — b2x's file SELECTION must equal rclone's, filter for
// filter, on the shapes the adapter-publish call sites this replaced actually
// pass.
//
// Those publish sites are `rclone copy --include "/$f" … "$OUT" "$PUB_DEST"`
// wrapped in a 3-try loop whose success condition is a read-back hash check.
// Migrating one to b2x changes WHICH BYTES GET PUBLISHED, so the evidence for
// the move has to be a selection comparison against the tool being replaced,
// not a unit test of our own glob matcher agreeing with itself.
//
// The b2x side is a REAL runPush against the in-process S3 server (the same
// walk, the same matchFilters, the same key construction) — not a
// reimplementation of the predicate. The rclone side is `rclone lsf -R
// --files-only` with the identical flags. Both are asked for the same fixture
// tree; the assertion is set equality.
//
// Skips when rclone is absent. That is honest rather than convenient: the
// comparand IS rclone, so without it there is nothing to compare to.

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// publishFixture builds a tree shaped like a real training OUT dir: the
// published payload at the root, and the things that must NOT ship — a
// mid-training checkpoint carrying same-named files, log dirs, and the
// publish stage's own scratch files. PUB_* below are the file-set variable
// names the publish script this replaced used; they are kept so the fixture
// still maps onto the flags in that script's rclone lines.
func publishFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	files := []string{
		// the PUB_REQ + PUB_TOK payload
		"adapter_config.json", "adapter_model.safetensors",
		"train_summary.json", "artifact-manifest.json",
		"tokenizer.json", "tokenizer_config.json", "special_tokens_map.json",
		// present at the root, deliberately NOT in PUB_FILES
		"corpus-identity.json", "PUBLISHED.json", "README.md",
		"training_args.bin", ".publish-lsjson.json",
		// the trap: same names, one level down
		"checkpoint-40/adapter_config.json",
		"checkpoint-40/adapter_model.safetensors",
		"checkpoint-40/trainer_state.json",
		"checkpoint-40/tokenizer.json",
		"logs/train.log",
		"runs/train/events.out.tfevents",
	}
	for _, f := range files {
		p := filepath.Join(root, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(f+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// rcloneSelect returns what `rclone copy` WOULD move, as root-relative paths.
func rcloneSelect(t *testing.T, root string, flags []string) []string {
	t.Helper()
	bin, err := exec.LookPath("rclone")
	if err != nil {
		t.Skip("rclone not on PATH — the comparand for this test is rclone itself")
	}
	args := append([]string{"lsf", "-R", "--files-only"}, flags...)
	args = append(args, root)
	out, err := exec.Command(bin, args...).Output()
	if err != nil {
		t.Fatalf("rclone lsf %v: %v", args, err)
	}
	var got []string
	for _, l := range strings.Split(string(out), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			got = append(got, l)
		}
	}
	sort.Strings(got)
	return got
}

// b2xSelect returns what `b2x push` actually uploads, as root-relative keys.
func b2xSelect(t *testing.T, root string, flags []string) []string {
	t.Helper()
	f, c, cfg := testHarness(t)
	inc, exc := splitFilterFlags(t, flags)
	const dst = "checkpoints/equiv-fixture/"
	err := runPush(context.Background(), c, cfg, root, dst,
		pushOpts{includes: inc, excludes: exc},
		newStats("push", root, dst, false, cfg.concurrency))
	if err != nil {
		t.Fatalf("runPush: %v", err)
	}
	var got []string
	f.mu.Lock()
	for k := range f.objects {
		got = append(got, strings.TrimPrefix(k, dst))
	}
	f.mu.Unlock()
	sort.Strings(got)
	return got
}

// splitFilterFlags parses the same --include/--exclude vector both tools get,
// so neither side can be handed a different filter list than the other.
func splitFilterFlags(t *testing.T, flags []string) (inc, exc []string) {
	t.Helper()
	for i := 0; i < len(flags); i += 2 {
		if i+1 >= len(flags) {
			t.Fatalf("dangling filter flag %q", flags[i])
		}
		switch flags[i] {
		case "--include":
			inc = append(inc, flags[i+1])
		case "--exclude":
			exc = append(exc, flags[i+1])
		default:
			t.Fatalf("unexpected flag %q", flags[i])
		}
	}
	return
}

func TestPublishFilterSelectionMatchesRclone(t *testing.T) {
	// PUB_FILES as the sites build it, verbatim: PUB_REQ then the PUB_TOK
	// files that exist. This is the flag vector a migrated site passes.
	pubFiles := []string{
		"adapter_config.json", "adapter_model.safetensors",
		"train_summary.json", "artifact-manifest.json",
		"tokenizer.json", "tokenizer_config.json", "special_tokens_map.json",
	}
	var pubInc []string
	for _, f := range pubFiles {
		pubInc = append(pubInc, "--include", "/"+f)
	}

	for _, tc := range []struct {
		name  string
		flags []string
	}{
		{"PUB_INC payload", pubInc},
		// the seal, a second copy with a single root-anchored include
		{"PUBLISHED.json seal", []string{"--include", "/PUBLISHED.json"}},
		{"anchored glob", []string{"--include", "/*.json"}},
		{"anchored dir glob", []string{"--include", "/checkpoint-*/**"}},
		{"anchored exclude", []string{"--exclude", "/README.md"}},
		// A MIXED vector where nothing overlaps: the only mixed shape any known
		// call site uses (a boot script's checkpoint flush) and the only one
		// rclone defines — see TestMixedIncludeExcludeIsUndefinedInRclone.
		{"disjoint include plus exclude", []string{
			"--exclude", "/README.md",
			"--include", "/adapter_config.json",
			"--include", "/adapter_model.safetensors"}},
		{"no filters at all", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := publishFixture(t)
			want := rcloneSelect(t, root, tc.flags)
			got := b2xSelect(t, root, tc.flags)
			if strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Errorf("selection differs for %v\nrclone: %v\nb2x:    %v",
					tc.flags, want, got)
			}
			if len(want) == 0 {
				t.Errorf("fixture selected nothing for %v — a vacuous pass", tc.flags)
			}
		})
	}
}

// The ONE place b2x and rclone differ, found by the equivalence sweep above
// and deliberately NOT closed — because on this shape rclone has no answer to
// match. Given an --include and an --exclude that both match a path, rclone
// logs "Using --filter is recommended instead of both --include and --exclude
// as the order they are parsed in is indeterminate" and, measured on v1.74.4,
// lets the INCLUDE win in either command-line order. b2x applies every exclude
// first, so an exclude always wins.
//
// Equivalence is therefore undefined here, not violated, and b2x picks the safe
// direction: on a filter that decides which bytes get published, refusing is
// the cheaper error. Closing it would also mean teaching main.go to observe the
// interleaving, which Go's flag package does not preserve across two vectors.
//
// Unreachable on the fleet this grew in: the only mixed-filter call site was a
// boot script's checkpoint flush (--exclude '.preempt_*' plus --include globs
// read from a file) and those cannot match the same path.
func TestMixedIncludeExcludeIsUndefinedInRclone(t *testing.T) {
	inc := []string{"/adapter_config.json", "/adapter_model.safetensors"}
	exc := []string{"/adapter_config.json"}
	if matchFilters("adapter_config.json", inc, exc) {
		t.Error("b2x must let the exclude win regardless of flag order")
	}
	if !matchFilters("adapter_model.safetensors", inc, exc) {
		t.Error("an unexcluded include must still select")
	}
	// Pin rclone's actual behaviour rather than the doc's "indeterminate": if a
	// future rclone starts honouring the exclude, the divergence is gone and
	// this comment is what needs rewriting.
	root := publishFixture(t)
	for _, order := range [][]string{
		{"--include", "/adapter_config.json", "--exclude", "/adapter_config.json"},
		{"--exclude", "/adapter_config.json", "--include", "/adapter_config.json"},
	} {
		got := rcloneSelect(t, root, order)
		if len(got) != 1 || got[0] != "adapter_config.json" {
			t.Errorf("rclone's overlapping-filter precedence changed (%v -> %v) "+
				"— re-derive the divergence before trusting the comment above",
				order, got)
		}
	}
}

// The trap the root anchoring exists for, stated as its own assertion so a
// regression names itself instead of showing up as a set diff.
func TestPublishFilterNeverSweepsAMidTrainingCheckpoint(t *testing.T) {
	root := publishFixture(t)
	got := b2xSelect(t, root, []string{
		"--include", "/adapter_config.json",
		"--include", "/adapter_model.safetensors",
		"--include", "/tokenizer.json"})
	for _, k := range got {
		if strings.Contains(k, "/") {
			t.Errorf("a root-anchored publish uploaded a NESTED key: %q "+
				"(a checkpoint-N adapter reaching checkpoints/$RUN_NAME/ is a "+
				"silent adapter swap)", k)
		}
	}
	if len(got) != 3 {
		t.Errorf("want the 3 root files, got %v", got)
	}
}
