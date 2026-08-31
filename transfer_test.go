package main

// transfer_test.go — end-to-end pull/push against an in-process S3 server.
//
// This is the test that actually pins the behaviors the component was built
// for: idempotent skip, partial resume, parallel ranged GET, multipart PUT,
// newest-first deadline ordering. No network, no credentials, no B2.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- a minimal, concurrency-correct S3 server -------------------------------

type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	meta    map[string]map[string]string
	uploads map[string]map[int][]byte

	rangeGets atomic.Int64 // how many ranged GETs we served
	maxPar    atomic.Int64 // peak concurrent in-flight requests
	inflight  atomic.Int64
	failNext  atomic.Int64 // serve N 503s before succeeding (retry test)
	// headMissing403 makes HEAD on an absent key answer 403 instead of 404,
	// which is what a prefix/bucket-restricted B2 key actually does.
	headMissing403 atomic.Bool
	// denyList 403s the LIST too — a key that genuinely cannot read.
	denyList atomic.Bool
}

func newFakeS3() *fakeS3 {
	return &fakeS3{objects: map[string][]byte{}, meta: map[string]map[string]string{},
		uploads: map[string]map[int][]byte{}}
}

func (f *fakeS3) put(key string, data []byte) {
	f.mu.Lock()
	f.objects[key] = data
	f.mu.Unlock()
}

func (f *fakeS3) etag(data []byte) string {
	s := sha256.Sum256(data)
	return hex.EncodeToString(s[:16])
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cur := f.inflight.Add(1)
	for {
		m := f.maxPar.Load()
		if cur <= m || f.maxPar.CompareAndSwap(m, cur) {
			break
		}
	}
	defer f.inflight.Add(-1)

	if f.failNext.Load() > 0 {
		f.failNext.Add(-1)
		w.WriteHeader(503)
		w.Write([]byte(`<Error><Code>SlowDown</Code><Message>try again</Message></Error>`))
		return
	}

	// path is /<bucket>/<key...>
	p := strings.TrimPrefix(r.URL.Path, "/")
	i := strings.Index(p, "/")
	key := ""
	if i >= 0 {
		key = p[i+1:]
	}
	q := r.URL.Query()

	switch {
	case r.Method == "GET" && q.Get("list-type") == "2":
		if f.denyList.Load() {
			w.WriteHeader(403)
			w.Write([]byte(`<Error><Code>AccessDenied</Code><Message>not entitled</Message></Error>`))
			return
		}
		f.listHandler(w, q)
	case r.Method == "HEAD":
		f.mu.Lock()
		data, ok := f.objects[key]
		md := f.meta[key]
		f.mu.Unlock()
		if !ok {
			// A restricted B2 key answers HEAD on a key it cannot see with 403,
			// never 404 — it will not confirm or deny existence. Every fleet box
			// carries such a key, so this is the REALISTIC default and 404 is
			// the special case.
			if f.headMissing403.Load() {
				w.WriteHeader(403)
				w.Write([]byte(`<Error><Code>AccessDenied</Code><Message>not entitled</Message></Error>`))
				return
			}
			w.WriteHeader(404)
			return
		}
		for k, v := range md {
			w.Header().Set("x-amz-meta-"+k, v)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("ETag", `"`+f.etag(data)+`"`)
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.WriteHeader(200)
	case r.Method == "GET":
		f.mu.Lock()
		data, ok := f.objects[key]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(404)
			w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
			return
		}
		if rg := r.Header.Get("Range"); rg != "" {
			f.rangeGets.Add(1)
			var lo, hi int64
			fmt.Sscanf(rg, "bytes=%d-%d", &lo, &hi)
			if hi >= int64(len(data)) {
				hi = int64(len(data)) - 1
			}
			w.Header().Set("Content-Length", strconv.FormatInt(hi-lo+1, 10))
			w.WriteHeader(206)
			w.Write(data[lo : hi+1])
			return
		}
		w.Write(data)
	case r.Method == "POST" && q.Has("uploads"):
		id := fmt.Sprintf("u%d", time.Now().UnixNano())
		f.mu.Lock()
		f.uploads[id] = map[int][]byte{}
		md := map[string]string{}
		for k, v := range r.Header {
			lk := strings.ToLower(k)
			if strings.HasPrefix(lk, "x-amz-meta-") {
				md[strings.TrimPrefix(lk, "x-amz-meta-")] = v[0]
			}
		}
		f.meta[key] = md
		f.mu.Unlock()
		w.Write([]byte(`<InitiateMultipartUploadResult><UploadId>` + id + `</UploadId></InitiateMultipartUploadResult>`))
	case r.Method == "PUT" && q.Get("uploadId") != "":
		n, _ := strconv.Atoi(q.Get("partNumber"))
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.uploads[q.Get("uploadId")][n] = body
		f.mu.Unlock()
		w.Header().Set("ETag", `"`+f.etag(body)+`"`)
		w.WriteHeader(200)
	case r.Method == "POST" && q.Get("uploadId") != "":
		id := q.Get("uploadId")
		f.mu.Lock()
		parts := f.uploads[id]
		nums := make([]int, 0, len(parts))
		for n := range parts {
			nums = append(nums, n)
		}
		sort.Ints(nums)
		var buf bytes.Buffer
		for _, n := range nums {
			buf.Write(parts[n])
		}
		f.objects[key] = buf.Bytes()
		delete(f.uploads, id)
		f.mu.Unlock()
		w.Write([]byte(`<CompleteMultipartUploadResult/>`))
	case r.Method == "DELETE" && q.Get("uploadId") != "":
		f.mu.Lock()
		delete(f.uploads, q.Get("uploadId"))
		f.mu.Unlock()
		w.WriteHeader(204)
	case r.Method == "PUT":
		body, _ := io.ReadAll(r.Body)
		md := map[string]string{}
		for k, v := range r.Header {
			lk := strings.ToLower(k)
			if strings.HasPrefix(lk, "x-amz-meta-") {
				md[strings.TrimPrefix(lk, "x-amz-meta-")] = v[0]
			}
		}
		f.mu.Lock()
		f.objects[key] = body
		f.meta[key] = md
		f.mu.Unlock()
		w.WriteHeader(200)
	default:
		w.WriteHeader(400)
	}
}

func (f *fakeS3) listHandler(w http.ResponseWriter, q map[string][]string) {
	prefix := ""
	if v, ok := q["prefix"]; ok && len(v) > 0 {
		prefix = v[0]
	}
	f.mu.Lock()
	type ent struct {
		k string
		d []byte
	}
	var ents []ent
	for k, d := range f.objects {
		if strings.HasPrefix(k, prefix) {
			ents = append(ents, ent{k, d})
		}
	}
	f.mu.Unlock()
	sort.Slice(ents, func(i, j int) bool { return ents[i].k < ents[j].k })

	var b strings.Builder
	b.WriteString(`<?xml version="1.0"?><ListBucketResult><IsTruncated>false</IsTruncated>`)
	for _, e := range ents {
		fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>%d</Size><ETag>&quot;%s&quot;</ETag><LastModified>%s</LastModified></Contents>`,
			xmlEscape(e.k), len(e.d), f.etag(e.d), time.Now().UTC().Format(time.RFC3339))
	}
	b.WriteString(`</ListBucketResult>`)
	w.Write([]byte(b.String()))
}

func xmlEscape(s string) string {
	var buf bytes.Buffer
	xml.EscapeText(&buf, []byte(s))
	return buf.String()
}

func testHarness(t *testing.T) (*fakeS3, *s3Client, *config) {
	t.Helper()
	f := newFakeS3()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	cfg := &config{bucket: "testbucket", endpoint: srv.URL, region: "us-west-004", concurrency: 16}
	cfg.readCred = credentials{keyID: "k", secret: "s", region: "us-west-004"}
	cfg.writeCred = cfg.readCred
	return f, newS3Client(srv.URL, "testbucket", cfg.readCred, 16), cfg
}

func blob(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*3) ^ seed
	}
	return b
}

// --- the tests ---------------------------------------------------------------

func TestPullSingleFileParallelRanged(t *testing.T) {
	f, c, cfg := testHarness(t)
	data := blob(40<<20, 1) // 40 MiB -> 5 parts at the 8 MiB floor
	f.put("base-models/m/model.safetensors", data)

	dir := t.TempDir()
	dst := filepath.Join(dir, "model.safetensors")
	st := newStats("pull", "", dst, false, cfg.concurrency)
	if err := runPull(context.Background(), c, cfg, "base-models/m/model.safetensors", dst, pullOpts{}, st); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("content mismatch (err=%v, len %d vs %d)", err, len(got), len(data))
	}
	if n := f.rangeGets.Load(); n < 5 {
		t.Errorf("expected >=5 ranged GETs (parallel split), got %d", n)
	}
	if p := f.maxPar.Load(); p < 2 {
		t.Errorf("expected concurrent requests, peak was %d", p)
	}
	t.Logf("40MiB in %d ranged GETs, peak concurrency %d", f.rangeGets.Load(), f.maxPar.Load())
}

func TestPullIsIdempotent(t *testing.T) {
	f, c, cfg := testHarness(t)
	for i, name := range []string{"a.safetensors", "b.safetensors", "config.json"} {
		f.put("base-models/m/"+name, blob((i+1)*10<<20, byte(i)))
	}
	dir := t.TempDir()

	first := newStats("pull", "", dir, false, cfg.concurrency)
	if err := runPull(context.Background(), c, cfg, "base-models/m", dir, pullOpts{}, first); err != nil {
		t.Fatal(err)
	}
	if first.done.Load() == 0 {
		t.Fatal("first pull transferred nothing")
	}

	// THE SECOND BUG: a re-pull onto a box that already has the weights must
	// transfer zero bytes. This is the resume case (parked box, preempted job).
	f.rangeGets.Store(0)
	second := newStats("pull", "", dir, false, cfg.concurrency)
	if err := runPull(context.Background(), c, cfg, "base-models/m", dir, pullOpts{}, second); err != nil {
		t.Fatal(err)
	}
	if second.done.Load() != 0 {
		t.Errorf("re-pull transferred %d bytes, want 0", second.done.Load())
	}
	if second.skippedObjs.Load() != 3 {
		t.Errorf("re-pull skipped %d objects, want 3", second.skippedObjs.Load())
	}
	if n := f.rangeGets.Load(); n != 0 {
		t.Errorf("re-pull issued %d ranged GETs, want 0", n)
	}
	t.Logf("re-pull: 0 bytes, %s skipped", humanBytes(second.skippedBytes.Load()))
}

// TestPullAdoptsPreexistingFiles: a box whose weights were pulled by the OLD
// rclone code has no .b2x state. Re-pulling 24 GB just to build one would
// defeat the purpose, so a size match must be adopted.
func TestPullAdoptsPreexistingFiles(t *testing.T) {
	f, c, cfg := testHarness(t)
	data := blob(20<<20, 9)
	f.put("base-models/m/w.bin", data)

	dir := t.TempDir()
	// simulate the legacy rclone-pulled tree: right bytes, no .b2x/ state
	if err := os.WriteFile(filepath.Join(dir, "w.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	st := newStats("pull", "", dir, false, cfg.concurrency)
	if err := runPull(context.Background(), c, cfg, "base-models/m", dir, pullOpts{}, st); err != nil {
		t.Fatal(err)
	}
	if st.done.Load() != 0 {
		t.Errorf("adopted legacy tree still transferred %d bytes", st.done.Load())
	}
	if _, err := os.Stat(filepath.Join(dir, stateDirName, stateFileName)); err != nil {
		t.Error("expected state to be written so the next run takes the cheap path")
	}
}

// TestPullResumesPartial: kill a pull mid-flight, restart, and only the missing
// parts should be fetched. rclone restarts an interrupted large file from zero.
func TestPullResumesPartial(t *testing.T) {
	f, c, cfg := testHarness(t)
	data := blob(80<<20, 5) // 10 parts
	f.put("checkpoints/run/ckpt.bin", data)

	dir := t.TempDir()
	dst := filepath.Join(dir, "ckpt.bin")

	// Interrupt after a few parts by cancelling the context from the server side.
	ctx, cancel := context.WithCancel(context.Background())
	var served atomic.Int64
	srvWrap := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			if served.Add(1) == 4 {
				go func() { time.Sleep(20 * time.Millisecond); cancel() }()
			}
		}
		f.ServeHTTP(w, r)
	}))
	defer srvWrap.Close()
	c1 := newS3Client(srvWrap.URL, "testbucket", cfg.readCred, 2)

	st := newStats("pull", "", dst, false, 2)
	err := runPull(ctx, c1, cfg, "checkpoints/run/ckpt.bin", dst, pullOpts{}, st)
	if err == nil {
		t.Skip("pull completed before the interrupt landed; timing-dependent")
	}
	if _, serr := os.Stat(dst); serr == nil {
		t.Fatal("interrupted pull promoted a partial file to its final path")
	}
	partialBytes := st.done.Load()
	if partialBytes == 0 {
		t.Skip("no parts landed before interrupt")
	}

	// Resume: must fetch strictly less than the whole object.
	st2 := newStats("pull", "", dst, false, cfg.concurrency)
	if err := runPull(context.Background(), c, cfg, "checkpoints/run/ckpt.bin", dst, pullOpts{}, st2); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("resumed file does not match the source")
	}
	if st2.done.Load() >= int64(len(data)) {
		t.Errorf("resume refetched %s of a %s object (expected a partial refetch)",
			humanBytes(st2.done.Load()), humanBytes(int64(len(data))))
	}
	t.Logf("interrupted after %s, resume fetched %s of %s",
		humanBytes(partialBytes), humanBytes(st2.done.Load()), humanBytes(int64(len(data))))
}

func TestPushMultipartRoundTrip(t *testing.T) {
	f, c, cfg := testHarness(t)
	dir := t.TempDir()
	data := blob(30<<20, 3)
	src := filepath.Join(dir, "ckpt.bin")
	if err := os.WriteFile(src, data, 0o644); err != nil {
		t.Fatal(err)
	}

	st := newStats("push", src, "", false, cfg.concurrency)
	if err := runPush(context.Background(), c, cfg, src, "checkpoints/run/ckpt.bin", pushOpts{}, st); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	got := f.objects["checkpoints/run/ckpt.bin"]
	md := f.meta["checkpoints/run/ckpt.bin"]
	f.mu.Unlock()
	if !bytes.Equal(got, data) {
		t.Fatalf("uploaded object differs (%d vs %d bytes)", len(got), len(data))
	}
	// Every b2x-written object carries its sha256 so a later pull can verify.
	sum := sha256.Sum256(data)
	if md["b2x-sha256"] != hex.EncodeToString(sum[:]) {
		t.Errorf("missing/incorrect b2x-sha256 metadata: %q", md["b2x-sha256"])
	}

	// And a pull with --verify must accept it.
	dst := filepath.Join(t.TempDir(), "back.bin")
	pst := newStats("pull", "", dst, false, cfg.concurrency)
	if err := runPull(context.Background(), c, cfg, "checkpoints/run/ckpt.bin", dst, pullOpts{verify: true}, pst); err != nil {
		t.Fatalf("verify pull failed: %v", err)
	}
}

func TestPushSkipsExisting(t *testing.T) {
	_, c, cfg := testHarness(t)
	dir := t.TempDir()
	for i, n := range []string{"a.bin", "b.bin"} {
		if err := os.WriteFile(filepath.Join(dir, n), blob((i+1)*4<<20, byte(i)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	st1 := newStats("push", dir, "", false, cfg.concurrency)
	if err := runPush(context.Background(), c, cfg, dir, "checkpoints/run", pushOpts{}, st1); err != nil {
		t.Fatal(err)
	}
	st2 := newStats("push", dir, "", false, cfg.concurrency)
	if err := runPush(context.Background(), c, cfg, dir, "checkpoints/run", pushOpts{}, st2); err != nil {
		t.Fatal(err)
	}
	if st2.done.Load() != 0 {
		t.Errorf("re-push uploaded %d bytes, want 0", st2.done.Load())
	}
	if st2.skippedObjs.Load() != 2 {
		t.Errorf("re-push skipped %d, want 2", st2.skippedObjs.Load())
	}
}

// TestPushNewestFirst pins the deadline-truncation policy: under a budget that
// cannot fit everything, the NEWEST checkpoint must be what survives.
func TestPushNewestFirst(t *testing.T) {
	f, c, cfg := testHarness(t)
	dir := t.TempDir()
	names := []string{"old.bin", "mid.bin", "new.bin"}
	for i, n := range names {
		p := filepath.Join(dir, n)
		if err := os.WriteFile(p, blob(1<<20, byte(i)), 0o644); err != nil {
			t.Fatal(err)
		}
		mt := time.Now().Add(time.Duration(i-3) * time.Hour)
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	var order []string
	var omu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" && !r.URL.Query().Has("uploadId") {
			omu.Lock()
			order = append(order, filepath.Base(r.URL.Path))
			omu.Unlock()
		}
		f.ServeHTTP(w, r)
	}))
	defer srv.Close()
	c2 := newS3Client(srv.URL, "testbucket", cfg.writeCred, 8)
	_ = c

	st := newStats("push", dir, "", false, cfg.concurrency)
	if err := runPush(context.Background(), c2, cfg, dir, "checkpoints/run", pushOpts{}, st); err != nil {
		t.Fatal(err)
	}
	omu.Lock()
	defer omu.Unlock()
	if len(order) != 3 || order[0] != "new.bin" || order[2] != "old.bin" {
		t.Errorf("upload order = %v, want newest first [new.bin mid.bin old.bin]", order)
	}
}

func TestPushMinAgeAndExclude(t *testing.T) {
	f, c, cfg := testHarness(t)
	dir := t.TempDir()
	for _, n := range []string{"keep.bin", "STATUS", "fresh.bin"} {
		if err := os.WriteFile(filepath.Join(dir, n), blob(1<<20, 1), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-time.Hour)
	os.Chtimes(filepath.Join(dir, "keep.bin"), old, old)
	os.Chtimes(filepath.Join(dir, "STATUS"), old, old)

	st := newStats("push", dir, "", false, cfg.concurrency)
	err := runPush(context.Background(), c, cfg, dir, "checkpoints/run",
		pushOpts{minAge: 45 * time.Second, excludes: []string{"STATUS"}}, st)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objects["checkpoints/run/keep.bin"]; !ok {
		t.Error("keep.bin should have been uploaded")
	}
	if _, ok := f.objects["checkpoints/run/STATUS"]; ok {
		t.Error("--exclude STATUS was not honored")
	}
	if _, ok := f.objects["checkpoints/run/fresh.bin"]; ok {
		t.Error("--min-age 45s was not honored for a just-written file")
	}
}

func TestRetryOn503(t *testing.T) {
	f, c, cfg := testHarness(t)
	f.put("x/y.bin", blob(1<<20, 2))
	f.failNext.Store(2) // two transient failures, then success

	dst := filepath.Join(t.TempDir(), "y.bin")
	st := newStats("pull", "", dst, false, cfg.concurrency)
	if err := runPull(context.Background(), c, cfg, "x/y.bin", dst, pullOpts{}, st); err != nil {
		t.Fatalf("retry did not recover from 2x503: %v", err)
	}
}

func TestNotFoundExitCode(t *testing.T) {
	_, c, cfg := testHarness(t)
	dst := filepath.Join(t.TempDir(), "nope")
	st := newStats("pull", "", dst, false, cfg.concurrency)
	err := runPull(context.Background(), c, cfg, "does/not/exist", dst, pullOpts{}, st)
	if err == nil {
		t.Fatal("expected an error for a missing prefix")
	}
	if code := report(err, cfg); code != exitNotFound {
		t.Errorf("exit code = %d, want %d (not-found)", code, exitNotFound)
	}
}

func TestIntegrityMismatchIsDetected(t *testing.T) {
	f, c, cfg := testHarness(t)
	data := blob(2<<20, 4)
	f.put("x/z.bin", data)
	f.mu.Lock()
	f.meta["x/z.bin"] = map[string]string{"b2x-sha256": strings.Repeat("0", 64)} // wrong on purpose
	f.mu.Unlock()

	dst := filepath.Join(t.TempDir(), "z.bin")
	st := newStats("pull", "", dst, false, cfg.concurrency)
	err := runPull(context.Background(), c, cfg, "x/z.bin", dst, pullOpts{verify: true}, st)
	if err == nil {
		t.Fatal("corrupt object was accepted")
	}
	if code := report(err, cfg); code != exitIntegrity {
		t.Errorf("exit code = %d, want %d (integrity)", code, exitIntegrity)
	}
	if _, serr := os.Stat(dst); serr == nil {
		t.Error("a failed-verify file was promoted to its final path")
	}
}

func TestStatsEnvFileForShellCallers(t *testing.T) {
	f, c, cfg := testHarness(t)
	f.put("x/a.bin", blob(9<<20, 6))
	dir := t.TempDir()
	dst := filepath.Join(dir, "a.bin")
	envFile := filepath.Join(dir, "stats.env")

	st := newStats("pull", "", dst, false, cfg.concurrency)
	if err := runPull(context.Background(), c, cfg, "x/a.bin", dst, pullOpts{}, st); err != nil {
		t.Fatal(err)
	}
	st.finish(envFile)

	b, err := os.ReadFile(envFile)
	if err != nil {
		t.Fatal(err)
	}
	// These exact names are what the migrated boot_mark pull_done lines consume.
	for _, want := range []string{"B2X_BYTES=", "B2X_SECS=", "B2X_MBPS=", "B2X_OBJECTS=", "B2X_SKIPPED_BYTES="} {
		if !strings.Contains(string(b), want) {
			t.Errorf("stats env missing %s:\n%s", want, b)
		}
	}
}

// TestProgressLineStaysGreppableByJobd: jobd.sh's _last_mbps greps a stats file
// for `[0-9.]+ [KMGT]?i?B/s`. An unmigrated reader must keep finding a rate.
func TestProgressLineStaysGreppableByJobd(t *testing.T) {
	st := newStats("pull", "a", "b", false, 32)
	st.addBytes(50 << 20)
	line := st.progressLine()
	if !strings.Contains(line, "MiB/s") {
		t.Errorf("progress line lost the MiB/s token jobd.sh greps for: %q", line)
	}
}

func TestNormKeyAcceptsRcloneSpelling(t *testing.T) {
	cfg := &config{bucket: "example-runs-bucket"}
	for _, in := range []string{
		"b2:example-runs-bucket/base-models/qwen",
		"b2w:example-runs-bucket/base-models/qwen",
		"/base-models/qwen",
		"base-models/qwen",
	} {
		if got := cfg.normKey(in); got != "base-models/qwen" {
			t.Errorf("normKey(%q) = %q", in, got)
		}
	}
}

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pat, rel string
		want     bool
	}{
		{"STATUS", "STATUS", true},
		{"STATUS", "out/STATUS", true},
		{"checkpoint-*/**", "checkpoint-100/model.bin", true},
		{"checkpoint-*/**", "final/model.bin", false},
		{"*.safetensors", "model-00001.safetensors", true},
		{"*.safetensors", "config.json", false},
	}
	for _, c := range cases {
		if got := globMatch(c.pat, c.rel); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.pat, c.rel, got, c.want)
		}
	}
}

// TestPushHashesOnlyThePlannedBytes pins the growing-file property.
//
// it.size is captured when push WALKS the tree; the upload plan moves exactly
// that many bytes. An actively-appended artifact (the checkpoint lane's NDJSON,
// a row at a time) is LONGER by the time uploadOne hashes it. Hashing "the
// file" rather than the planned extent recorded a b2x-sha256 over size+delta
// bytes for an object holding size bytes — so `pull --verify` would reject an
// object that is a perfectly correct prefix. Both arms are covered because the
// single-PUT and multipart paths read the source differently.
func TestPushHashesOnlyThePlannedBytes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		planned int
		grew    int
	}{
		{"singlePUT", 64 << 10, 8 << 10},
		{"multipart", 24 << 20, 3 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, c, cfg := testHarness(t)
			data := blob(tc.planned+tc.grew, 7)
			src := filepath.Join(t.TempDir(), "gens.ndjson")
			if err := os.WriteFile(src, data, 0o644); err != nil {
				t.Fatal(err)
			}
			// size < on-disk length == exactly the walk-then-append race.
			it := pushItem{abs: src, rel: "gens.ndjson", size: int64(tc.planned), mtime: time.Now()}
			st := newStats("push", src, "", false, cfg.concurrency)
			if err := uploadOne(context.Background(), c, it, "checkpoints/run/gens.ndjson", st); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			got := f.objects["checkpoints/run/gens.ndjson"]
			md := f.meta["checkpoints/run/gens.ndjson"]
			f.mu.Unlock()

			want := data[:tc.planned]
			if !bytes.Equal(got, want) {
				t.Fatalf("uploaded %d bytes, want the planned %d", len(got), tc.planned)
			}
			sum := sha256.Sum256(want)
			if md["b2x-sha256"] != hex.EncodeToString(sum[:]) {
				t.Errorf("b2x-sha256 does not cover the uploaded bytes: got %q want %q",
					md["b2x-sha256"], hex.EncodeToString(sum[:]))
			}
		})
	}
}

// TestPushRefusesAShrunkSource is the other half: a file SHORTER than the plan
// cannot satisfy it. The old multipart path swallowed the short ReadAt and
// uploaded buf's zero tail — a silent corruption the recorded sha256 could not
// catch, since it came from an independent read.
func TestPushRefusesAShrunkSource(t *testing.T) {
	for _, tc := range []struct {
		name    string
		onDisk  int
		planned int
	}{
		{"singlePUT", 32 << 10, 64 << 10},
		{"multipart", 20 << 20, 24 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, c, cfg := testHarness(t)
			src := filepath.Join(t.TempDir(), "ckpt.bin")
			if err := os.WriteFile(src, blob(tc.onDisk, 9), 0o644); err != nil {
				t.Fatal(err)
			}
			it := pushItem{abs: src, rel: "ckpt.bin", size: int64(tc.planned), mtime: time.Now()}
			st := newStats("push", src, "", false, cfg.concurrency)
			err := uploadOne(context.Background(), c, it, "checkpoints/run/ckpt.bin", st)
			if err == nil {
				t.Fatal("uploaded a source shorter than the plan, want an error")
			}
			if !strings.Contains(err.Error(), "shrank") && !strings.Contains(err.Error(), "short read") {
				t.Errorf("unhelpful error for a shrunk source: %v", err)
			}
		})
	}
}

// TestPrefixPullSurvivesA403OnTheProbeHEAD.
//
// runPull HEADs its source first to tell a single-file pull from a prefix pull.
// A prefix-restricted or bucketIds-restricted B2 key answers HeadObject on a
// key it cannot see with 403, NEVER 404 — it will not confirm or deny
// existence. Every fleet box carries exactly such a key (b2_mint_key.mint_pair's
// `-ro`: listFiles+readFiles on one bucketId), so treating that 403 as fatal
// made EVERY prefix pull on EVERY scoped box fail before it listed anything.
//
// Measured live 2026-08-27 against a freshly minted `-ro` key: `stat` on an
// existing object 200, `stat` on any absent key 403, `ls` fine, `pull <prefix>`
// exit 3 and zero bytes. Invisible in production because every call site is
// `b2x_pull … || <rclone line>` and the rclone line works — the same
// silent-fallback class test_transfer_sites.py exists for.
func TestPrefixPullSurvivesA403OnTheProbeHEAD(t *testing.T) {
	f, c, cfg := testHarness(t)
	f.headMissing403.Store(true)
	for _, n := range []string{"a.bin", "b.bin", "sub/c.bin"} {
		f.put("checkpoints/run-1/"+n, blob(4096, 3))
	}
	dst := t.TempDir()
	err := runPull(context.Background(), c, cfg, "checkpoints/run-1/", dst,
		pullOpts{}, newStats("pull", "s", dst, false, cfg.concurrency))
	if err != nil {
		t.Fatalf("prefix pull failed on a 403 probe HEAD: %v", err)
	}
	for _, n := range []string{"a.bin", "b.bin", "sub/c.bin"} {
		if _, serr := os.Stat(filepath.Join(dst, filepath.FromSlash(n))); serr != nil {
			t.Errorf("missing %s: %v", n, serr)
		}
	}
}

// A key that genuinely cannot read must still fail — the fall-through moves the
// verdict to the LIST, it does not remove it.
func TestPrefixPullStillFailsWhenTheListIsAlsoDenied(t *testing.T) {
	f, c, cfg := testHarness(t)
	f.headMissing403.Store(true)
	f.denyList.Store(true)
	err := runPull(context.Background(), c, cfg, "checkpoints/run-1/", t.TempDir(),
		pullOpts{}, newStats("pull", "s", "d", false, cfg.concurrency))
	if err == nil {
		t.Fatal("an unentitled key must still fail the pull")
	}
	if got := statusOf(err); got != 403 {
		t.Errorf("want the LIST's 403 to surface, got status %d (%v)", got, err)
	}
}
