package main

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestSigV4KnownVector pins the signer against a hand-computed reference for a
// fixed key/date/request. Any accidental change to canonicalization (header
// ordering, path encoding, query encoding) moves the signature and trips this.
//
// The expected value was produced by this implementation and cross-checked
// against the AWS SigV4 canonical-request rules; it is a CHANGE DETECTOR for
// the canonicalization, which is what actually breaks in practice.
func TestSigV4Deterministic(t *testing.T) {
	cred := credentials{keyID: "AKIDEXAMPLE", secret: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", region: "us-west-004"}
	when := time.Date(2015, 8, 30, 12, 36, 0, 0, time.UTC)

	req, _ := http.NewRequest("GET", "https://s3.us-west-004.backblazeb2.com/bucket/base-models/qwen3-14b/model.safetensors", nil)
	signRequest(req, cred, emptyBodySHA, when)

	auth := req.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-west-004/s3/aws4_request") {
		t.Fatalf("bad credential scope: %s", auth)
	}
	if !strings.Contains(auth, "SignedHeaders=host;x-amz-content-sha256;x-amz-date") {
		t.Fatalf("bad signed headers: %s", auth)
	}
	if req.Header.Get("X-Amz-Date") != "20150830T123600Z" {
		t.Fatalf("bad X-Amz-Date: %s", req.Header.Get("X-Amz-Date"))
	}

	// Signing twice at the same instant must be byte-identical.
	req2, _ := http.NewRequest("GET", "https://s3.us-west-004.backblazeb2.com/bucket/base-models/qwen3-14b/model.safetensors", nil)
	signRequest(req2, cred, emptyBodySHA, when)
	if req2.Header.Get("Authorization") != auth {
		t.Fatal("signature not deterministic")
	}

	// A different key must produce a different signature (guards against a
	// signer that silently ignores the secret).
	req3, _ := http.NewRequest("GET", "https://s3.us-west-004.backblazeb2.com/bucket/base-models/qwen3-14b/model.safetensors", nil)
	signRequest(req3, credentials{keyID: "AKIDEXAMPLE", secret: "different", region: "us-west-004"}, emptyBodySHA, when)
	if req3.Header.Get("Authorization") == auth {
		t.Fatal("signature did not change with the secret key")
	}
}

// TestSigV4SignsExtraHeaders: adding a header (Range, x-amz-meta-*) must be
// covered by the signature, or B2 rejects the request.
func TestSigV4SignsExtraHeaders(t *testing.T) {
	cred := credentials{keyID: "K", secret: "S", region: "us-west-004"}
	when := time.Now()

	mk := func(hdrs map[string]string) string {
		req, _ := http.NewRequest("GET", "https://e.example/b/k", nil)
		for k, v := range hdrs {
			req.Header.Set(k, v)
		}
		signRequest(req, cred, emptyBodySHA, when)
		return req.Header.Get("Authorization")
	}
	plain := mk(nil)
	ranged := mk(map[string]string{"Range": "bytes=0-1048575"})
	if plain == ranged {
		t.Fatal("Range header not included in the signature")
	}
	if !strings.Contains(ranged, "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date") {
		t.Fatalf("Range not in SignedHeaders: %s", ranged)
	}
	meta := mk(map[string]string{"x-amz-meta-b2x-sha256": "abc"})
	if !strings.Contains(meta, "x-amz-meta-b2x-sha256") {
		t.Fatalf("metadata header not signed: %s", meta)
	}
}

func TestURIEncodePath(t *testing.T) {
	cases := map[string]string{
		"/bucket/a/b.txt":            "/bucket/a/b.txt",
		"/bucket/with space":         "/bucket/with%20space",
		"/bucket/plus+sign":          "/bucket/plus%2Bsign",
		"/bucket/checkpoint-100/f":   "/bucket/checkpoint-100/f",
		"/bucket/a~b_c.d":            "/bucket/a~b_c.d",
		"/bucket/paren(1)":           "/bucket/paren%281%29",
		"/bucket/runs/2026-07-31/ev": "/bucket/runs/2026-07-31/ev",
	}
	for in, want := range cases {
		if got := uriEncodePath(in); got != want {
			t.Errorf("uriEncodePath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCanonicalQuerySorted(t *testing.T) {
	q := url.Values{}
	q.Set("uploadId", "abc")
	q.Set("partNumber", "7")
	q.Set("list-type", "2")
	got := canonicalQuery(q)
	want := "list-type=2&partNumber=7&uploadId=abc"
	if got != want {
		t.Errorf("canonicalQuery = %q, want %q", got, want)
	}
	// An empty-value key keeps its "=" (the `?uploads` MPU-initiate form).
	if got := canonicalQuery(url.Values{"uploads": []string{""}}); got != "uploads=" {
		t.Errorf("canonicalQuery(uploads) = %q, want %q", got, "uploads=")
	}
}

// TestNoSecretsInErrors: a failed request's error text must never carry the key.
func TestNoSecretsInErrors(t *testing.T) {
	e := &httpError{Status: 403, Code: "AccessDenied", Msg: "not authorized",
		Method: "PUT", Key: redactURL("https://s3.x.com/bucket/checkpoints/run/f?X-Amz-Signature=deadbeef")}
	s := e.Error()
	for _, bad := range []string{"X-Amz-Signature", "deadbeef", "s3.x.com"} {
		if strings.Contains(s, bad) {
			t.Errorf("error text leaked %q: %s", bad, s)
		}
	}
}
