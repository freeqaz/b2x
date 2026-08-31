package main

// sigv4.go — AWS Signature Version 4 for the B2 S3-compatible endpoint.
//
// Hand-rolled and stdlib-only ON PURPOSE. b2x must be a single static binary
// that runs on a bare box with nothing installed; every dependency we add is a
// dependency the build (and any future rebuild) has to resolve. The S3 surface
// b2x needs is tiny — ListObjectsV2, HeadObject, ranged GetObject, and the
// three multipart calls — so the signer is ~120 lines and is covered by the
// AWS-published test vectors in sigv4_test.go.
//
// SECRETS: nothing in this file ever logs. The signing key derivation and the
// Authorization header stay inside the request; redact() in errors.go is what
// guards anything that escapes.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	sigAlgorithm  = "AWS4-HMAC-SHA256"
	sigService    = "s3"
	emptyBodySHA  = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	unsignedPaylo = "UNSIGNED-PAYLOAD"
)

type credentials struct {
	keyID  string
	secret string
	region string
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// uriEncodePath encodes each path segment per SigV4 rules (RFC 3986, but "/"
// stays literal). Go's url.EscapedPath is close but escapes too little for some
// characters S3 cares about, so we do it explicitly.
func uriEncodePath(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		segs[i] = uriEncode(s)
	}
	return strings.Join(segs, "/")
}

func uriEncode(s string) string {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if strings.IndexByte(unreserved, c) >= 0 {
			b.WriteByte(c)
		} else {
			b.WriteString("%")
			b.WriteString(strings.ToUpper(hex.EncodeToString([]byte{c})))
		}
	}
	return b.String()
}

// canonicalQuery builds the SigV4 canonical query string: sorted by encoded key,
// every key and value URI-encoded, empty values keep their "=".
func canonicalQuery(q url.Values) string {
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		vals := append([]string(nil), q[k]...)
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, uriEncode(k)+"="+uriEncode(v))
		}
	}
	return strings.Join(parts, "&")
}

// signRequest signs req in place. payloadSHA is the hex sha256 of the body, or
// unsignedPaylo for streamed bodies whose bytes we do not want to buffer.
func signRequest(req *http.Request, cred credentials, payloadSHA string, now time.Time) {
	amzDate := now.UTC().Format("20060102T150405Z")
	dateStamp := now.UTC().Format("20060102")

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadSHA)
	if req.Host == "" {
		req.Host = req.URL.Host
	}

	// --- canonical headers: every header we send, lowercased and sorted ------
	// Signing ALL headers (rather than a hardcoded host;x-amz-* subset) means a
	// caller adding a header can never silently invalidate the signature.
	type hdr struct{ k, v string }
	hs := []hdr{{"host", req.Host}}
	for k, vs := range req.Header {
		lk := strings.ToLower(k)
		if lk == "authorization" || lk == "content-length" || lk == "user-agent" {
			continue // excluded by convention / rewritten by the transport
		}
		hs = append(hs, hdr{lk, strings.TrimSpace(strings.Join(vs, ","))})
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i].k < hs[j].k })

	var canonHeaders, signedHeaders strings.Builder
	for i, h := range hs {
		canonHeaders.WriteString(h.k + ":" + h.v + "\n")
		if i > 0 {
			signedHeaders.WriteString(";")
		}
		signedHeaders.WriteString(h.k)
	}

	canonicalRequest := strings.Join([]string{
		req.Method,
		uriEncodePath(req.URL.Path),
		canonicalQuery(req.URL.Query()),
		canonHeaders.String(),
		signedHeaders.String(),
		payloadSHA,
	}, "\n")

	scope := strings.Join([]string{dateStamp, cred.region, sigService, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		sigAlgorithm,
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+cred.secret), dateStamp)
	kRegion := hmacSHA256(kDate, cred.region)
	kService := hmacSHA256(kRegion, sigService)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	req.Header.Set("Authorization", sigAlgorithm+
		" Credential="+cred.keyID+"/"+scope+
		", SignedHeaders="+signedHeaders.String()+
		", Signature="+signature)
}
