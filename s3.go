package main

// s3.go — the minimal S3 surface b2x needs against the B2 S3-compatible endpoint.
//
// Deliberately small: ListObjectsV2, HeadObject, ranged GetObject, PutObject,
// and the three multipart calls. Every request goes through do(), which owns
// retry/backoff so no caller has to think about it.

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type s3Client struct {
	endpoint string // e.g. https://s3.us-west-004.backblazeb2.com
	bucket   string
	cred     credentials
	hc       *http.Client
	maxTries int
}

func newS3Client(endpoint, bucket string, cred credentials, conns int) *s3Client {
	// One shared transport with a connection pool sized to the concurrency we
	// actually run. The default MaxIdleConnsPerHost of 2 would silently
	// serialize teardown/setup of every ranged GET beyond the second — the
	// exact "you asked for N flows and got 2" shape we are here to kill.
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          conns * 2,
		MaxIdleConnsPerHost:   conns * 2,
		MaxConnsPerHost:       0, // unbounded; our scheduler is the limiter
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     false, // HTTP/1.1: we want N real TCP flows, not N HTTP/2 streams on one shaped flow
		WriteBufferSize:       256 << 10,
		ReadBufferSize:        256 << 10,
	}
	return &s3Client{
		endpoint: strings.TrimRight(endpoint, "/"),
		bucket:   bucket,
		cred:     cred,
		hc:       &http.Client{Transport: tr},
		maxTries: 6,
	}
}

// httpError carries the status of a failed S3 call without ever carrying
// credentials. Callers switch on Status for exit-code mapping.
type httpError struct {
	Status int
	Code   string
	Msg    string
	Method string
	Key    string
}

func (e *httpError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("%s %s: HTTP %d %s: %s", e.Method, e.Key, e.Status, e.Code, e.Msg)
	}
	return fmt.Sprintf("%s %s: HTTP %d", e.Method, e.Key, e.Status)
}

func statusOf(err error) int {
	var he *httpError
	if errors.As(err, &he) {
		return he.Status
	}
	return 0
}

type s3ErrBody struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

// url for a key. Path-style addressing: B2's S3 endpoint accepts it and it
// avoids a per-bucket DNS dependency.
func (c *s3Client) keyURL(key string, q url.Values) string {
	u := c.endpoint + "/" + c.bucket
	if key != "" {
		u += "/" + key
	}
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u
}

// retryable reports whether a failed attempt is worth repeating. 5xx and 429 are
// transient; 408 is a server-side read timeout. Network errors are retryable.
// 4xx other than those are terminal — retrying a 403 just burns the deadline.
func retryable(err error, status int) bool {
	if status >= 500 || status == 429 || status == 408 {
		return true
	}
	if status != 0 {
		return false
	}
	return err != nil
}

// do issues a signed request with bounded exponential backoff + jitter.
// body must be re-creatable: newBody is called once per attempt.
func (c *s3Client) do(ctx context.Context, method, rawURL string, hdrs map[string]string,
	newBody func() (io.ReadCloser, int64, string), wantStream bool) (*http.Response, error) {

	var lastErr error
	for attempt := 0; attempt < c.maxTries; attempt++ {
		if attempt > 0 {
			// 250ms, 500ms, 1s, 2s, 4s (+/- 25% jitter), respecting ctx.
			back := time.Duration(250<<uint(attempt-1)) * time.Millisecond
			if back > 8*time.Second {
				back = 8 * time.Second
			}
			jit := time.Duration(rand.Int63n(int64(back/2+1))) - back/4
			select {
			case <-time.After(back + jit):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		var body io.ReadCloser
		var clen int64
		payloadSHA := emptyBodySHA
		if newBody != nil {
			body, clen, payloadSHA = newBody()
		}
		req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
		if err != nil {
			return nil, err // malformed URL: not retryable
		}
		if clen > 0 {
			req.ContentLength = clen
		}
		for k, v := range hdrs {
			req.Header.Set(k, v)
		}
		signRequest(req, c.cred, payloadSHA, time.Now())

		resp, err := c.hc.Do(req)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if wantStream {
				return resp, nil
			}
			// Non-streaming call: read the body here and hand back a response
			// whose Body is an in-memory reader. Closing the real body inside
			// do() (the obvious `defer resp.Body.Close()`) would close it
			// before the CALLER ever reads it — which silently broke every
			// list/createMPU with "read on closed response body".
			buf, rerr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
			resp.Body.Close()
			if rerr != nil {
				lastErr = rerr
				continue
			}
			resp.Body = io.NopCloser(bytes.NewReader(buf))
			return resp, nil
		}

		// Error path: drain a bounded amount so the connection can be reused.
		slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		resp.Body.Close()
		var eb s3ErrBody
		_ = xml.Unmarshal(slurp, &eb)
		he := &httpError{Status: resp.StatusCode, Code: eb.Code, Msg: eb.Message, Method: method, Key: redactURL(rawURL)}
		lastErr = he
		if !retryable(he, resp.StatusCode) {
			return nil, he
		}
	}
	return nil, fmt.Errorf("after %d attempts: %w", c.maxTries, lastErr)
}

// --- ListObjectsV2 ----------------------------------------------------------

type s3Object struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

type listResult struct {
	XMLName     xml.Name `xml:"ListBucketResult"`
	IsTruncated bool     `xml:"IsTruncated"`
	NextToken   string   `xml:"NextContinuationToken"`
	Contents    []struct {
		Key          string    `xml:"Key"`
		Size         int64     `xml:"Size"`
		ETag         string    `xml:"ETag"`
		LastModified time.Time `xml:"LastModified"`
	} `xml:"Contents"`
}

// list enumerates every object under prefix, following continuation tokens.
func (c *s3Client) list(ctx context.Context, prefix string) ([]s3Object, error) {
	var out []s3Object
	token := ""
	for {
		q := url.Values{}
		q.Set("list-type", "2")
		q.Set("prefix", prefix)
		q.Set("max-keys", "1000")
		if token != "" {
			q.Set("continuation-token", token)
		}
		resp, err := c.do(ctx, "GET", c.keyURL("", q), nil, nil, false)
		if err != nil {
			return nil, err
		}
		var lr listResult
		dec := xml.NewDecoder(resp.Body)
		if err := dec.Decode(&lr); err != nil {
			return nil, fmt.Errorf("list %s: parse: %w", prefix, err)
		}
		for _, o := range lr.Contents {
			// B2 surfaces "directory placeholder" objects for some uploads;
			// a trailing-slash zero-byte key is a directory, not a file.
			if strings.HasSuffix(o.Key, "/") && o.Size == 0 {
				continue
			}
			out = append(out, s3Object{Key: o.Key, Size: o.Size, ETag: strings.Trim(o.ETag, `"`), LastModified: o.LastModified})
		}
		if !lr.IsTruncated || lr.NextToken == "" {
			return out, nil
		}
		token = lr.NextToken
	}
}

// head returns metadata for one key. A 404 surfaces as *httpError{Status:404}.
func (c *s3Client) head(ctx context.Context, key string) (s3Object, map[string]string, error) {
	resp, err := c.do(ctx, "HEAD", c.keyURL(key, nil), nil, nil, false)
	if err != nil {
		return s3Object{}, nil, err
	}
	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	lm, _ := http.ParseTime(resp.Header.Get("Last-Modified"))
	meta := map[string]string{}
	for k, v := range resp.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-meta-") && len(v) > 0 {
			meta[strings.TrimPrefix(lk, "x-amz-meta-")] = v[0]
		}
	}
	return s3Object{Key: key, Size: size, ETag: strings.Trim(resp.Header.Get("ETag"), `"`), LastModified: lm}, meta, nil
}

// getRange streams bytes [off, off+n) of key. Caller MUST close the body.
// n <= 0 means "to end of object".
func (c *s3Client) getRange(ctx context.Context, key string, off, n int64) (io.ReadCloser, error) {
	h := map[string]string{}
	if off > 0 || n > 0 {
		if n > 0 {
			h["Range"] = fmt.Sprintf("bytes=%d-%d", off, off+n-1)
		} else {
			h["Range"] = fmt.Sprintf("bytes=%d-", off)
		}
	}
	resp, err := c.do(ctx, "GET", c.keyURL(key, nil), h, nil, true)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// --- uploads ----------------------------------------------------------------

func (c *s3Client) putObject(ctx context.Context, key string, data []byte, meta map[string]string) error {
	sha := sha256Hex(data)
	h := map[string]string{}
	for k, v := range meta {
		h["x-amz-meta-"+k] = v
	}
	_, err := c.do(ctx, "PUT", c.keyURL(key, nil), h, func() (io.ReadCloser, int64, string) {
		return io.NopCloser(strings.NewReader(string(data))), int64(len(data)), sha
	}, false)
	return err
}

type initiateMPU struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	UploadID string   `xml:"UploadId"`
}

func (c *s3Client) createMPU(ctx context.Context, key string, meta map[string]string) (string, error) {
	q := url.Values{"uploads": []string{""}}
	h := map[string]string{}
	for k, v := range meta {
		h["x-amz-meta-"+k] = v
	}
	resp, err := c.do(ctx, "POST", c.keyURL(key, q), h, nil, false)
	if err != nil {
		return "", err
	}
	var r initiateMPU
	if err := xml.NewDecoder(resp.Body).Decode(&r); err != nil {
		return "", fmt.Errorf("createMPU %s: parse: %w", key, err)
	}
	if r.UploadID == "" {
		return "", fmt.Errorf("createMPU %s: empty UploadId", key)
	}
	return r.UploadID, nil
}

func (c *s3Client) uploadPart(ctx context.Context, key, uploadID string, partNum int, data []byte) (string, error) {
	q := url.Values{"partNumber": []string{strconv.Itoa(partNum)}, "uploadId": []string{uploadID}}
	sha := sha256Hex(data)
	resp, err := c.do(ctx, "PUT", c.keyURL(key, q), nil, func() (io.ReadCloser, int64, string) {
		return io.NopCloser(strings.NewReader(string(data))), int64(len(data)), sha
	}, false)
	if err != nil {
		return "", err
	}
	etag := strings.Trim(resp.Header.Get("ETag"), `"`)
	if etag == "" {
		return "", fmt.Errorf("uploadPart %s #%d: no ETag returned", key, partNum)
	}
	return etag, nil
}

type completePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMPUReq struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []completePart `xml:"Part"`
}

func (c *s3Client) completeMPU(ctx context.Context, key, uploadID string, parts []completePart) error {
	q := url.Values{"uploadId": []string{uploadID}}
	body, err := xml.Marshal(completeMPUReq{Parts: parts})
	if err != nil {
		return err
	}
	sha := sha256Hex(body)
	resp, err := c.do(ctx, "POST", c.keyURL(key, q), map[string]string{"Content-Type": "application/xml"},
		func() (io.ReadCloser, int64, string) {
			return io.NopCloser(strings.NewReader(string(body))), int64(len(body)), sha
		}, false)
	if err != nil {
		return err
	}
	// S3 can return 200 with an <Error> body on complete — check for it.
	slurp, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	if strings.Contains(string(slurp), "<Error>") {
		var eb s3ErrBody
		_ = xml.Unmarshal(slurp, &eb)
		return &httpError{Status: 200, Code: eb.Code, Msg: eb.Message, Method: "POST", Key: key}
	}
	return nil
}

// abortMPU releases the parts of an upload we are not going to complete. B2
// bills for orphaned parts, so the deadline path calls this best-effort.
func (c *s3Client) abortMPU(ctx context.Context, key, uploadID string) error {
	q := url.Values{"uploadId": []string{uploadID}}
	_, err := c.do(ctx, "DELETE", c.keyURL(key, q), nil, nil, false)
	return err
}

func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<url>"
	}
	return u.Path
}
