package p115

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AdapterError struct {
	Code       string
	Uncertain  bool
	RemoteCode string
	Step       string
}

func (e *AdapterError) Error() string {
	if e.RemoteCode != "" {
		return fmt.Sprintf("%s (remote=%s)", e.Code, e.RemoteCode)
	}
	return e.Code
}

type CloudFile struct {
	FID      string `json:"fid"`
	CID      string `json:"cid"`
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Pickcode string `json:"pickcode"`
	IsDir    bool   `json:"is_dir"`
	SHA1     string `json:"sha1"`
}

type FilePage struct {
	Files      []CloudFile
	NextOffset *int
}

type OfflineTask struct {
	InfoHash string `json:"info_hash"`
	URL      string `json:"url"`
	Name     string `json:"name"`
	Status   string `json:"status"` // downloading | success | failed
	TargetID string `json:"target_cid"`
	AddTime  int64  `json:"add_time"`
}

type ArchiveEntry struct {
	Path  string `json:"path"`
	Size  int64  `json:"size"`
	IsDir bool   `json:"is_dir"`
}

type DownloadInfo struct {
	URL     string
	Headers map[string]string
	SHA1    string
}

type LifeEvent struct {
	ID       string
	Type     int
	FID      string
	CID      string
	Name     string
	Pickcode string
	Size     int64
	SHA1     string
	Time     int64
}

type FileLocator struct {
	FID  string `json:"fid"`
	CID  string `json:"cid"`
	Size int64  `json:"size"`
	SHA1 string `json:"sha1"`
}

// Port defines the narrow boundary between Ark business logic and 115 remote APIs.
type Port interface {
	Files(ctx context.Context, cid string, offset int) (FilePage, error)
	DirectoryPath(ctx context.Context, cid string) ([]string, error)
	EnsureFolder(ctx context.Context, parentCID, name string) (string, error)
	MoveVerified(ctx context.Context, loc FileLocator, targetCID string) error
	DeleteVerified(ctx context.Context, loc FileLocator) error
	SubmitOffline(ctx context.Context, links []string, targetCID string) error
	OfflineSnapshot(ctx context.Context) ([]OfflineTask, error)
	PrepareExtract(ctx context.Context, pickcode, secret string) (string, error)
	InspectExtract(ctx context.Context, pickcode string) (string, error)
	StartExtract(ctx context.Context, pickcode, outputCID string) (string, error)
	ExtractStatus(ctx context.Context, remoteID string) (string, error)
	ArchiveManifest(ctx context.Context, pickcode string) ([]ArchiveEntry, error)
	LifeEvents(ctx context.Context, sinceUnix int64) ([]LifeEvent, error)
	DownloadURL(ctx context.Context, pickcode string) (DownloadInfo, error)
}

// Governor enforces circuit breaking and hourly/daily request budgets to protect the 115 account.
type Governor struct {
	mu           sync.Mutex
	circuitUntil time.Time
	circuitCode  string
	hourlyLimit  int
	hourBucket   int64
	hourCount    int
	totalCalls   int64
	totalErrors  int64
}

func NewGovernor(hourlyLimit int) *Governor {
	if hourlyLimit <= 0 {
		hourlyLimit = 600
	}
	return &Governor{hourlyLimit: hourlyLimit}
}

func (g *Governor) Before(now time.Time) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if now.Before(g.circuitUntil) {
		return &AdapterError{Code: "REMOTE_CIRCUIT_OPEN", Step: g.circuitCode}
	}
	bucket := now.Unix() / 3600
	if bucket != g.hourBucket {
		g.hourBucket = bucket
		g.hourCount = 0
	}
	if g.hourCount >= g.hourlyLimit {
		return &AdapterError{Code: "REQUEST_BUDGET_EXCEEDED"}
	}
	g.hourCount++
	g.totalCalls++
	return nil
}

func (g *Governor) Failure(code string, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.totalErrors++
	switch code {
	case "AUTH_OR_CHALLENGE_REQUIRED":
		g.circuitUntil = now.Add(30 * time.Minute)
		g.circuitCode = code
	case "REMOTE_RATE_LIMIT":
		g.circuitUntil = now.Add(5 * time.Minute)
		g.circuitCode = code
	}
}

func (g *Governor) ResetCircuit() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.circuitUntil = time.Time{}
	g.circuitCode = ""
}

func (g *Governor) Stats() map[string]any {
	g.mu.Lock()
	defer g.mu.Unlock()
	return map[string]any{
		"hourly_limit": g.hourlyLimit,
		"hour_used":    g.hourCount,
		"total_calls":  g.totalCalls,
		"total_errors": g.totalErrors,
		"circuit_open": time.Now().Before(g.circuitUntil),
		"circuit_code": g.circuitCode,
	}
}

type Client struct {
	cookie     string
	userAgent  string
	httpClient *http.Client
	gov        *Governor

	// Overridable base URLs for deterministic contract tests.
	webAPIBase  string
	lixianBase  string
	proAPIBase  string
	lifeAPIBase string

	reqMu       sync.Mutex
	nextReqAt   time.Time
	minInterval time.Duration

	folderMu sync.Mutex

	snapMu    sync.Mutex
	snapAt    time.Time
	snapItems []OfflineTask

	signMu    sync.Mutex
	signAt    time.Time
	signUID   string
	signVal   string
	signTime  int64
}

func NewClient(cookie string, gov *Governor) (*Client, error) {
	cookie = strings.TrimSpace(cookie)
	if cookie == "" {
		return nil, errors.New("COOKIE_REQUIRED")
	}
	if gov == nil {
		gov = NewGovernor(600)
	}
	return &Client{
		cookie:    cookie,
		userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
		httpClient: &http.Client{
			Timeout: 45 * time.Second,
		},
		gov:         gov,
		webAPIBase:  "https://webapi.115.com",
		lixianBase:  "https://115.com",
		proAPIBase:  "https://proapi.115.com",
		lifeAPIBase: "https://lifebehavior.115.com",
		minInterval: 1 * time.Second,
	}, nil
}

func (c *Client) doJSON(ctx context.Context, method, rawURL string, form url.Values, mutation bool) (map[string]any, error) {
	if err := c.gov.Before(time.Now()); err != nil {
		return nil, err
	}
	c.reqMu.Lock()
	defer c.reqMu.Unlock()

	if wait := time.Until(c.nextReqAt); wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	c.nextReqAt = time.Now().Add(c.minInterval)

	var bodyReader io.Reader
	if form != nil {
		bodyReader = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, bodyReader)
	if err != nil {
		return nil, &AdapterError{Code: "REQUEST_BUILD_FAILED"}
	}
	req.Header.Set("Cookie", c.cookie)
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Referer", "https://115.com/")
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		c.gov.Failure("REMOTE_UNAVAILABLE", time.Now())
		// Never include raw HTTP error text which could embed signed URLs or tokens.
		return nil, &AdapterError{Code: "REMOTE_UNAVAILABLE", Uncertain: mutation}
	}
	defer resp.Body.Close()

	limited := io.LimitReader(resp.Body, 4*1024*1024)
	raw, err := io.ReadAll(limited)
	if err != nil {
		c.gov.Failure("REMOTE_UNAVAILABLE", time.Now())
		return nil, &AdapterError{Code: "REMOTE_UNAVAILABLE", Uncertain: mutation}
	}

	var parsed map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&parsed); err != nil {
		return nil, &AdapterError{Code: "INVALID_REMOTE_RESPONSE", Uncertain: mutation}
	}
	if state, ok := parsed["state"].(bool); ok && !state {
		code := firstString(parsed, "errno", "errcode", "errNo")
		switch code {
		case "99", "401", "911", "990009":
			c.gov.Failure("AUTH_OR_CHALLENGE_REQUIRED", time.Now())
			return nil, &AdapterError{Code: "AUTH_OR_CHALLENGE_REQUIRED", RemoteCode: code}
		case "429", "10001":
			c.gov.Failure("REMOTE_RATE_LIMIT", time.Now())
			return nil, &AdapterError{Code: "REMOTE_RATE_LIMIT", RemoteCode: code}
		default:
			return nil, &AdapterError{Code: "REMOTE_REJECTED", RemoteCode: code}
		}
	}
	return parsed, nil
}

func (c *Client) Files(ctx context.Context, cid string, offset int) (FilePage, error) {
	q := url.Values{
		"aid":      {"1"},
		"cid":      {cid},
		"offset":   {strconv.Itoa(offset)},
		"show_dir": {"1"},
		"limit":    {"115"},
		"cur":      {"1"},
		"format":   {"json"},
	}
	resp, err := c.doJSON(ctx, http.MethodGet, c.webAPIBase+"/files?"+q.Encode(), nil, false)
	if err != nil {
		return FilePage{}, err
	}
	// Verify returned breadcrumb path ends with requested CID (prevents silent fallback to root).
	if cid != "0" {
		if !verifyBreadcrumb(resp["path"], cid) {
			return FilePage{}, &AdapterError{Code: "CID_NOT_VERIFIED", Step: cid}
		}
	}
	rawList, ok := resp["data"].([]any)
	if !ok {
		return FilePage{}, &AdapterError{Code: "INVALID_FILE_LIST"}
	}
	files := make([]CloudFile, 0, len(rawList))
	for _, entry := range rawList {
		m, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		fid := firstString(m, "fid")
		dirCID := firstString(m, "cid")
		isDir := fid == ""
		id := fid
		if isDir {
			id = dirCID
		}
		files = append(files, CloudFile{
			FID:      id,
			CID:      cid,
			Name:     firstString(m, "n"),
			Size:     asInt64(m["s"]),
			Pickcode: firstString(m, "pc"),
			IsDir:    isDir,
			SHA1:     strings.ToLower(firstString(m, "sha", "sha1")),
		})
	}
	count := int(asInt64(resp["count"]))
	var nextOffset *int
	if len(rawList) > 0 && offset+len(rawList) < count {
		n := offset + len(rawList)
		nextOffset = &n
	}
	return FilePage{Files: files, NextOffset: nextOffset}, nil
}

func (c *Client) DirectoryPath(ctx context.Context, cid string) ([]string, error) {
	q := url.Values{
		"aid": {"1"}, "cid": {cid}, "limit": {"1"}, "offset": {"0"}, "show_dir": {"1"}, "cur": {"1"}, "format": {"json"},
	}
	resp, err := c.doJSON(ctx, http.MethodGet, c.webAPIBase+"/files?"+q.Encode(), nil, false)
	if err != nil {
		return nil, err
	}
	parts := extractBreadcrumb(resp["path"])
	if cid != "0" && (len(parts) == 0 || parts[len(parts)-1] != cid) {
		return nil, &AdapterError{Code: "CID_NOT_VERIFIED", Step: cid}
	}
	if len(parts) == 0 {
		return []string{"0"}, nil
	}
	return parts, nil
}

func (c *Client) EnsureFolder(ctx context.Context, parentCID, name string) (string, error) {
	c.folderMu.Lock()
	defer c.folderMu.Unlock()

	offset := 0
	for range 100 {
		page, err := c.Files(ctx, parentCID, offset)
		if err != nil {
			return "", err
		}
		var matches []CloudFile
		for _, f := range page.Files {
			if f.IsDir && f.Name == name {
				matches = append(matches, f)
			}
		}
		if len(matches) > 1 {
			return "", &AdapterError{Code: "AMBIGUOUS_FOLDER", Step: name}
		}
		if len(matches) == 1 {
			return matches[0].FID, nil
		}
		if page.NextOffset == nil {
			break
		}
		offset = *page.NextOffset
	}

	form := url.Values{"pid": {parentCID}, "cname": {name}}
	resp, err := c.doJSON(ctx, http.MethodPost, c.webAPIBase+"/files/add", form, true)
	if err != nil {
		return "", err
	}
	cid := firstString(resp, "cid", "file_id")
	if cid == "" {
		if data, ok := resp["data"].(map[string]any); ok {
			cid = firstString(data, "cid", "file_id")
		}
	}
	if cid == "" {
		return "", &AdapterError{Code: "FOLDER_CREATE_UNKNOWN", Uncertain: true}
	}
	return cid, nil
}

func (c *Client) verifyLocator(ctx context.Context, loc FileLocator) (CloudFile, error) {
	offset := 0
	for range 100 {
		page, err := c.Files(ctx, loc.CID, offset)
		if err != nil {
			return CloudFile{}, err
		}
		for _, f := range page.Files {
			if f.FID == loc.FID {
				if f.Size != loc.Size || (loc.SHA1 != "" && !strings.EqualFold(f.SHA1, loc.SHA1)) {
					return CloudFile{}, &AdapterError{Code: "SOURCE_VERSION_NOT_VERIFIED"}
				}
				return f, nil
			}
		}
		if page.NextOffset == nil {
			break
		}
		offset = *page.NextOffset
	}
	return CloudFile{}, &AdapterError{Code: "SOURCE_VERSION_NOT_VERIFIED"}
}

func (c *Client) MoveVerified(ctx context.Context, loc FileLocator, targetCID string) error {
	if _, err := c.verifyLocator(ctx, loc); err != nil {
		return err
	}
	form := url.Values{"fid[0]": {loc.FID}, "pid": {targetCID}}
	_, err := c.doJSON(ctx, http.MethodPost, c.webAPIBase+"/files/move", form, true)
	return err
}

func (c *Client) DeleteVerified(ctx context.Context, loc FileLocator) error {
	if _, err := c.verifyLocator(ctx, loc); err != nil {
		return err
	}
	form := url.Values{"fid[0]": {loc.FID}, "ignore_warn": {"1"}}
	_, err := c.doJSON(ctx, http.MethodPost, c.webAPIBase+"/rb/delete", form, true)
	return err
}

func (c *Client) offlineSign(ctx context.Context) (uid, sign string, ts int64, err error) {
	c.signMu.Lock()
	if c.signVal != "" && time.Since(c.signAt) < 10*time.Minute {
		uid, sign, ts = c.signUID, c.signVal, c.signTime
		c.signMu.Unlock()
		return uid, sign, ts, nil
	}
	c.signMu.Unlock()

	rawURL := fmt.Sprintf("%s/?ct=offline&ac=space&_=%d", c.lixianBase, time.Now().UnixMilli())
	resp, err := c.doJSON(ctx, http.MethodGet, rawURL, nil, false)
	if err != nil {
		return "", "", 0, err
	}
	sign = firstString(resp, "sign")
	ts = asInt64(resp["time"])
	uid = extractUIDFromCookie(c.cookie)
	if sign == "" {
		return "", "", 0, &AdapterError{Code: "OFFLINE_SIGN_UNAVAILABLE"}
	}
	c.signMu.Lock()
	c.signAt = time.Now()
	c.signUID, c.signVal, c.signTime = uid, sign, ts
	c.signMu.Unlock()
	return uid, sign, ts, nil
}

func (c *Client) SubmitOffline(ctx context.Context, links []string, targetCID string) error {
	if len(links) == 0 || len(links) > 10 {
		return errors.New("SUBMIT_BATCH_LIMIT_10")
	}
	uid, sign, ts, err := c.offlineSign(ctx)
	if err != nil {
		return err
	}
	form := url.Values{
		"wp_path_id": {targetCID},
		"uid":        {uid},
		"sign":       {sign},
		"time":       {strconv.FormatInt(ts, 10)},
	}
	for i, l := range links {
		form.Set(fmt.Sprintf("url[%d]", i), l)
	}
	resp, err := c.doJSON(ctx, http.MethodPost, c.lixianBase+"/web/lixian/?ct=lixian&ac=add_task_urls", form, true)
	if err != nil {
		return err
	}
	if results, ok := resp["result"].([]any); ok {
		for _, item := range results {
			if m, ok := item.(map[string]any); ok {
				if st, exists := m["state"].(bool); exists && !st {
					return &AdapterError{Code: "PARTIAL_SUBMISSION_REQUIRES_RECONCILIATION", Uncertain: true}
				}
			}
		}
	}
	c.InvalidateOfflineCache()
	return nil
}

func (c *Client) InvalidateOfflineCache() {
	c.snapMu.Lock()
	c.snapAt = time.Time{}
	c.snapMu.Unlock()
}

func (c *Client) OfflineSnapshot(ctx context.Context) ([]OfflineTask, error) {
	c.snapMu.Lock()
	defer c.snapMu.Unlock()

	if time.Since(c.snapAt) < 20*time.Second && c.snapItems != nil {
		out := make([]OfflineTask, len(c.snapItems))
		copy(out, c.snapItems)
		return out, nil
	}

	uid := extractUIDFromCookie(c.cookie)
	form := url.Values{"page": {"1"}, "page_size": {"100"}, "uid": {uid}}
	resp, err := c.doJSON(ctx, http.MethodPost, c.lixianBase+"/web/lixian/?ct=lixian&ac=task_lists", form, false)
	if err != nil {
		return nil, err
	}
	rawList, _ := resp["tasks"].([]any)
	if rawList == nil {
		if data, ok := resp["data"].([]any); ok {
			rawList = data
		} else if dm, ok := resp["data"].(map[string]any); ok {
			if l, ok := dm["tasks"].([]any); ok {
				rawList = l
			} else if l, ok := dm["list"].([]any); ok {
				rawList = l
			}
		}
	}
	if rawList == nil {
		rawList = []any{}
	}
	tasks := make([]OfflineTask, 0, len(rawList))
	for _, entry := range rawList {
		m, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		stNum := asInt64(m["status"])
		st := "downloading"
		if stNum == 2 {
			st = "success"
		} else if stNum < 0 {
			st = "failed"
		}
		tasks = append(tasks, OfflineTask{
			InfoHash: strings.ToLower(firstString(m, "info_hash", "hash")),
			URL:      firstString(m, "url"),
			Name:     firstString(m, "name", "file_name"),
			Status:   st,
			TargetID: firstString(m, "wp_path_id"),
			AddTime:  asInt64(m["add_time"]),
		})
	}
	c.snapAt = time.Now()
	c.snapItems = tasks
	out := make([]OfflineTask, len(tasks))
	copy(out, tasks)
	return out, nil
}

func mapUnzipStatus(resp map[string]any) string {
	data, ok := resp["data"].(map[string]any)
	if !ok {
		return "unknown"
	}
	raw, exists := data["unzip_status"]
	if !exists {
		return "unknown"
	}
	switch asInt64(raw) {
	case 0, 1:
		return "running"
	case 2, 3, 7:
		return "failed"
	case 4:
		return "success"
	case 6:
		return "password"
	default:
		return "unknown"
	}
}

func (c *Client) PrepareExtract(ctx context.Context, pickcode, secret string) (string, error) {
	form := url.Values{"pick_code": {pickcode}, "secret": {secret}}
	resp, err := c.doJSON(ctx, http.MethodPost, c.webAPIBase+"/files/push_extract", form, true)
	if err != nil {
		return "", err
	}
	return mapUnzipStatus(resp), nil
}

func (c *Client) InspectExtract(ctx context.Context, pickcode string) (string, error) {
	q := url.Values{"pick_code": {pickcode}}
	resp, err := c.doJSON(ctx, http.MethodGet, c.webAPIBase+"/files/push_extract?"+q.Encode(), nil, false)
	if err != nil {
		return "", err
	}
	return mapUnzipStatus(resp), nil
}

func (c *Client) StartExtract(ctx context.Context, pickcode, outputCID string) (string, error) {
	form := url.Values{"pick_code": {pickcode}, "to_pid": {outputCID}, "extract_file": {""}, "paths": {""}}
	resp, err := c.doJSON(ctx, http.MethodPost, c.webAPIBase+"/files/extract_file", form, true)
	if err != nil {
		return "", err
	}
	remoteID := firstString(resp, "extract_id")
	if remoteID == "" {
		if data, ok := resp["data"].(map[string]any); ok {
			remoteID = firstString(data, "extract_id")
		}
	}
	if remoteID == "" {
		return "", &AdapterError{Code: "EXTRACT_SUBMISSION_UNKNOWN", Uncertain: true}
	}
	return remoteID, nil
}

func (c *Client) ExtractStatus(ctx context.Context, remoteID string) (string, error) {
	q := url.Values{"extract_id": {remoteID}}
	resp, err := c.doJSON(ctx, http.MethodGet, c.webAPIBase+"/files/extract_progress?"+q.Encode(), nil, false)
	if err != nil {
		return "", err
	}
	data, ok := resp["data"].(map[string]any)
	if !ok || len(data) == 0 {
		return "unknown", nil
	}
	if asInt64(data["extract_status"]) == -1 {
		return "failed", nil
	}
	if firstString(data, "percent", "progress") == "100" {
		return "success", nil
	}
	return "running", nil
}

func (c *Client) ArchiveManifest(ctx context.Context, pickcode string) ([]ArchiveEntry, error) {
	queue := []string{""}
	var out []ArchiveEntry
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		marker := ""
		for range 100 {
			q := url.Values{"pick_code": {pickcode}, "file_name": {cur}, "next_marker": {marker}}
			resp, err := c.doJSON(ctx, http.MethodGet, c.webAPIBase+"/files/extract_list?"+q.Encode(), nil, false)
			if err != nil {
				return nil, err
			}
			data, ok := resp["data"].(map[string]any)
			if !ok {
				return nil, &AdapterError{Code: "INVALID_ARCHIVE_MANIFEST"}
			}
			items, ok := data["list"].([]any)
			if !ok {
				return nil, &AdapterError{Code: "INVALID_ARCHIVE_MANIFEST"}
			}
			for _, raw := range items {
				m, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				name := firstString(m, "file_name")
				if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
					return nil, &AdapterError{Code: "UNSAFE_ARCHIVE_PATH"}
				}
				rel := name
				if cur != "" {
					rel = path.Join(cur, name)
				}
				isDir := asInt64(m["file_category"]) == 0
				if isDir {
					queue = append(queue, rel)
				} else {
					out = append(out, ArchiveEntry{
						Path:  rel,
						Size:  asInt64(m["file_size"]),
						IsDir: false,
					})
				}
				if len(out)+len(queue) > 10000 {
					return nil, &AdapterError{Code: "ARCHIVE_MANIFEST_BUDGET"}
				}
			}
			marker = firstString(data, "next_marker")
			if marker == "" {
				break
			}
		}
	}
	return out, nil
}

func (c *Client) LifeEvents(ctx context.Context, sinceUnix int64) ([]LifeEvent, error) {
	q := url.Values{
		"type":       {""},
		"limit":      {"100"},
		"start_time": {strconv.FormatInt(sinceUnix, 10)},
	}
	resp, err := c.doJSON(ctx, http.MethodGet, c.lifeAPIBase+"/api/1.0/web/1.0/history/list?"+q.Encode(), nil, false)
	if err != nil {
		return nil, err
	}
	data, _ := resp["data"].(map[string]any)
	rawList, _ := data["list"].([]any)
	events := make([]LifeEvent, 0, len(rawList))
	for _, entry := range rawList {
		m, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		events = append(events, LifeEvent{
			ID:       firstString(m, "id"),
			Type:     int(asInt64(m["type"])),
			FID:      firstString(m, "file_id", "fid"),
			CID:      firstString(m, "parent_id", "cid"),
			Name:     firstString(m, "file_name", "name"),
			Pickcode: firstString(m, "pick_code", "pc"),
			Size:     asInt64(m["file_size"]),
			SHA1:     strings.ToLower(firstString(m, "sha1")),
			Time:     asInt64(m["create_time"]),
		})
	}
	return events, nil
}

func (c *Client) DownloadURL(ctx context.Context, pickcode string) (DownloadInfo, error) {
	key, err := GenerateM115Key()
	if err != nil {
		return DownloadInfo{}, &AdapterError{Code: "DOWNLOAD_CAPABILITY_UNAVAILABLE"}
	}
	payload, _ := json.Marshal(map[string]string{"pickcode": pickcode})
	encoded := M115Encode(payload, key)
	rawURL := fmt.Sprintf("%s/app/chrome/downurl?t=%d", c.proAPIBase, time.Now().Unix())
	resp, err := c.doJSON(ctx, http.MethodPost, rawURL, url.Values{"data": {encoded}}, false)
	if err != nil {
		return DownloadInfo{}, err
	}
	encData := firstString(resp, "data")
	if encData == "" {
		return DownloadInfo{}, &AdapterError{Code: "DOWNLOAD_CAPABILITY_UNAVAILABLE"}
	}
	plain, err := M115Decode(encData, key)
	if err != nil {
		return DownloadInfo{}, &AdapterError{Code: "DOWNLOAD_CAPABILITY_UNAVAILABLE"}
	}
	var decoded map[string]any
	if err := json.Unmarshal(plain, &decoded); err != nil {
		return DownloadInfo{}, &AdapterError{Code: "DOWNLOAD_CAPABILITY_UNAVAILABLE"}
	}
	for _, val := range decoded {
		if m, ok := val.(map[string]any); ok {
			if urlObj, ok := m["url"].(map[string]any); ok {
				if u := firstString(urlObj, "url"); u != "" {
					return DownloadInfo{
						URL: u,
						Headers: map[string]string{
							"User-Agent": c.userAgent,
							"Cookie":     c.cookie,
						},
						SHA1: strings.ToLower(firstString(m, "sha1")),
					}, nil
				}
			}
		}
	}
	return DownloadInfo{}, &AdapterError{Code: "DOWNLOAD_CAPABILITY_UNAVAILABLE"}
}

func verifyBreadcrumb(raw any, cid string) bool {
	parts := extractBreadcrumb(raw)
	return len(parts) > 0 && parts[len(parts)-1] == cid
}

func extractBreadcrumb(raw any) []string {
	list, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			if id := firstString(m, "cid", "file_id"); id != "" {
				out = append(out, id)
			}
		}
	}
	return out
}

func extractUIDFromCookie(cookie string) string {
	for _, part := range strings.Split(cookie, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 && strings.EqualFold(kv[0], "UID") {
			if idx := strings.IndexByte(kv[1], '_'); idx > 0 {
				return kv[1][:idx]
			}
			return kv[1]
		}
	}
	return ""
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		v, ok := m[k]
		if !ok || v == nil {
			continue
		}
		switch t := v.(type) {
		case string:
			if t != "" {
				return t
			}
		case float64:
			return strconv.FormatInt(int64(t), 10)
		case json.Number:
			return t.String()
		}
	}
	return ""
}

func asInt64(v any) int64 {
	switch t := v.(type) {
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return n
		}
		if f, err := t.Float64(); err == nil {
			return int64(f)
		}
		return 0
	case float64:
		return int64(t)
	case int64:
		return t
	case int:
		return int64(t)
	case string:
		n, _ := strconv.ParseInt(t, 10, 64)
		return n
	default:
		return 0
	}
}
