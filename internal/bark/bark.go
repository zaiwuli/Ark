package bark

import (
	"context"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/zaiwuli/Ark/internal/store"
)

const (
	DefaultWindowDuration = 90 * time.Second
	maxFieldLen           = 4096
)

var pathLineRe = regexp.MustCompile(`(?im)(?:路径|path)\s*[：:]\s*([^\r\n]+)`)

type Incoming struct {
	Title string
	Body  string
	Group string
}

type Target struct {
	SourceID     string
	ReportedPath string
	RelativePath string
}

type Receiver struct {
	store     *store.Store
	windowDur time.Duration
	onTrigger func(ctx context.Context, targets []Target)

	mu            sync.Mutex
	activeWinID   string
	activeCount   int
	activeTargets map[string]Target
	windowTimer   *time.Timer
}

func NewReceiver(st *store.Store, windowDur time.Duration, onTrigger func(ctx context.Context, targets []Target)) *Receiver {
	if windowDur <= 0 {
		windowDur = DefaultWindowDuration
	}
	return &Receiver{
		store:     st,
		windowDur: windowDur,
		onTrigger: onTrigger,
	}
}

func (r *Receiver) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.windowTimer != nil {
		r.windowTimer.Stop()
		r.windowTimer = nil
	}
}

// NormalizePath cleans URL-encoded slashes, backslashes, and rejects path traversal segments.
func NormalizePath(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}
	if unesc, err := url.PathUnescape(s); err == nil {
		s = unesc
	}
	s = strings.ReplaceAll(s, "\\", "/")
	parts := make([]string, 0, 8)
	for _, seg := range strings.Split(s, "/") {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		if seg == "." || seg == ".." {
			return ""
		}
		parts = append(parts, seg)
	}
	if len(parts) == 0 {
		return ""
	}
	return "/" + strings.Join(parts, "/")
}

func (r *Receiver) Ingest(ctx context.Context, in Incoming) (store.BarkMessage, error) {
	title := truncate(strings.TrimSpace(in.Title), maxFieldLen)
	body := truncate(strings.TrimSpace(in.Body), maxFieldLen)
	group := truncate(strings.TrimSpace(in.Group), 256)

	matchedSourceID, reportedPath, relPath := r.matchSourcePrefix(ctx, title, body)

	r.mu.Lock()
	if r.activeWinID == "" {
		r.activeWinID = store.NewID("bwin")
		r.activeCount = 1
		r.activeTargets = make(map[string]Target)
		winID := r.activeWinID
		r.windowTimer = time.AfterFunc(r.windowDur, func() {
			r.fireWindow(winID)
		})
	} else {
		r.activeCount++
	}
	if matchedSourceID != "" {
		if r.activeTargets == nil {
			r.activeTargets = make(map[string]Target)
		}
		key := matchedSourceID + "|" + relPath
		r.activeTargets[key] = Target{
			SourceID:     matchedSourceID,
			ReportedPath: reportedPath,
			RelativePath: relPath,
		}
	}
	winID := r.activeWinID
	r.mu.Unlock()

	msg, err := r.store.SaveBarkMessage(ctx, store.BarkMessage{
		Title:           title,
		Body:            body,
		GroupName:       group,
		MatchedSourceID: matchedSourceID,
		Status:          "queued",
		WindowID:        winID,
	})
	return msg, err
}

func (r *Receiver) fireWindow(winID string) {
	r.mu.Lock()
	if r.activeWinID != winID {
		r.mu.Unlock()
		return
	}
	count := r.activeCount
	targets := make([]Target, 0, len(r.activeTargets))
	for _, t := range r.activeTargets {
		targets = append(targets, t)
	}
	r.activeWinID = ""
	r.activeCount = 0
	r.activeTargets = nil
	r.windowTimer = nil
	cb := r.onTrigger
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_ = r.store.MarkBarkWindowDispatched(ctx, winID)
	msg := "Bark 90 秒合并窗口到期，触发云端扫描（合并信号 " + itoa(count) + " 条"
	if len(targets) > 0 {
		msg += "，定向路径 " + itoa(len(targets)) + " 组"
	}
	msg += "）"
	_ = r.store.RecordEvent(ctx, "system", "BARK_WINDOW_FIRED", "bark", msg, true)
	if cb != nil {
		cb(ctx, targets)
	}
}

// matchSourcePrefix finds the longest matching cloud source bark_prefix against "路径：" / "path:" in body or title.
func (r *Receiver) matchSourcePrefix(ctx context.Context, title, body string) (sourceID, reportedPath, relPath string) {
	m := pathLineRe.FindStringSubmatch(body)
	if len(m) < 2 {
		m = pathLineRe.FindStringSubmatch(title)
	}
	if len(m) < 2 {
		return "", "", ""
	}
	reportedPath = NormalizePath(m[1])
	if reportedPath == "" {
		return "", "", ""
	}
	sources, err := r.store.ListSources(ctx, "cloud")
	if err != nil {
		return "", reportedPath, ""
	}
	bestID := ""
	bestPrefix := ""
	for _, src := range sources {
		if !src.Enabled || strings.TrimSpace(src.BarkPrefix) == "" {
			continue
		}
		prefix := NormalizePath(src.BarkPrefix)
		if prefix == "" {
			continue
		}
		if reportedPath == prefix || strings.HasPrefix(reportedPath, prefix+"/") {
			if len(prefix) > len(bestPrefix) {
				bestPrefix = prefix
				bestID = src.ID
			}
		}
	}
	if bestID == "" {
		return "", reportedPath, ""
	}
	rel := strings.Trim(strings.TrimPrefix(reportedPath, bestPrefix), "/")
	if rel != "" && len(strings.Split(rel, "/")) > 8 {
		return "", reportedPath, ""
	}
	return bestID, reportedPath, rel
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for n > 0 {
		i--
		digits[i] = byte('0' + n%10)
		n /= 10
	}
	return string(digits[i:])
}
