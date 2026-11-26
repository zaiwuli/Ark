package bark

import (
	"context"
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

var pathLineRe = regexp.MustCompile(`(?m)路径[：:]\s*([^\r\n]+)`)

type Incoming struct {
	Title string
	Body  string
	Group string
}

type Receiver struct {
	store     *store.Store
	windowDur time.Duration
	onTrigger func(ctx context.Context)

	mu            sync.Mutex
	activeWinID   string
	activeCount   int
	windowTimer   *time.Timer
}

func NewReceiver(st *store.Store, windowDur time.Duration, onTrigger func(ctx context.Context)) *Receiver {
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

func (r *Receiver) Ingest(ctx context.Context, in Incoming) (store.BarkMessage, error) {
	title := truncate(strings.TrimSpace(in.Title), maxFieldLen)
	body := truncate(strings.TrimSpace(in.Body), maxFieldLen)
	group := truncate(strings.TrimSpace(in.Group), 256)

	matchedSourceID := r.matchSourcePrefix(ctx, body)

	r.mu.Lock()
	if r.activeWinID == "" {
		r.activeWinID = store.NewID("bwin")
		r.activeCount = 1
		winID := r.activeWinID
		r.windowTimer = time.AfterFunc(r.windowDur, func() {
			r.fireWindow(winID)
		})
	} else {
		r.activeCount++
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
	r.activeWinID = ""
	r.activeCount = 0
	r.windowTimer = nil
	cb := r.onTrigger
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_ = r.store.RecordEvent(ctx, "system", "BARK_WINDOW_FIRED", "bark",
		"Bark 90 秒合并窗口到期，触发云端变化检查（合并信号 "+itoa(count)+" 条）", false)
	if cb != nil {
		cb(ctx)
	}
}

// matchSourcePrefix finds the longest matching cloud source bark_prefix against "路径：" in the message body.
func (r *Receiver) matchSourcePrefix(ctx context.Context, body string) string {
	m := pathLineRe.FindStringSubmatch(body)
	if len(m) < 2 {
		return ""
	}
	reportedPath := strings.TrimRight(strings.TrimSpace(m[1]), "/")
	sources, err := r.store.ListSources(ctx, "cloud")
	if err != nil {
		return ""
	}
	bestID := ""
	bestLen := 0
	for _, src := range sources {
		if !src.Enabled || strings.TrimSpace(src.BarkPrefix) == "" {
			continue
		}
		prefix := strings.TrimRight(strings.TrimSpace(src.BarkPrefix), "/")
		if reportedPath == prefix || strings.HasPrefix(reportedPath, prefix+"/") {
			if len(prefix) > bestLen {
				bestLen = len(prefix)
				bestID = src.ID
			}
		}
	}
	return bestID
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
