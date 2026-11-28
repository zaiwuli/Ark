package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/zaiwuli/Ark/internal/store"
)

// WorkbenchConfig persists the user's preferred Organize Workbench rules across sessions.
type WorkbenchConfig struct {
	JunkExts          string   `json:"junk_exts"`
	JunkKeywords      string   `json:"junk_keywords"`
	JunkKeywordMaxMB  float64  `json:"junk_keyword_max_mb"`
	JunkMaxMB         float64  `json:"junk_max_mb"`
	JunkSafeExts      string   `json:"junk_safe_exts"`
	JunkSHA1Blacklist []string `json:"junk_sha1_blacklist"`
	DedupKeepPolicy   string   `json:"dedup_keep_policy"` // cleanest | newest | oldest
	RenameCleanAds    bool     `json:"rename_clean_ads"`
	RenameFind        string   `json:"rename_find"`
	RenameReplace     string   `json:"rename_replace"`
	RenameRegex       bool     `json:"rename_regex"`
	RenamePrefix      string   `json:"rename_prefix"`
	ClassifyMode      string   `json:"classify_mode"` // type | month
}

func DefaultWorkbenchConfig() WorkbenchConfig {
	return WorkbenchConfig{
		JunkExts:          ".txt,.url,.html,.htm,.apk,.nfo,.lnk",
		JunkKeywords:      "广告,宣传,推广,公众号,进群,网址,最新地址,备用地址,必看,免费看,thumbs.db,.ds_store",
		JunkKeywordMaxMB:  20,
		JunkMaxMB:         0,
		JunkSafeExts:      ".srt,.ass,.ssa,.vtt,.sub,.zip,.rar,.7z",
		JunkSHA1Blacklist: []string{},
		DedupKeepPolicy:   "cleanest",
		RenameCleanAds:    true,
		ClassifyMode:      "type",
	}
}

func (c WorkbenchConfig) Normalize() WorkbenchConfig {
	def := DefaultWorkbenchConfig()
	if strings.TrimSpace(c.JunkExts) == "" {
		c.JunkExts = def.JunkExts
	}
	if strings.TrimSpace(c.JunkSafeExts) == "" {
		c.JunkSafeExts = def.JunkSafeExts
	}
	if c.JunkKeywordMaxMB < 0 {
		c.JunkKeywordMaxMB = 0
	}
	if c.JunkMaxMB < 0 {
		c.JunkMaxMB = 0
	}
	if c.DedupKeepPolicy != "cleanest" && c.DedupKeepPolicy != "newest" && c.DedupKeepPolicy != "oldest" {
		c.DedupKeepPolicy = "cleanest"
	}
	if c.ClassifyMode != "type" && c.ClassifyMode != "month" {
		c.ClassifyMode = "type"
	}
	seen := make(map[string]bool)
	cleaned := make([]string, 0, len(c.JunkSHA1Blacklist))
	for _, h := range c.JunkSHA1Blacklist {
		h = strings.ToLower(strings.TrimSpace(h))
		if len(h) >= 8 && !seen[h] {
			seen[h] = true
			cleaned = append(cleaned, h)
		}
	}
	c.JunkSHA1Blacklist = cleaned
	return c
}

func (s *Service) WorkbenchConfig(ctx context.Context) WorkbenchConfig {
	cfg := DefaultWorkbenchConfig()
	_, _ = s.store.GetSetting(ctx, "organizer.workbench", &cfg)
	return cfg.Normalize()
}

func (s *Service) SaveWorkbenchConfig(ctx context.Context, incoming WorkbenchConfig) (WorkbenchConfig, error) {
	norm := incoming.Normalize()
	if err := s.store.SetSetting(ctx, "organizer.workbench", norm, false); err != nil {
		return WorkbenchConfig{}, err
	}
	return norm, nil
}

func (s *Service) AddJunkSHA1(ctx context.Context, sha1Hex string) (WorkbenchConfig, error) {
	h := strings.ToLower(strings.TrimSpace(sha1Hex))
	if len(h) < 8 {
		return WorkbenchConfig{}, store.ErrInvalid
	}
	cfg := s.WorkbenchConfig(ctx)
	cfg.JunkSHA1Blacklist = append(cfg.JunkSHA1Blacklist, h)
	return s.SaveWorkbenchConfig(ctx, cfg)
}

type RenameUndoEntry struct {
	Scope     string
	SourceCID string
	FID       string
	LocalDir  string
	OldName   string
	NewName   string
}

var (
	adPrefixPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^\s*(?:https?://)?(?:www\.)?[a-z0-9.-]+\.[a-z]{2,6}\s*(?:\[url=[^\]]*\]@\[/url\]|@|\s*-\s*)`),
		regexp.MustCompile(`(?i)\[url=[^\]]*\][^[]*\[/url\]`),
		regexp.MustCompile(`【(?:自转|115ED2k|夸克/115eD2k|115|资源|原档|首发|独家|福利|广告|推广|免翻)[^】]{0,20}】`),
		regexp.MustCompile(`\[(?:自转|115ED2k|115|AD|广告)[^\]]{0,20}\]`),
	}
	illegalNameChars = regexp.MustCompile(`[\\/:*?"<>|]+`)
	multiSpaceRe     = regexp.MustCompile(`\s{2,}`)
)

// cleanFilename computes a cleaned filename according to WorkbenchConfig, preserving file extension.
func cleanFilename(name string, cfg WorkbenchConfig) (newName string, reason string) {
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	if base == "" {
		return name, ""
	}
	origBase := base
	var reasons []string

	if cfg.RenameCleanAds {
		for _, re := range adPrefixPatterns {
			next := re.ReplaceAllString(base, "")
			if next != base {
				base = next
			}
		}
		base = strings.TrimSpace(multiSpaceRe.ReplaceAllString(base, " "))
		base = strings.TrimLeft(base, "@_-·. ")
		if base != origBase {
			reasons = append(reasons, "去广告水印")
		}
	}

	if cfg.RenameFind != "" {
		before := base
		if cfg.RenameRegex {
			if re, err := regexp.Compile(cfg.RenameFind); err == nil {
				base = re.ReplaceAllString(base, cfg.RenameReplace)
			}
		} else {
			base = strings.ReplaceAll(base, cfg.RenameFind, cfg.RenameReplace)
		}
		if base != before {
			reasons = append(reasons, "规则替换")
		}
	}

	if pref := strings.TrimSpace(cfg.RenamePrefix); pref != "" && !strings.HasPrefix(base, pref) {
		base = pref + base
		reasons = append(reasons, "加前缀")
	}

	base = strings.TrimSpace(illegalNameChars.ReplaceAllString(base, " "))
	base = strings.TrimSpace(multiSpaceRe.ReplaceAllString(base, " "))
	if base == "" {
		return name, ""
	}
	candidate := base + ext
	if candidate == name {
		return name, ""
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "规范化")
	}
	return candidate, strings.Join(reasons, "+")
}

func parseKeywords(raw string) []string {
	var out []string
	for _, part := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '，' || r == '\n'
	}) {
		k := strings.ToLower(strings.TrimSpace(part))
		if k != "" {
			out = append(out, k)
		}
	}
	return out
}

func matchKeyword(name string, keywords []string) string {
	lower := strings.ToLower(name)
	for _, kw := range keywords {
		if strings.Contains(lower, kw) {
			return kw
		}
	}
	return ""
}

// UndoLastRename reverts the most recent batch rename executed in the Organize Center.
func (s *Service) UndoLastRename(ctx context.Context) (int, error) {
	if s.center == nil {
		return 0, errors.New("NO_RENAME_HISTORY")
	}
	s.center.mu.Lock()
	if s.center.state.Status == "running" {
		s.center.mu.Unlock()
		return 0, errors.New("ORGANIZE_JOB_BUSY")
	}
	entries := append([]RenameUndoEntry(nil), s.center.lastRenames...)
	s.center.mu.Unlock()

	if len(entries) == 0 {
		return 0, errors.New("NO_RENAME_HISTORY")
	}

	restored := 0
	cloud, _ := s.snapshotCloud()
	cloudGroups := make(map[string]map[string]string)

	for _, e := range entries {
		if e.Scope == "local" {
			curPath := filepath.Join(e.LocalDir, e.NewName)
			origPath := filepath.Join(e.LocalDir, e.OldName)
			if err := os.Rename(curPath, origPath); err == nil {
				restored++
			}
		} else if e.Scope == "cloud" {
			if cloudGroups[e.SourceCID] == nil {
				cloudGroups[e.SourceCID] = make(map[string]string)
			}
			cloudGroups[e.SourceCID][e.FID] = e.OldName
		}
	}

	if len(cloudGroups) > 0 {
		if cloud == nil {
			return restored, ErrP115NotConfigured
		}
		for srcCID, rmap := range cloudGroups {
			if err := cloud.BatchRename(ctx, srcCID, rmap); err != nil {
				return restored, err
			}
			restored += len(rmap)
		}
	}

	s.center.mu.Lock()
	s.center.lastRenames = nil
	s.center.state.UndoRenameCount = 0
	s.center.addLogLocked(fmt.Sprintf("已撤销上次批量改名：恢复 %d 个原文件名", restored))
	s.center.mu.Unlock()
	return restored, nil
}
