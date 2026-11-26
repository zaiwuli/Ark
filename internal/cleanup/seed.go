package cleanup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/zaiwuli/Ark/internal/p115"
	"github.com/zaiwuli/Ark/internal/store"
)

type SeedPolicy struct {
	Enabled     bool     `json:"enabled"`
	Mode        string   `json:"mode"`       // all | include | exclude
	Extensions  []string `json:"extensions"` // normalized lowercase with leading dot
	Keywords    []string `json:"keywords"`
	Scope       string   `json:"scope"` // path | name
	Minimum     *int64   `json:"minimum,omitempty"`
	Maximum     *int64   `json:"maximum,omitempty"`
	EmptyDirs   bool     `json:"empty_dirs"`
	DryRun      bool     `json:"dry_run"`
	AutoExtract bool     `json:"auto_extract"`
}

type SeedReport struct {
	Result             string   `json:"result"` // PREVIEW | SUCCEEDED | FAILED
	RootCID            string   `json:"root_cid,omitempty"`
	Matched            int      `json:"matched"`
	Preview            []string `json:"preview,omitempty"`
	DeletedFiles       []string `json:"deleted_files,omitempty"`
	DeletedDirectories []string `json:"deleted_directories,omitempty"`
	Error              string   `json:"error,omitempty"`
}

func DefaultSeedPolicy() SeedPolicy {
	return SeedPolicy{
		Enabled:   false,
		Mode:      "exclude",
		Scope:     "path",
		EmptyDirs: true,
		DryRun:    false,
	}
}

func ValidatePolicy(p SeedPolicy) (SeedPolicy, error) {
	if p.Mode == "" {
		p.Mode = "exclude"
	}
	if p.Scope == "" {
		p.Scope = "path"
	}
	if p.Mode != "all" && p.Mode != "include" && p.Mode != "exclude" {
		return SeedPolicy{}, errors.New("INVALID_SEED_CLEANUP")
	}
	if p.Scope != "path" && p.Scope != "name" {
		return SeedPolicy{}, errors.New("INVALID_SEED_CLEANUP")
	}
	if len(p.Extensions) > 100 || len(p.Keywords) > 100 {
		return SeedPolicy{}, errors.New("INVALID_SEED_CLEANUP")
	}

	p.Extensions = normalizeList(p.Extensions, true)
	p.Keywords = normalizeList(p.Keywords, false)
	if p.Mode == "include" && len(p.Extensions) == 0 {
		return SeedPolicy{}, errors.New("SEED_KEEP_TYPES_REQUIRED")
	}
	if p.Minimum != nil && *p.Minimum < 0 {
		return SeedPolicy{}, errors.New("INVALID_SEED_CLEANUP_SIZE")
	}
	if p.Maximum != nil && *p.Maximum < 0 {
		return SeedPolicy{}, errors.New("INVALID_SEED_CLEANUP_SIZE")
	}
	if p.Minimum != nil && p.Maximum != nil && *p.Minimum > *p.Maximum {
		return SeedPolicy{}, errors.New("INVALID_SEED_CLEANUP_SIZE")
	}
	return p, nil
}

func normalizeList(in []string, addDot bool) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.ToLower(strings.TrimSpace(raw))
		if v == "" || len(v) > 200 {
			continue
		}
		if addDot && !strings.HasPrefix(v, ".") {
			v = "." + v
		}
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	return out
}

func ShouldRemove(relPath string, size int64, p SeedPolicy) bool {
	if !p.Enabled {
		return false
	}
	ext := strings.ToLower(path.Ext(relPath))
	typed := false
	for _, e := range p.Extensions {
		if ext == e {
			typed = true
			break
		}
	}
	if p.Mode == "include" && !typed {
		return true
	}
	if p.Mode == "exclude" && typed {
		return true
	}
	target := strings.ToLower(relPath)
	if p.Scope == "name" {
		target = strings.ToLower(path.Base(relPath))
	}
	for _, kw := range p.Keywords {
		if strings.Contains(target, kw) {
			return true
		}
	}
	if p.Minimum != nil && size < *p.Minimum {
		return true
	}
	if p.Maximum != nil && size > *p.Maximum {
		return true
	}
	return false
}

type Engine struct {
	store *store.Store
	cloud p115.Port
}

func NewEngine(st *store.Store, cloud p115.Port) *Engine {
	return &Engine{store: st, cloud: cloud}
}

func listAll(ctx context.Context, cloud p115.Port, cid string) ([]p115.CloudFile, error) {
	var all []p115.CloudFile
	offset := 0
	for range 100 {
		page, err := cloud.Files(ctx, cid, offset)
		if err != nil {
			return nil, err
		}
		all = append(all, page.Files...)
		if page.NextOffset == nil {
			break
		}
		offset = *page.NextOffset
	}
	return all, nil
}

// Run executes seed post-download cleanup strictly inside the unique seed resource subfolder.
func (e *Engine) Run(ctx context.Context, pipe store.Pipeline, rawPolicy SeedPolicy, rootCID string) (SeedReport, error) {
	policy, err := ValidatePolicy(rawPolicy)
	if err != nil {
		return SeedReport{Result: "FAILED", Error: err.Error()}, err
	}
	_ = e.store.RecordEvent(ctx, pipe.ID, "SEED_CLEANUP_STARTED", "seed_cleanup", "开始种子下载后清理", false)

	topEntries, err := listAll(ctx, e.cloud, pipe.TargetCID)
	if err != nil {
		return SeedReport{Result: "FAILED", Error: err.Error()}, err
	}
	var roots []p115.CloudFile
	for _, f := range topEntries {
		if f.IsDir && f.Name == pipe.Name {
			roots = append(roots, f)
		}
	}
	if len(roots) != 1 {
		err := errors.New("SEED_RESOURCE_DIRECTORY_NOT_UNIQUE")
		return SeedReport{Result: "FAILED", Error: err.Error()}, err
	}
	root := roots[0]
	if root.FID == "" || root.FID == "0" || root.FID == pipe.TargetCID || root.FID == rootCID {
		err := errors.New("UNSAFE_SEED_ROOT")
		return SeedReport{Result: "FAILED", Error: err.Error()}, err
	}

	type node struct {
		file p115.CloudFile
		rel  string
	}
	type dirTask struct {
		cid    string
		prefix string
	}
	var files, dirs []node
	queue := []dirTask{{cid: root.FID, prefix: ""}}
	seen := make(map[string]bool)

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if seen[cur.cid] || len(seen) >= 1000 {
			err := errors.New("SEED_DIRECTORY_BUDGET")
			return SeedReport{Result: "FAILED", Error: err.Error()}, err
		}
		seen[cur.cid] = true
		children, err := listAll(ctx, e.cloud, cur.cid)
		if err != nil {
			return SeedReport{Result: "FAILED", Error: err.Error()}, err
		}
		for _, ch := range children {
			if ch.Name == "" || ch.Name == "." || ch.Name == ".." || strings.ContainsAny(ch.Name, `/\`) {
				err := errors.New("UNSAFE_SEED_PATH")
				return SeedReport{Result: "FAILED", Error: err.Error()}, err
			}
			rel := ch.Name
			if cur.prefix != "" {
				rel = path.Join(cur.prefix, ch.Name)
			}
			if ch.IsDir {
				if seen[ch.FID] || ch.FID == "0" || ch.FID == pipe.TargetCID || ch.FID == rootCID {
					err := errors.New("UNSAFE_SEED_DIRECTORY")
					return SeedReport{Result: "FAILED", Error: err.Error()}, err
				}
				dirs = append(dirs, node{file: ch, rel: rel})
				queue = append(queue, dirTask{cid: ch.FID, prefix: rel})
			} else {
				files = append(files, node{file: ch, rel: rel})
			}
			if len(files)+len(dirs) > 10000 {
				err := errors.New("SEED_FILE_BUDGET")
				return SeedReport{Result: "FAILED", Error: err.Error()}, err
			}
		}
	}

	report := SeedReport{RootCID: root.FID}
	var selected []node
	for _, n := range files {
		if ShouldRemove(n.rel, n.file.Size, policy) {
			selected = append(selected, n)
			report.Preview = append(report.Preview, n.rel)
		}
	}
	report.Matched = len(selected)

	if policy.Enabled && !policy.DryRun {
		for _, n := range selected {
			if err := e.safeDelete(ctx, pipe.ID, n.file, n.rel, false); err != nil {
				report.Result = "FAILED"
				report.Error = err.Error()
				return report, err
			}
			report.DeletedFiles = append(report.DeletedFiles, n.rel)
		}
		if policy.EmptyDirs {
			sort.Slice(dirs, func(i, j int) bool {
				return strings.Count(dirs[i].rel, "/") > strings.Count(dirs[j].rel, "/")
			})
			for _, d := range dirs {
				rem, err := listAll(ctx, e.cloud, d.file.FID)
				if err != nil {
					report.Result = "FAILED"
					report.Error = err.Error()
					return report, err
				}
				if len(rem) == 0 {
					if err := e.safeDelete(ctx, pipe.ID, d.file, d.rel, true); err != nil {
						report.Result = "FAILED"
						report.Error = err.Error()
						return report, err
					}
					report.DeletedDirectories = append(report.DeletedDirectories, d.rel)
				}
			}
		}
	}

	if policy.Enabled && policy.DryRun {
		report.Result = "PREVIEW"
	} else {
		report.Result = "SUCCEEDED"
	}
	msg := fmt.Sprintf("种子清理匹配 %d 项（%s）", report.Matched, map[bool]string{true: "仅预演", false: "已执行"}[policy.DryRun])
	_ = e.store.RecordEvent(ctx, pipe.ID, "SEED_CLEANUP_COMPLETED", "complete", msg, true)
	return report, nil
}

func (e *Engine) safeDelete(ctx context.Context, pipeID string, f p115.CloudFile, rel string, isDir bool) error {
	opKey := "seed-delete:" + pipeID + ":" + f.FID
	existing, err := e.store.JournalGet(ctx, opKey)
	if err != nil {
		return err
	}
	if existing != nil {
		if existing.Status == "completed" {
			return nil
		}
		return errors.New("SEED_DELETE_REQUIRES_RECONCILIATION")
	}
	if err := e.store.JournalPrepare(ctx, opKey, pipeID, "seed_cleanup", map[string]any{
		"fid": f.FID, "cid": f.CID, "path": rel, "dir": isDir,
	}); err != nil {
		return err
	}
	if err := e.cloud.DeleteVerified(ctx, p115.FileLocator{
		FID: f.FID, CID: f.CID, Size: f.Size, SHA1: f.SHA1,
	}); err != nil {
		_ = e.store.JournalSetStatus(ctx, opKey, "uncertain")
		return err
	}
	_ = e.store.JournalSetStatus(ctx, opKey, "completed")
	_ = e.store.RecordEvent(ctx, pipeID, "SEED_FILE_DELETED", "seed_cleanup", "已清理："+rel, false)
	return nil
}

func EncodeReport(r SeedReport) string {
	b, _ := json.Marshal(map[string]any{"seed_cleanup": r})
	return string(b)
}
