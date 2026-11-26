package localwatch

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/zaiwuli/Ark/internal/extract"
	"github.com/zaiwuli/Ark/internal/store"
)

type LocalSourceConfig struct {
	Path             string `json:"path"`
	OutputDir        string `json:"output_dir"`
	SquashRoot       bool   `json:"squash_root"`
	DisableRecursion bool   `json:"disable_recursion"`
	DeleteOriginal   bool   `json:"delete_original"`
}

type observation struct {
	size        int64
	mtimeUnix   int64
	stableSince time.Time
}

// Watcher scans configured local/NAS directories and enqueues stable archives into pipelines.
type Watcher struct {
	store         *store.Store
	defaultWatch  string
	defaultOutput string
	stableWindow  time.Duration

	mu  sync.Mutex
	obs map[string]observation
}

func NewWatcher(st *store.Store, workDir string, stableWindow time.Duration) *Watcher {
	if stableWindow <= 0 {
		stableWindow = 15 * time.Second
	}
	var watchDir, outDir string
	if workDir != "" {
		watchDir = filepath.Join(workDir, "watch")
		outDir = filepath.Join(workDir, "output")
	}
	return &Watcher{
		store:         st,
		defaultWatch:  watchDir,
		defaultOutput: outDir,
		stableWindow:  stableWindow,
		obs:           make(map[string]observation),
	}
}

// Scan inspects all enabled local sources (plus ARK_WORK_DIR/watch if configured) and adopts stable archives.
func (w *Watcher) Scan(ctx context.Context, now time.Time) (int, error) {
	sources, err := w.store.ListSources(ctx, "local")
	if err != nil {
		return 0, err
	}

	type target struct {
		sourceID string
		cfg      LocalSourceConfig
	}
	targets := make([]target, 0, len(sources)+1)
	seenDir := make(map[string]bool)

	for _, src := range sources {
		if !src.Enabled {
			continue
		}
		var cfg LocalSourceConfig
		if err := json.Unmarshal([]byte(src.ConfigJSON), &cfg); err != nil || cfg.Path == "" {
			continue
		}
		if cfg.OutputDir == "" {
			cfg.OutputDir = w.defaultOutput
		}
		abs, _ := filepath.Abs(cfg.Path)
		seenDir[abs] = true
		targets = append(targets, target{sourceID: src.ID, cfg: cfg})
	}

	if w.defaultWatch != "" {
		abs, _ := filepath.Abs(w.defaultWatch)
		if !seenDir[abs] {
			targets = append(targets, target{
				sourceID: "default-local",
				cfg: LocalSourceConfig{
					Path:       w.defaultWatch,
					OutputDir:  w.defaultOutput,
					SquashRoot: true,
				},
			})
		}
	}

	adopted := 0
	for _, t := range targets {
		n, err := w.scanOne(ctx, t.sourceID, t.cfg, now)
		if err != nil {
			continue
		}
		adopted += n
	}
	return adopted, nil
}

func (w *Watcher) scanOne(ctx context.Context, sourceID string, cfg LocalSourceConfig, now time.Time) (int, error) {
	entries, err := os.ReadDir(cfg.Path)
	if err != nil {
		return 0, err
	}
	adopted := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		fullPath := filepath.Join(cfg.Path, e.Name())
		vol := extract.InspectVolume(fullPath)
		if !vol.IsArchive || !vol.IsPrimary || !vol.Complete {
			continue
		}

		// Verify all volume members are stable before queuing.
		allStable := true
		var totalSize int64
		var latestMtime int64
		for _, member := range vol.Members {
			info, err := os.Stat(member)
			if err != nil || info.Size() == 0 {
				allStable = false
				break
			}
			totalSize += info.Size()
			mt := info.ModTime().UnixNano()
			if mt > latestMtime {
				latestMtime = mt
			}
			if !w.isStable(member, info.Size(), mt, now) {
				allStable = false
			}
		}
		if !allStable {
			continue
		}

		identity := fmt.Sprintf("local:%s:%d:%d", filepath.ToSlash(fullPath), totalSize, latestMtime)
		locatorBytes, _ := json.Marshal(map[string]any{
			"path":    fullPath,
			"members": vol.Members,
		})
		policyBytes, _ := json.Marshal(cfg)

		created, err := w.store.CreateLocalArchivePipeline(ctx, store.LocalArchiveInput{
			Identity:    identity,
			SourceID:    sourceID,
			Name:        e.Name(),
			Size:        totalSize,
			LocatorJSON: string(locatorBytes),
			PolicyJSON:  string(policyBytes),
		})
		if err != nil {
			continue
		}
		if created {
			adopted++
		}
	}
	return adopted, nil
}

func (w *Watcher) isStable(path string, size, mtime int64, now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	prev, ok := w.obs[path]
	if !ok || prev.size != size || prev.mtimeUnix != mtime {
		w.obs[path] = observation{size: size, mtimeUnix: mtime, stableSince: now}
		return false
	}
	return now.Sub(prev.stableSince) >= w.stableWindow
}
