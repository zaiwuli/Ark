package pipeline

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/zaiwuli/Ark/internal/p115"
)

const organizeSnapshotTTL = 30 * time.Minute

type SnapshotChildFile struct {
	FID  string
	Path string
	Name string
	Size int64
}

type SnapshotChildSubdir struct {
	FID  string
	Path string
	Name string
}

type SnapshotFile struct {
	FID       string
	ParentCID string
	Path      string
	ParentDir string
	Name      string
	Size      int64
	SHA1      string
	ModTime   int64
	Depth     int
}

type SnapshotDir struct {
	FID        string
	ParentCID  string
	Path       string
	ParentDir  string
	Name       string
	Depth      int // 0 = direct child of target root
	IsComplete bool
	Files      []SnapshotChildFile
	Subdirs    []SnapshotChildSubdir
}

func (d *SnapshotDir) ChildCount() int {
	return len(d.Files) + len(d.Subdirs)
}

func (d *SnapshotDir) OnlyChild() (fid, path, name string, ok bool) {
	if len(d.Files) == 1 && len(d.Subdirs) == 0 {
		f := d.Files[0]
		return f.FID, f.Path, f.Name, true
	}
	if len(d.Subdirs) == 1 && len(d.Files) == 0 {
		sd := d.Subdirs[0]
		return sd.FID, sd.Path, sd.Name, true
	}
	return "", "", "", false
}

type TargetSnapshot struct {
	Target resolvedCenterTarget
	Files  []SnapshotFile
	Dirs   []SnapshotDir
}

type OrganizeSnapshot struct {
	CreatedAt   int64
	ExpiresAt   int64
	TargetKeys  []string
	TargetNames []string
	Targets     []TargetSnapshot
}

type SnapshotInfo struct {
	Ready       bool     `json:"ready"`
	CreatedAt   int64    `json:"created_at,omitempty"`
	ExpiresAt   int64    `json:"expires_at,omitempty"`
	TargetKeys  []string `json:"target_keys,omitempty"`
	TargetNames []string `json:"target_names,omitempty"`
	FileCount   int      `json:"file_count"`
	DirCount    int      `json:"dir_count"`
}

func (snap *OrganizeSnapshot) Info(now time.Time) *SnapshotInfo {
	if snap == nil || now.Unix() > snap.ExpiresAt {
		return nil
	}
	fc, dc := 0, 0
	for i := range snap.Targets {
		fc += len(snap.Targets[i].Files)
		dc += len(snap.Targets[i].Dirs)
	}
	return &SnapshotInfo{
		Ready:       true,
		CreatedAt:   snap.CreatedAt,
		ExpiresAt:   snap.ExpiresAt,
		TargetKeys:  append([]string(nil), snap.TargetKeys...),
		TargetNames: append([]string(nil), snap.TargetNames...),
		FileCount:   fc,
		DirCount:    dc,
	}
}

func canonicalTargetKeys(keys []string) []string {
	seen := make(map[string]bool, len(keys))
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k != "" && !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func sameStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *Service) captureCloudTargetSnapshot(
	ctx context.Context,
	cloud p115.Port,
	p115Cfg P115Settings,
	t resolvedCenterTarget,
	probeLeafDirs bool,
) TargetSnapshot {
	ts := TargetSnapshot{Target: t}

	seenFileFID := make(map[string]bool)
	addFile := func(f p115.CloudFile, curCID string, depth int) {
		if f.IsDir || f.FID == "" || seenFileFID[f.FID] {
			return
		}
		seenFileFID[f.FID] = true
		ts.Files = append(ts.Files, SnapshotFile{
			FID:       f.FID,
			ParentCID: curCID,
			Name:      f.Name,
			Size:      f.Size,
			SHA1:      f.SHA1,
			ModTime:   f.ModTime,
			Depth:     depth,
		})
	}

	dirIdxByFID := make(map[string]int)
	upsertDir := func(sd SnapshotDir) {
		if sd.FID == "" {
			return
		}
		if idx, ok := dirIdxByFID[sd.FID]; ok {
			if sd.IsComplete {
				ts.Dirs[idx] = sd
			}
			return
		}
		dirIdxByFID[sd.FID] = len(ts.Dirs)
		ts.Dirs = append(ts.Dirs, sd)
	}

	// Phase 0: Fast flat recursive file fetch (show_dir=0, cur=0, limit=1150)
	recFilesByParent := make(map[string][]SnapshotChildFile)
	recMaxSizeByParent := make(map[string]int64)
	recComplete := false
	if t.Depth >= 2 && t.CID != "" && t.CID != "0" && t.CID != p115Cfg.CloudRootCID {
		const maxRecPages = 15
		offset := 0
		recOK := true
		for pageIdx := 0; pageIdx < maxRecPages; pageIdx++ {
			if ctx.Err() != nil {
				return ts
			}
			page, err := cloud.FilesRecursive(ctx, t.CID, offset)
			if err != nil {
				if isCloudScanFatalErr(err) {
					s.center.addLog("检测到 115 熔断/限流/认证异常，已停止拉取快照。")
					return ts
				}
				s.center.addLog(fmt.Sprintf("极速递归拉取回退到逐层扫描（%s）", err.Error()))
				recOK = false
				break
			}
			for _, f := range page.Files {
				d := 1
				if f.CID == t.CID {
					d = 0
				}
				addFile(f, f.CID, d)
				recFilesByParent[f.CID] = append(recFilesByParent[f.CID], SnapshotChildFile{
					FID:  f.FID,
					Name: f.Name,
					Size: f.Size,
				})
				if f.Size > recMaxSizeByParent[f.CID] {
					recMaxSizeByParent[f.CID] = f.Size
				}
			}
			s.center.setProgress(pageIdx+1, maxRecPages, fmt.Sprintf("拉取快照 [115] %s（已收集 %d 个文件）…", t.Name, len(ts.Files)))
			if page.NextOffset == nil {
				recComplete = true
				break
			}
			offset = *page.NextOffset
		}
		if recOK {
			s.center.addLog(fmt.Sprintf("极速拉取 [115] %s 全层级文件完成：%d 个文件", t.Name, len(ts.Files)))
		}
	}

	// Phase 1: BFS directory tree capture (with smart pruning of normal multi-file video folders!)
	type dirNode struct {
		cid       string
		parentCID string
		name      string
		depth     int
	}
	maxSubDepth := t.Depth - 1
	queue := []dirNode{{cid: t.CID, depth: 0}}
	visited := make(map[string]bool)
	const maxCloudDirCalls = 200
	const pruneMinBigFileBytes = 20 * 1024 * 1024
	apiCalls := 0
	prunedDirs := 0
	budgetWarned := false

	reportProgress := func() {
		if apiCalls > 0 && apiCalls%5 == 0 {
			s.center.setProgress(apiCalls, maxCloudDirCalls, fmt.Sprintf("拉取目录树 [115] %s（已扫 %d 个目录）…", t.Name, apiCalls))
			s.center.addLog(fmt.Sprintf("目录树进度 [115] %s：已扫 %d 个目录，累计 %d 文件 / %d 目录", t.Name, apiCalls, len(ts.Files), len(ts.Dirs)))
		}
	}

	for len(queue) > 0 {
		if ctx.Err() != nil {
			return ts
		}
		if apiCalls >= maxCloudDirCalls {
			if !budgetWarned {
				budgetWarned = true
				s.center.addLog(fmt.Sprintf("[115] %s 目录树探测已达安全上限（%d 次请求），已保留已拉取快照", t.Name, maxCloudDirCalls))
			}
			break
		}
		cur := queue[0]
		queue = queue[1:]
		if cur.cid == "" || visited[cur.cid] {
			continue
		}
		visited[cur.cid] = true
		apiCalls++

		files, err := listAllCloudFiles(ctx, cloud, cur.cid)
		if err != nil {
			s.center.addLog(fmt.Sprintf("读取 115 目录 %s 失败：%s", cur.cid, err.Error()))
			if isCloudScanFatalErr(err) {
				s.center.addLog("检测到 115 熔断/限流/认证异常，已停止拉取当前目录树。")
				break
			}
			continue
		}
		reportProgress()

		if cur.depth > 0 {
			sd := SnapshotDir{
				FID:        cur.cid,
				ParentCID:  cur.parentCID,
				Name:       cur.name,
				Depth:      cur.depth - 1,
				IsComplete: true,
			}
			for _, cf := range files {
				if cf.IsDir {
					sd.Subdirs = append(sd.Subdirs, SnapshotChildSubdir{FID: cf.FID, Name: cf.Name})
				} else {
					sd.Files = append(sd.Files, SnapshotChildFile{FID: cf.FID, Name: cf.Name, Size: cf.Size})
				}
			}
			upsertDir(sd)
		}

		for _, f := range files {
			if ctx.Err() != nil {
				return ts
			}
			if f.IsDir {
				if isManagedCloudSkipDir(f, p115Cfg) {
					continue
				}
				// Register folder metadata (at least incomplete) so date_archive always sees top-level date folders
				upsertDir(SnapshotDir{
					FID:        f.FID,
					ParentCID:  cur.cid,
					Name:       f.Name,
					Depth:      cur.depth,
					IsComplete: false,
				})
				if cur.depth < maxSubDepth && !visited[f.FID] {
					queue = append(queue, dirNode{
						cid:       f.FID,
						parentCID: cur.cid,
						name:      f.Name,
						depth:     cur.depth + 1,
					})
					continue
				}
				if probeLeafDirs {
					// Smart lossless pruning: if Phase 0 already proved this leaf folder has >=2 direct files AND a >=20MB file,
					// it can NEVER be empty_dir, NEVER be single-child flatten, and NEVER be a <20MB junk folder!
					if recComplete {
						if pf := recFilesByParent[f.FID]; len(pf) >= 2 && recMaxSizeByParent[f.FID] >= pruneMinBigFileBytes {
							prunedDirs++
							upsertDir(SnapshotDir{
								FID:        f.FID,
								ParentCID:  cur.cid,
								Name:       f.Name,
								Depth:      cur.depth,
								IsComplete: false,
								Files:      pf,
							})
							continue
						}
					}
					if apiCalls >= maxCloudDirCalls {
						if !budgetWarned {
							budgetWarned = true
							s.center.addLog(fmt.Sprintf("[115] %s 目录树探测已达安全上限（%d 次请求），文件快照已完整", t.Name, maxCloudDirCalls))
						}
						continue
					}
					apiCalls++
					sub, subErr := cloud.Files(ctx, f.FID, 0)
					reportProgress()
					if subErr != nil {
						if isCloudScanFatalErr(subErr) {
							s.center.addLog("检测到 115 熔断/限流/认证异常，已停止拉取当前目录树。")
							break
						}
						continue
					}
					sd := SnapshotDir{
						FID:        f.FID,
						ParentCID:  cur.cid,
						Name:       f.Name,
						Depth:      cur.depth,
						IsComplete: sub.NextOffset == nil,
					}
					for _, sf := range sub.Files {
						if sf.IsDir {
							sd.Subdirs = append(sd.Subdirs, SnapshotChildSubdir{FID: sf.FID, Name: sf.Name})
						} else {
							sd.Files = append(sd.Files, SnapshotChildFile{FID: sf.FID, Name: sf.Name, Size: sf.Size})
							addFile(sf, f.FID, cur.depth+1)
						}
					}
					upsertDir(sd)
				}
				continue
			}
			addFile(f, cur.cid, cur.depth)
		}
	}
	if prunedDirs > 0 {
		s.center.addLog(fmt.Sprintf("⚡ 智能剪枝跳过 %d 个含大文件的正常目录（省去 %d 次 115 请求）", prunedDirs, prunedDirs))
	}
	return ts
}

func (s *Service) captureLocalTargetSnapshot(ctx context.Context, t resolvedCenterTarget) TargetSnapshot {
	ts := TargetSnapshot{Target: t}
	type dirNode struct {
		path      string
		parentDir string
		name      string
		depth     int
	}
	maxSubDepth := t.Depth - 1
	queue := []dirNode{{path: t.Path, depth: 0}}
	visited := make(map[string]bool)
	const maxDirs = 200

	dirIdxByPath := make(map[string]int)
	upsertDir := func(sd SnapshotDir) {
		if sd.Path == "" {
			return
		}
		if idx, ok := dirIdxByPath[sd.Path]; ok {
			if sd.IsComplete {
				ts.Dirs[idx] = sd
			}
			return
		}
		dirIdxByPath[sd.Path] = len(ts.Dirs)
		ts.Dirs = append(ts.Dirs, sd)
	}

	for len(queue) > 0 && len(visited) < maxDirs {
		if ctx.Err() != nil {
			return ts
		}
		cur := queue[0]
		queue = queue[1:]
		if cur.path == "" || visited[cur.path] {
			continue
		}
		visited[cur.path] = true

		entries, err := os.ReadDir(cur.path)
		if err != nil {
			s.center.addLog(fmt.Sprintf("读取本地目录 %s 失败：%s", cur.path, err.Error()))
			continue
		}

		if cur.depth > 0 {
			sd := SnapshotDir{
				Path:       cur.path,
				ParentDir:  cur.parentDir,
				Name:       cur.name,
				Depth:      cur.depth - 1,
				IsComplete: true,
			}
			for _, e := range entries {
				full := filepath.Join(cur.path, e.Name())
				if e.IsDir() {
					sd.Subdirs = append(sd.Subdirs, SnapshotChildSubdir{Path: full, Name: e.Name()})
				} else if info, err := e.Info(); err == nil {
					sd.Files = append(sd.Files, SnapshotChildFile{Path: full, Name: e.Name(), Size: info.Size()})
				}
			}
			upsertDir(sd)
		}

		for _, e := range entries {
			if ctx.Err() != nil {
				return ts
			}
			full := filepath.Join(cur.path, e.Name())
			if e.IsDir() {
				if cur.depth < maxSubDepth && !visited[full] {
					queue = append(queue, dirNode{path: full, parentDir: cur.path, name: e.Name(), depth: cur.depth + 1})
					continue
				}
				sd := SnapshotDir{
					Path:      full,
					ParentDir: cur.path,
					Name:      e.Name(),
					Depth:     cur.depth,
				}
				if children, rErr := os.ReadDir(full); rErr == nil {
					sd.IsComplete = true
					for _, ch := range children {
						cFull := filepath.Join(full, ch.Name())
						if ch.IsDir() {
							sd.Subdirs = append(sd.Subdirs, SnapshotChildSubdir{Path: cFull, Name: ch.Name()})
						} else if info, err := ch.Info(); err == nil {
							sd.Files = append(sd.Files, SnapshotChildFile{Path: cFull, Name: ch.Name(), Size: info.Size()})
						}
					}
				}
				upsertDir(sd)
				continue
			}
			info, infoErr := e.Info()
			if infoErr != nil {
				continue
			}
			ts.Files = append(ts.Files, SnapshotFile{
				Path:      full,
				ParentDir: cur.path,
				Name:      e.Name(),
				Size:      info.Size(),
				ModTime:   info.ModTime().Unix(),
				Depth:     cur.depth,
			})
		}
	}
	return ts
}

func evalSnapshotJunkFolder(dir SnapshotDir, keywords []string, safeExts map[string]bool, kwMaxBytes int64, kwMaxMB float64) (bool, string) {
	if !dir.IsComplete || strictDateDirRe.MatchString(dir.Name) || len(dir.Subdirs) > 0 || len(dir.Files) == 0 {
		return false, ""
	}
	kw := matchKeyword(dir.Name, keywords)
	if kw == "" {
		return false, ""
	}
	for _, ch := range dir.Files {
		ext := strings.ToLower(filepath.Ext(ch.Name))
		if ext != "" && safeExts[ext] {
			return false, ""
		}
		if kwMaxBytes > 0 && ch.Size >= kwMaxBytes {
			return false, ""
		}
	}
	return true, fmt.Sprintf("广告文件夹（命中「%s」且无≥%.0fMB大文件）", kw, kwMaxMB)
}

func (s *Service) evaluateSnapshot(
	snap *OrganizeSnapshot,
	req CenterScanRequest,
	wbCfg WorkbenchConfig,
	p115Cfg P115Settings,
	todayStr string,
	activeDates, activeCIDs map[string]bool,
) *CenterScanPlan {
	junkExts := parseExtSet(wbCfg.JunkExts)
	safeExts := parseExtSet(wbCfg.JunkSafeExts)
	sha1BL := make(map[string]bool, len(wbCfg.JunkSHA1Blacklist))
	for _, h := range wbCfg.JunkSHA1Blacklist {
		sha1BL[strings.ToLower(h)] = true
	}
	keywords := parseKeywords(wbCfg.JunkKeywords)
	kwMaxBytes := int64(wbCfg.JunkKeywordMaxMB * 1024 * 1024)
	junkMaxBytes := int64(wbCfg.JunkMaxMB * 1024 * 1024)

	seq := 0
	makeID := func() string {
		seq++
		return fmt.Sprintf("oi_%d", seq)
	}

	counts := map[string]int{
		"date_archive": 0,
		"duplicate":    0,
		"junk":         0,
		"classify":     0,
		"empty_dir":    0,
		"rename":       0,
		"flatten":      0,
	}
	var items []OrganizeItem
	emit := func(it OrganizeItem) {
		items = append(items, it)
		counts[it.Tool]++
	}

	managedSkipCIDs := make(map[string]bool)
	for _, managed := range []string{p115Cfg.CloudArchiveZoneCID, p115Cfg.CloudArchiveCID, p115Cfg.CloudFailedCID, p115Cfg.CloudHistoryCID} {
		if managed != "" {
			managedSkipCIDs[managed] = true
		}
	}

	for ti := range snap.Targets {
		ts := &snap.Targets[ti]
		t := ts.Target
		isCloud := t.Kind == "cloud"
		catName := sanitizeArchiveCatName(t.Name)

		skipParents := make(map[string]bool)
		if isCloud {
			for cid := range activeCIDs {
				skipParents[cid] = true
			}
			for cid := range managedSkipCIDs {
				skipParents[cid] = true
			}
		}
		deletedParents := make(map[string]bool)
		flattenedParents := make(map[string]bool)

		// 1. Directory-level rules
		for _, d := range ts.Dirs {
			key := d.Path
			if isCloud {
				key = d.FID
			}
			if isCloud && activeCIDs[d.FID] {
				continue
			}

			// Top-level old date folder (yesterday & earlier)
			if d.Depth == 0 && req.EnableDateArchive && strictDateDirRe.MatchString(d.Name) && d.Name < todayStr {
				if activeDates[d.Name] || (isCloud && activeCIDs[d.FID]) {
					skipParents[key] = true
					continue
				}
				if req.EnableEmptyDir && d.IsComplete && d.ChildCount() == 0 {
					deletedParents[key] = true
					destLbl := "删除空目录"
					if isCloud {
						destLbl = "移入 115 回收站"
					}
					emit(OrganizeItem{
						ID:         makeID(),
						Tool:       "empty_dir",
						Scope:      t.Kind,
						TargetName: t.Name,
						Name:       d.Name,
						Detail:     "空日期目录（昨日及以前）",
						Action:     "delete",
						DestLabel:  destLbl,
						SourceCID:  d.ParentCID,
						FID:        d.FID,
						SourcePath: d.Path,
					})
					continue
				}
				_, _, dispTarget := historyTargetRel(catName, d.Name)
				emit(OrganizeItem{
					ID:         makeID(),
					Tool:       "date_archive",
					Scope:      t.Kind,
					TargetName: t.Name,
					Name:       d.Name,
					Detail:     "旧日期目录（昨日及以前）",
					Action:     "move",
					DestLabel:  dispTarget,
					SourceCID:  d.ParentCID,
					RootCID:    t.CID,
					FID:        d.FID,
					SourcePath: d.Path,
					RootPath:   t.Path,
					DateStr:    d.Name,
				})
				continue
			}

			if !d.IsComplete {
				continue
			}
			if req.EnableEmptyDir && d.ChildCount() == 0 {
				deletedParents[key] = true
				destLbl := "删除空目录"
				if isCloud {
					destLbl = "移入 115 回收站"
				}
				emit(OrganizeItem{
					ID:         makeID(),
					Tool:       "empty_dir",
					Scope:      t.Kind,
					TargetName: t.Name,
					Name:       d.Name,
					Detail:     "空文件夹",
					Action:     "delete",
					DestLabel:  destLbl,
					SourceCID:  d.ParentCID,
					FID:        d.FID,
					SourcePath: d.Path,
				})
				continue
			}
			if req.EnableJunk && len(keywords) > 0 && d.ChildCount() > 0 {
				if hit, reason := evalSnapshotJunkFolder(d, keywords, safeExts, kwMaxBytes, wbCfg.JunkKeywordMaxMB); hit {
					deletedParents[key] = true
					destLbl := "删除广告文件夹"
					if isCloud {
						destLbl = "移入 115 回收站"
					}
					emit(OrganizeItem{
						ID:         makeID(),
						Tool:       "junk",
						Scope:      t.Kind,
						TargetName: t.Name,
						Name:       d.Name,
						Detail:     reason,
						Action:     "delete",
						DestLabel:  destLbl,
						SourceCID:  d.ParentCID,
						FID:        d.FID,
						SourcePath: d.Path,
					})
					continue
				}
			}
			if req.EnableFlatten && d.ChildCount() == 1 && !strictDateDirRe.MatchString(d.Name) {
				if cfid, cpath, cname, ok := d.OnlyChild(); ok {
					flattenedParents[key] = true
					emit(OrganizeItem{
						ID:         makeID(),
						Tool:       "flatten",
						Scope:      t.Kind,
						TargetName: t.Name,
						Name:       d.Name + "/" + cname,
						Detail:     "单子项套娃文件夹",
						Action:     "flatten",
						DestLabel:  "提至上层并清壳",
						SourceCID:  d.ParentCID,
						FID:        d.FID,
						ChildFID:   cfid,
						ChildName:  cname,
						SourcePath: d.Path,
						RootPath:   d.ParentDir,
						ChildPath:  cpath,
					})
					continue
				}
			}
		}

		// 2. Collect eligible files (excluding skipped/deleted/flattened parent folders)
		eligibleIdx := make([]int, 0, len(ts.Files))
		localSizeCounts := make(map[int64]int)
		for fi := range ts.Files {
			f := &ts.Files[fi]
			pkey := f.ParentDir
			if isCloud {
				pkey = f.ParentCID
			}
			if skipParents[pkey] || deletedParents[pkey] || flattenedParents[pkey] {
				continue
			}
			eligibleIdx = append(eligibleIdx, fi)
			if !isCloud && f.Size > 0 {
				localSizeCounts[f.Size]++
			}
		}

		// 3. Junk detection FIRST — so blacklisted SHA1 or repeated junk files ALL land in "junk" (not split across duplicate+junk!)
		junkIdx := make(map[int]bool)
		if req.EnableJunk {
			for _, fi := range eligibleIdx {
				f := &ts.Files[fi]
				if !isCloud && f.SHA1 == "" && len(sha1BL) > 0 && f.Size > 0 && f.Size <= 50*1024*1024 {
					if h, err := hashFileSHA1(f.Path); err == nil {
						f.SHA1 = h
					}
				}
				if hit, reason := evalJunkFile(f.Name, f.SHA1, f.Size, junkExts, safeExts, sha1BL, keywords, kwMaxBytes, junkMaxBytes, wbCfg.JunkMaxMB, wbCfg.JunkKeywordMaxMB); hit {
					if !isCloud && f.SHA1 == "" && f.Size > 0 && f.Size <= 50*1024*1024 {
						if h, err := hashFileSHA1(f.Path); err == nil {
							f.SHA1 = h
						}
					}
					junkIdx[fi] = true
					destLbl := "删除文件"
					if isCloud {
						destLbl = "移入 115 回收站"
					}
					emit(OrganizeItem{
						ID:         makeID(),
						Tool:       "junk",
						Scope:      t.Kind,
						TargetName: t.Name,
						Name:       f.Name,
						SHA1:       f.SHA1,
						Size:       f.Size,
						Detail:     reason,
						Action:     "delete",
						DestLabel:  destLbl,
						SourceCID:  f.ParentCID,
						FID:        f.FID,
						SourcePath: f.Path,
					})
				}
			}
		}

		// 4. SHA1 Duplicate detection on non-junk files
		dupIdx := make(map[int]bool)
		if req.EnableDuplicate && len(eligibleIdx) > 1 {
			groups := make(map[string][]int)
			var order []string
			for _, fi := range eligibleIdx {
				if junkIdx[fi] {
					continue
				}
				f := &ts.Files[fi]
				if !isCloud {
					if localSizeCounts[f.Size] < 2 {
						continue
					}
					if f.SHA1 == "" {
						if h, err := hashFileSHA1(f.Path); err == nil {
							f.SHA1 = h
						}
					}
				}
				if f.SHA1 == "" {
					continue
				}
				if _, ok := groups[f.SHA1]; !ok {
					order = append(order, f.SHA1)
				}
				groups[f.SHA1] = append(groups[f.SHA1], fi)
			}
			for _, sha := range order {
				grp := groups[sha]
				if len(grp) < 2 {
					continue
				}
				names := make([]string, len(grp))
				mods := make([]int64, len(grp))
				for i, fi := range grp {
					names[i] = ts.Files[fi].Name
					mods[i] = ts.Files[fi].ModTime
				}
				keepPos := pickDupKeeperIndex(names, mods, wbCfg.DedupKeepPolicy)
				keepName := ts.Files[grp[keepPos]].Name
				for i, fi := range grp {
					if i == keepPos {
						continue
					}
					dupIdx[fi] = true
					f := ts.Files[fi]
					destLbl := "删除重复副本"
					if isCloud {
						destLbl = "移入 115 回收站"
					}
					emit(OrganizeItem{
						ID:         makeID(),
						Tool:       "duplicate",
						Scope:      t.Kind,
						TargetName: t.Name,
						Name:       f.Name,
						SHA1:       f.SHA1,
						Size:       f.Size,
						Detail:     "保留「" + keepName + "」",
						Action:     "delete",
						DestLabel:  destLbl,
						SourceCID:  f.ParentCID,
						FID:        f.FID,
						SourcePath: f.Path,
					})
				}
			}
		}

		// 5. Rename (smart Folder<->Video bidirectional + ad cleanup) & Classify on non-junk, non-duplicate items
		dirByKey := make(map[string]SnapshotDir, len(ts.Dirs))
		for _, d := range ts.Dirs {
			dk := d.Path
			if isCloud {
				dk = d.FID
			}
			if dk != "" {
				dirByKey[dk] = d
			}
		}
		videosByParent := make(map[string][]SnapshotFile)
		usedNamesByParent := make(map[string]map[string]bool)
		for _, fi := range eligibleIdx {
			if junkIdx[fi] || dupIdx[fi] {
				continue
			}
			f := ts.Files[fi]
			pkey := f.ParentDir
			if isCloud {
				pkey = f.ParentCID
			}
			if usedNamesByParent[pkey] == nil {
				usedNamesByParent[pkey] = make(map[string]bool)
			}
			usedNamesByParent[pkey][strings.ToLower(f.Name)] = true
			if isVideoFileName(f.Name) {
				videosByParent[pkey] = append(videosByParent[pkey], f)
			}
		}
		allocUniqueName := func(pkey, stem, ext string) string {
			used := usedNamesByParent[pkey]
			if used == nil {
				used = make(map[string]bool)
				usedNamesByParent[pkey] = used
			}
			cand := stem + ext
			if !used[strings.ToLower(cand)] {
				used[strings.ToLower(cand)] = true
				return cand
			}
			for n := 2; n < 1000; n++ {
				alt := fmt.Sprintf("%s_%02d%s", stem, n, ext)
				if !used[strings.ToLower(alt)] {
					used[strings.ToLower(alt)] = true
					return alt
				}
			}
			return cand
		}

		smartLink := wbCfg.SmartLinkEnabled()
		videoOrderByParent := make(map[string]int)

		for _, fi := range eligibleIdx {
			if junkIdx[fi] || dupIdx[fi] {
				continue
			}
			f := &ts.Files[fi]
			pkey := f.ParentDir
			if isCloud {
				pkey = f.ParentCID
			}
			if req.EnableRename {
				var newName, reason string
				if smartLink && isVideoFileName(f.Name) {
					ext := filepath.Ext(f.Name)
					stem := strings.TrimSuffix(f.Name, ext)
					if isSimpleVideoStem(stem) {
						if pd, ok := dirByKey[pkey]; ok && !strictDateDirRe.MatchString(pd.Name) && !isMeaninglessFolderName(pd.Name) {
							cleanDir, _ := cleanFolderName(pd.Name, wbCfg)
							if hasCJK(cleanDir) {
								videoOrderByParent[pkey]++
								seq := extractVideoSeq(stem)
								targetStem := cleanDir
								if seq != "" {
									targetStem = cleanDir + "_" + seq
								} else if len(videosByParent[pkey]) > 1 {
									targetStem = fmt.Sprintf("%s_%02d", cleanDir, videoOrderByParent[pkey])
								}
								cand := allocUniqueName(pkey, targetStem, ext)
								if cand != f.Name {
									newName = cand
									if seq != "" {
										reason = "视频随目录名（保留序号）"
									} else {
										reason = "视频随目录名"
									}
								}
							}
						}
					}
				}
				if newName == "" {
					if nn, r := cleanFilename(f.Name, wbCfg); nn != f.Name {
						ext := filepath.Ext(nn)
						stem := strings.TrimSuffix(nn, ext)
						newName = allocUniqueName(pkey, stem, ext)
						reason = r
					}
				}
				if newName != "" && newName != f.Name {
					emit(OrganizeItem{
						ID:         makeID(),
						Tool:       "rename",
						Scope:      t.Kind,
						TargetName: t.Name,
						Name:       f.Name,
						NewName:    newName,
						Size:       f.Size,
						Detail:     reason,
						Action:     "rename",
						DestLabel:  "→ " + newName,
						SourceCID:  f.ParentCID,
						FID:        f.FID,
						SourcePath: f.Path,
						RootPath:   f.ParentDir,
					})
				}
			}
			if req.EnableClassify && f.Depth == 0 {
				subDir := classifyFolderName(f.Name, f.ModTime, wbCfg.ClassifyMode, s.loc)
				emit(OrganizeItem{
					ID:         makeID(),
					Tool:       "classify",
					Scope:      t.Kind,
					TargetName: t.Name,
					Name:       f.Name,
					Size:       f.Size,
					Detail:     "归类到「" + subDir + "」",
					Action:     "move",
					DestLabel:  t.Name + "/" + subDir,
					SourceCID:  f.ParentCID,
					RootCID:    t.CID,
					FID:        f.FID,
					SourcePath: f.Path,
					RootPath:   t.Path,
					DestSubdir: subDir,
				})
			}
		}

		// 5b. Folder Renames: meaningless outer folder -> largest descriptive video name; or strip ad prefixes from folder names
		if req.EnableRename {
			dirUsedByParent := make(map[string]map[string]bool)
			for _, d := range ts.Dirs {
				pk := d.ParentDir
				if isCloud {
					pk = d.ParentCID
				}
				if dirUsedByParent[pk] == nil {
					dirUsedByParent[pk] = make(map[string]bool)
				}
				dirUsedByParent[pk][strings.ToLower(d.Name)] = true
			}
			for _, d := range ts.Dirs {
				dk := d.Path
				pk := d.ParentDir
				if isCloud {
					dk = d.FID
					pk = d.ParentCID
				}
				if dk == "" || deletedParents[dk] || flattenedParents[dk] || skipParents[dk] || strictDateDirRe.MatchString(d.Name) {
					continue
				}
				if isCloud && activeCIDs[d.FID] {
					continue
				}
				var newDirName, reason string
				if smartLink && isMeaninglessFolderName(d.Name) {
					// Pick the largest descriptive (CJK) video inside this folder
					var bestVid SnapshotFile
					var bestStem string
					for _, vf := range videosByParent[dk] {
						ext := filepath.Ext(vf.Name)
						rawStem := strings.TrimSuffix(vf.Name, ext)
						cleanStem, _ := cleanNameStem(rawStem, wbCfg)
						if hasCJK(cleanStem) && vf.Size >= bestVid.Size {
							bestVid = vf
							bestStem = cleanStem
						}
					}
					if bestStem != "" && bestStem != d.Name {
						newDirName = bestStem
						reason = "目录随视频名"
					}
				}
				if newDirName == "" {
					if nn, r := cleanFolderName(d.Name, wbCfg); nn != d.Name {
						newDirName = nn
						reason = "目录" + r
					}
				}
				if newDirName != "" && newDirName != d.Name {
					used := dirUsedByParent[pk]
					if used == nil {
						used = make(map[string]bool)
						dirUsedByParent[pk] = used
					}
					cand := newDirName
					if used[strings.ToLower(cand)] {
						for n := 2; n < 1000; n++ {
							alt := fmt.Sprintf("%s (%d)", newDirName, n)
							if !used[strings.ToLower(alt)] {
								cand = alt
								break
							}
						}
					}
					used[strings.ToLower(cand)] = true
					emit(OrganizeItem{
						ID:         makeID(),
						Tool:       "rename",
						Scope:      t.Kind,
						TargetName: t.Name,
						Name:       d.Name,
						NewName:    cand,
						Detail:     reason,
						Action:     "rename",
						DestLabel:  "📁 → " + cand,
						SourceCID:  d.ParentCID,
						FID:        d.FID,
						SourcePath: d.Path,
						RootPath:   d.ParentDir,
					})
				}
			}
		}
	}

	return &CenterScanPlan{
		GeneratedAt: time.Now().UnixNano(),
		Items:       items,
		Counts:      counts,
	}
}

// RecomputeCenterPreview synchronously re-evaluates the requested tools & rules against the cached
// in-memory directory snapshot with ZERO 115 API calls!
func (s *Service) RecomputeCenterPreview(req CenterScanRequest) (*CenterScanPlan, *SnapshotInfo, error) {
	if s.center == nil {
		return nil, nil, errors.New("SNAPSHOT_REQUIRED")
	}
	s.center.mu.Lock()
	if s.center.state.Status == "running" {
		s.center.mu.Unlock()
		return nil, nil, errors.New("ORGANIZE_JOB_BUSY")
	}
	snap := s.center.treeSnap
	now := time.Now()
	if snap == nil || now.Unix() > snap.ExpiresAt {
		s.center.mu.Unlock()
		return nil, nil, errors.New("SNAPSHOT_REQUIRED")
	}
	if len(req.TargetKeys) > 0 && !sameStringSlice(canonicalTargetKeys(req.TargetKeys), snap.TargetKeys) {
		s.center.mu.Unlock()
		return nil, nil, errors.New("SNAPSHOT_TARGET_MISMATCH")
	}
	s.center.mu.Unlock()

	ctx := context.Background()
	wbCfg := s.mergeWorkbenchReq(ctx, req)
	_, p115Cfg := s.snapshotCloud()
	todayStr := time.Now().In(s.loc).Format("2006-01-02")
	activeDates, activeCIDs := s.collectActiveGuards(ctx)

	s.center.mu.Lock()
	defer s.center.mu.Unlock()
	if s.center.treeSnap == nil {
		return nil, nil, errors.New("SNAPSHOT_REQUIRED")
	}
	plan := s.evaluateSnapshot(s.center.treeSnap, req, wbCfg, p115Cfg, todayStr, activeDates, activeCIDs)
	info := s.center.treeSnap.Info(time.Now())
	s.center.state.Plan = plan
	s.center.state.Snapshot = info
	s.center.state.Mode = "scan"
	s.center.state.Status = "done"
	s.center.state.StageLabel = fmt.Sprintf("本地快照秒刷完成，命中 %d 条（0 次 115 请求）", len(plan.Items))
	s.center.addLogLocked(fmt.Sprintf("⚡ 本地快照重算预览（0 请求）：日期 %d · 洗名 %d · 查重 %d · 垃圾 %d · 归类 %d · 解包 %d · 空目录 %d",
		plan.Counts["date_archive"], plan.Counts["rename"], plan.Counts["duplicate"], plan.Counts["junk"], plan.Counts["classify"], plan.Counts["flatten"], plan.Counts["empty_dir"]))
	return plan, info, nil
}

// syncSnapshotAfterExec keeps the cached snapshot accurate after deletions/renames/moves/flattens
// so newly emptied folders and newly created single-child wrappers are detected with 0 API calls!
func syncSnapshotAfterExec(snap *OrganizeSnapshot, completed []OrganizeItem) {
	if snap == nil || len(completed) == 0 {
		return
	}
	delFIDs := make(map[string]bool)
	delPaths := make(map[string]bool)
	renFIDs := make(map[string]string)
	renPaths := make(map[string]string)
	movedFIDs := make(map[string]bool)
	movedPaths := make(map[string]bool)
	flatWrappers := make(map[string]OrganizeItem) // fid or path -> item

	for _, it := range completed {
		switch it.Action {
		case "delete":
			if it.FID != "" {
				delFIDs[it.FID] = true
			}
			if it.SourcePath != "" {
				delPaths[it.SourcePath] = true
			}
		case "rename":
			if it.FID != "" {
				renFIDs[it.FID] = it.NewName
			}
			if it.SourcePath != "" {
				renPaths[it.SourcePath] = it.NewName
			}
		case "flatten":
			if it.FID != "" {
				flatWrappers[it.FID] = it
			}
			if it.SourcePath != "" {
				flatWrappers[it.SourcePath] = it
			}
		default: // move
			if it.FID != "" {
				movedFIDs[it.FID] = true
			}
			if it.SourcePath != "" {
				movedPaths[it.SourcePath] = true
			}
		}
	}

	for ti := range snap.Targets {
		ts := &snap.Targets[ti]
		isCloud := ts.Target.Kind == "cloud"

		// Update Files
		remFiles := ts.Files[:0]
		for _, f := range ts.Files {
			if isCloud {
				if delFIDs[f.FID] || delFIDs[f.ParentCID] || movedFIDs[f.FID] || movedFIDs[f.ParentCID] {
					continue
				}
				if nn, ok := renFIDs[f.FID]; ok {
					f.Name = nn
				}
				if fw, ok := flatWrappers[f.ParentCID]; ok && f.FID == fw.ChildFID {
					f.ParentCID = fw.SourceCID
					if f.Depth > 0 {
						f.Depth--
					}
				}
			} else {
				if delPaths[f.Path] || delPaths[f.ParentDir] || movedPaths[f.Path] || movedPaths[f.ParentDir] {
					continue
				}
				if nn, ok := renPaths[f.Path]; ok {
					f.Name = nn
					f.Path = filepath.Join(f.ParentDir, nn)
				}
				if fw, ok := flatWrappers[f.ParentDir]; ok && f.Path == fw.ChildPath {
					f.ParentDir = fw.RootPath
					f.Path = filepath.Join(fw.RootPath, fw.ChildName)
					if f.Depth > 0 {
						f.Depth--
					}
				}
			}
			remFiles = append(remFiles, f)
		}
		ts.Files = remFiles

		// Update Dirs and their DirectFiles / DirectSubdirs so newly emptied or single-child dirs appear instantly!
		remDirs := ts.Dirs[:0]
		for _, d := range ts.Dirs {
			if isCloud {
				if delFIDs[d.FID] || movedFIDs[d.FID] || flatWrappers[d.FID].ID != "" {
					continue
				}
				if nn, ok := renFIDs[d.FID]; ok {
					d.Name = nn
				}
				nf := d.Files[:0]
				for _, cf := range d.Files {
					if delFIDs[cf.FID] || movedFIDs[cf.FID] {
						continue
					}
					if nn, ok := renFIDs[cf.FID]; ok {
						cf.Name = nn
					}
					nf = append(nf, cf)
				}
				d.Files = nf

				ns := d.Subdirs[:0]
				for _, sd := range d.Subdirs {
					if delFIDs[sd.FID] || movedFIDs[sd.FID] {
						continue
					}
					if fw, ok := flatWrappers[sd.FID]; ok {
						// Wrapper removed, its single child moved up into d!
						d.Files = append(d.Files, SnapshotChildFile{FID: fw.ChildFID, Name: fw.ChildName})
						continue
					}
					ns = append(ns, sd)
				}
				d.Subdirs = ns
			} else {
				if delPaths[d.Path] || movedPaths[d.Path] || flatWrappers[d.Path].ID != "" {
					continue
				}
				nf := d.Files[:0]
				for _, cf := range d.Files {
					if delPaths[cf.Path] || movedPaths[cf.Path] {
						continue
					}
					if nn, ok := renPaths[cf.Path]; ok {
						cf.Name = nn
						cf.Path = filepath.Join(d.Path, nn)
					}
					nf = append(nf, cf)
				}
				d.Files = nf

				ns := d.Subdirs[:0]
				for _, sd := range d.Subdirs {
					if delPaths[sd.Path] || movedPaths[sd.Path] {
						continue
					}
					if fw, ok := flatWrappers[sd.Path]; ok {
						d.Files = append(d.Files, SnapshotChildFile{Path: filepath.Join(d.Path, fw.ChildName), Name: fw.ChildName})
						continue
					}
					ns = append(ns, sd)
				}
				d.Subdirs = ns
			}
			remDirs = append(remDirs, d)
		}
		ts.Dirs = remDirs
	}
}
