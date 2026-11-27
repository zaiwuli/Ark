package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/zaiwuli/Ark/internal/p115"
)

var strictDateDirRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

type OrganizerConfig struct {
	AutoEnabled     bool     `json:"auto_enabled"`
	RetentionMode   string   `json:"retention_mode"` // current_month (default) | days
	RetentionDays   int      `json:"retention_days"` // default 14 (used when mode == "days")
	CleanEmpty      bool     `json:"clean_empty"`    // default true
	IncludeCloud    bool     `json:"include_cloud"`  // default true
	IncludeLocal    bool     `json:"include_local"`  // default true
	CloudCategories []string `json:"cloud_categories,omitempty"` // offline | output | archive (nil => all 3)
	LocalCategories []string `json:"local_categories,omitempty"` // watch | output | archive (nil => output, archive)
	ExtraSourceIDs  []string `json:"extra_source_ids,omitempty"` // user-added cloud/local source IDs
	LastAutoKey     string   `json:"last_auto_key,omitempty"`
}

func DefaultOrganizerConfig() OrganizerConfig {
	return OrganizerConfig{
		AutoEnabled:   false,
		RetentionMode: "current_month",
		RetentionDays: 14,
		CleanEmpty:    true,
		IncludeCloud:  true,
		IncludeLocal:  true,
	}
}

func (c OrganizerConfig) Normalize() OrganizerConfig {
	out := c
	if out.RetentionMode != "current_month" && out.RetentionMode != "days" {
		out.RetentionMode = "current_month"
	}
	if out.RetentionDays <= 0 {
		out.RetentionDays = 14
	}
	if out.RetentionDays > 365 {
		out.RetentionDays = 365
	}
	return out
}

type OrganizeAction struct {
	Scope      string `json:"scope"`       // cloud | local
	Category   string `json:"category"`    // 离线下载 | 解压输出 | 原包留存
	Date       string `json:"date"`        // YYYY-MM-DD
	Action     string `json:"action"`      // move | delete_empty
	SourcePath string `json:"source_path"` // local path or cloud label
	TargetPath string `json:"target_path"` // e.g. 归档区/历史归档/解压输出/2026年/2026-10/2026-10-03
	// Internal fields for execution (not needed by UI)
	SourceCID string `json:"-"`
	FolderFID string `json:"-"`
}

type OrganizePlan struct {
	GeneratedAt   int64            `json:"generated_at"`
	Config        OrganizerConfig  `json:"config"`
	Actions       []OrganizeAction `json:"actions"`
	MoveCount     int              `json:"move_count"`
	EmptyCount    int              `json:"empty_count"`
	SkippedActive int              `json:"skipped_active"`
}

type OrganizeResult struct {
	MovedCount    int      `json:"moved_count"`
	DeletedEmpty  int      `json:"deleted_empty"`
	SkippedActive int      `json:"skipped_active"`
	FailedCount   int      `json:"failed_count"`
	Errors        []string `json:"errors,omitempty"`
}

func (s *Service) OrganizerConfig(ctx context.Context) OrganizerConfig {
	cfg := DefaultOrganizerConfig()
	_, _ = s.store.GetSetting(ctx, "organizer.config", &cfg)
	return cfg.Normalize()
}

func (s *Service) SaveOrganizerConfig(ctx context.Context, incoming OrganizerConfig) (OrganizerConfig, error) {
	existing := s.OrganizerConfig(ctx)
	norm := incoming.Normalize()
	if norm.LastAutoKey == "" {
		norm.LastAutoKey = existing.LastAutoKey
	}
	if err := s.store.SetSetting(ctx, "organizer.config", norm, false); err != nil {
		return OrganizerConfig{}, err
	}
	return norm, nil
}

func (s *Service) isDateEligible(name string, now time.Time, cfg OrganizerConfig, manual bool) (time.Time, bool) {
	if !strictDateDirRe.MatchString(name) {
		return time.Time{}, false
	}
	parsed, err := time.ParseInLocation("2006-01-02", name, s.loc)
	if err != nil {
		return time.Time{}, false
	}
	todayStr := now.Format("2006-01-02")
	if name >= todayStr {
		return time.Time{}, false
	}
	if manual {
		// Manual organization archives yesterday and all earlier YYYY-MM-DD folders.
		return parsed, true
	}
	if cfg.RetentionMode == "days" {
		cutoff := now.AddDate(0, 0, -cfg.RetentionDays).Format("2006-01-02")
		return parsed, name < cutoff
	}
	// Default: current_month -> keep everything in current YYYY-MM, fold earlier months.
	curMonth := now.Format("2006-01")
	return parsed, name[:7] < curMonth
}

func categorySelected(selected []string, key string, defaultVal bool) bool {
	if selected == nil {
		return defaultVal
	}
	for _, s := range selected {
		if s == key {
			return true
		}
	}
	return false
}

func sanitizeArchiveCatName(name string) string {
	s := strings.TrimSpace(name)
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	if s == "" {
		return "其他来源"
	}
	return s
}

func historyTargetRel(category, dateStr string) (yearDir, monthDir, display string) {
	yearDir = dateStr[:4] + "年"
	monthDir = dateStr[:7]
	display = fmt.Sprintf("归档区/历史归档/%s/%s/%s/%s", category, yearDir, monthDir, dateStr)
	return yearDir, monthDir, display
}

// PreviewOrganize scans managed folders using the configured automatic retention policy.
func (s *Service) PreviewOrganize(ctx context.Context) (OrganizePlan, error) {
	return s.buildOrganizePlan(ctx, time.Now().In(s.loc), false)
}

// PreviewOrganizeWithMode produces a dry-run plan; when manual is true, yesterday and all earlier dates are included
// and per-subfolder cloud empty checks are skipped for instant 1-2s preview (actual empty check happens on run).
func (s *Service) PreviewOrganizeWithMode(ctx context.Context, manual bool) (OrganizePlan, error) {
	return s.buildOrganizePlanOpts(ctx, time.Now().In(s.loc), manual, !manual)
}

func (s *Service) buildOrganizePlan(ctx context.Context, now time.Time, manual bool) (OrganizePlan, error) {
	return s.buildOrganizePlanOpts(ctx, now, manual, true)
}

func (s *Service) buildOrganizePlanOpts(ctx context.Context, now time.Time, manual, probeCloudEmpty bool) (OrganizePlan, error) {
	orgCfg := s.OrganizerConfig(ctx)
	activeDates, activeCIDs, err := s.store.ActiveDatesAndCIDs(ctx)
	if err != nil {
		return OrganizePlan{}, err
	}

	extraSet := make(map[string]bool, len(orgCfg.ExtraSourceIDs))
	for _, id := range orgCfg.ExtraSourceIDs {
		if strings.TrimSpace(id) != "" {
			extraSet[strings.TrimSpace(id)] = true
		}
	}

	plan := OrganizePlan{
		GeneratedAt: now.Unix(),
		Config:      orgCfg,
		Actions:     make([]OrganizeAction, 0),
	}

	// 1. Scan local managed folders & selected extra local sources
	type localTarget struct {
		cat string
		dir string
	}
	var localTargets []localTarget
	if orgCfg.IncludeLocal {
		if categorySelected(orgCfg.LocalCategories, "watch", false) {
			localTargets = append(localTargets, localTarget{"本地监控", filepath.Join(s.workDir, "本地监控")})
		}
		if categorySelected(orgCfg.LocalCategories, "output", true) {
			localTargets = append(localTargets, localTarget{"解压输出", s.outputDir()})
		}
		if categorySelected(orgCfg.LocalCategories, "archive", true) {
			localTargets = append(localTargets, localTarget{"原包留存", s.originalsDir()})
		}
	}
	if len(extraSet) > 0 {
		if localSources, lErr := s.store.ListSources(ctx, "local"); lErr == nil {
			for _, src := range localSources {
				if !extraSet[src.ID] {
					continue
				}
				var sc struct {
					Path string `json:"path"`
				}
				if json.Unmarshal([]byte(src.ConfigJSON), &sc) == nil && strings.TrimSpace(sc.Path) != "" {
					localTargets = append(localTargets, localTarget{
						cat: sanitizeArchiveCatName(src.Name),
						dir: strings.TrimSpace(sc.Path),
					})
				}
			}
		}
	}
	for _, lc := range localTargets {
		entries, err := os.ReadDir(lc.dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if _, ok := s.isDateEligible(name, now, orgCfg, manual); !ok {
				continue
			}
			if activeDates[name] {
				plan.SkippedActive++
				continue
			}
			full := filepath.Join(lc.dir, name)
			children, _ := os.ReadDir(full)
			_, _, dispTarget := historyTargetRel(lc.cat, name)
			if len(children) == 0 && orgCfg.CleanEmpty {
				plan.Actions = append(plan.Actions, OrganizeAction{
					Scope:      "local",
					Category:   lc.cat,
					Date:       name,
					Action:     "delete_empty",
					SourcePath: full,
					TargetPath: "清理空目录",
				})
				plan.EmptyCount++
			} else {
				plan.Actions = append(plan.Actions, OrganizeAction{
					Scope:      "local",
					Category:   lc.cat,
					Date:       name,
					Action:     "move",
					SourcePath: full,
					TargetPath: dispTarget,
				})
				plan.MoveCount++
			}
		}
	}

	// 2. Scan 115 cloud managed folders & selected extra cloud sources
	cloud, p115Cfg := s.snapshotCloud()
	if cloud != nil && (p115Cfg.CloudHistoryCID != "" || p115Cfg.CloudArchiveZoneCID != "" || p115Cfg.CloudRootCID != "") {
		type cloudTarget struct {
			cat string
			cid string
		}
		var cloudTargets []cloudTarget
		if orgCfg.IncludeCloud {
			if categorySelected(orgCfg.CloudCategories, "offline", true) && p115Cfg.OfflineRootCID != "" {
				cloudTargets = append(cloudTargets, cloudTarget{"离线下载", p115Cfg.OfflineRootCID})
			}
			if categorySelected(orgCfg.CloudCategories, "output", true) && p115Cfg.CloudOutputCID != "" {
				cloudTargets = append(cloudTargets, cloudTarget{"解压输出", p115Cfg.CloudOutputCID})
			}
			if categorySelected(orgCfg.CloudCategories, "archive", true) && p115Cfg.CloudArchiveCID != "" {
				cloudTargets = append(cloudTargets, cloudTarget{"原包留存", p115Cfg.CloudArchiveCID})
			}
		}
		if len(extraSet) > 0 {
			if cloudSources, cErr := s.store.ListSources(ctx, "cloud"); cErr == nil {
				for _, src := range cloudSources {
					if !extraSet[src.ID] {
						continue
					}
					var sc struct {
						CID string `json:"cid"`
					}
					if json.Unmarshal([]byte(src.ConfigJSON), &sc) == nil && strings.TrimSpace(sc.CID) != "" {
						cloudTargets = append(cloudTargets, cloudTarget{
							cat: sanitizeArchiveCatName(src.Name),
							cid: strings.TrimSpace(sc.CID),
						})
					}
				}
			}
		}
		for _, cc := range cloudTargets {
			if cc.cid == "" {
				continue
			}
			files, err := listAllCloudFiles(ctx, cloud, cc.cid)
			if err != nil {
				continue
			}
			for _, f := range files {
				if !f.IsDir {
					continue
				}
				if _, ok := s.isDateEligible(f.Name, now, orgCfg, manual); !ok {
					continue
				}
				if activeDates[f.Name] || activeCIDs[f.FID] {
					plan.SkippedActive++
					continue
				}
				isEmpty := false
				if probeCloudEmpty && orgCfg.CleanEmpty {
					sub, subErr := cloud.Files(ctx, f.FID, 0)
					isEmpty = subErr == nil && len(sub.Files) == 0
				}
				_, _, dispTarget := historyTargetRel(cc.cat, f.Name)
				if isEmpty && orgCfg.CleanEmpty {
					plan.Actions = append(plan.Actions, OrganizeAction{
						Scope:      "cloud",
						Category:   cc.cat,
						Date:       f.Name,
						Action:     "delete_empty",
						SourcePath: cc.cat + "/" + f.Name,
						TargetPath: "清理空目录",
						SourceCID:  cc.cid,
						FolderFID:  f.FID,
					})
					plan.EmptyCount++
				} else {
					plan.Actions = append(plan.Actions, OrganizeAction{
						Scope:      "cloud",
						Category:   cc.cat,
						Date:       f.Name,
						Action:     "move",
						SourcePath: cc.cat + "/" + f.Name,
						TargetPath: dispTarget,
						SourceCID:  cc.cid,
						FolderFID:  f.FID,
					})
					plan.MoveCount++
				}
			}
		}
	}

	sort.Slice(plan.Actions, func(i, j int) bool {
		if plan.Actions[i].Scope != plan.Actions[j].Scope {
			return plan.Actions[i].Scope < plan.Actions[j].Scope
		}
		if plan.Actions[i].Category != plan.Actions[j].Category {
			return plan.Actions[i].Category < plan.Actions[j].Category
		}
		return plan.Actions[i].Date < plan.Actions[j].Date
	})
	return plan, nil
}

// RunOrganize executes the date-folder organization plan using the automatic retention policy.
func (s *Service) RunOrganize(ctx context.Context, notifyUser bool) (OrganizeResult, error) {
	return s.runOrganizeAt(ctx, time.Now().In(s.loc), notifyUser, false)
}

// RunOrganizeWithMode executes the date-folder organization plan; when manual is true, yesterday and earlier are archived.
func (s *Service) RunOrganizeWithMode(ctx context.Context, notifyUser, manual bool) (OrganizeResult, error) {
	return s.runOrganizeAt(ctx, time.Now().In(s.loc), notifyUser, manual)
}

func (s *Service) runOrganizeAt(ctx context.Context, now time.Time, notifyUser, manual bool) (OrganizeResult, error) {
	plan, err := s.buildOrganizePlan(ctx, now, manual)
	if err != nil {
		return OrganizeResult{}, err
	}
	res := OrganizeResult{
		SkippedActive: plan.SkippedActive,
	}
	if len(plan.Actions) == 0 {
		return res, nil
	}

	cloud, p115Cfg := s.snapshotCloud()
	cloudHistoryCID := p115Cfg.CloudHistoryCID
	ensureCloudHistory := func() (string, error) {
		if cloudHistoryCID != "" {
			return cloudHistoryCID, nil
		}
		if cloud == nil {
			return "", ErrP115NotConfigured
		}
		zoneCID := p115Cfg.CloudArchiveZoneCID
		if zoneCID == "" && p115Cfg.CloudRootCID != "" {
			var err error
			zoneCID, err = cloud.EnsureFolder(ctx, p115Cfg.CloudRootCID, "归档区")
			if err != nil {
				return "", err
			}
			p115Cfg.CloudArchiveZoneCID = zoneCID
		}
		if zoneCID == "" {
			return "", ErrCloudRootRequired
		}
		hid, err := cloud.EnsureFolder(ctx, zoneCID, "历史归档")
		if err != nil {
			return "", err
		}
		cloudHistoryCID = hid
		p115Cfg.CloudHistoryCID = hid
		_, _ = s.SaveP115Config(ctx, p115Cfg)
		return hid, nil
	}

	const maxCloudOpsPerRun = 50
	cloudOps := 0
	monthFolderCache := make(map[string]string)

	for _, act := range plan.Actions {
		if act.Scope == "local" {
			if act.Action == "delete_empty" {
				if err := os.Remove(act.SourcePath); err != nil {
					res.FailedCount++
					res.Errors = append(res.Errors, act.SourcePath+": "+err.Error())
				} else {
					res.DeletedEmpty++
				}
				continue
			}
			yearDir, monthDir, _ := historyTargetRel(act.Category, act.Date)
			destParent := filepath.Join(s.historyDir(), act.Category, yearDir, monthDir)
			if err := os.MkdirAll(destParent, 0o755); err != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, act.SourcePath+": "+err.Error())
				continue
			}
			destDir := uniqueDestPath(filepath.Join(destParent, act.Date))
			if err := os.Rename(act.SourcePath, destDir); err != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, act.SourcePath+": "+err.Error())
				continue
			}
			res.MovedCount++
			continue
		}

		// Cloud operation (rate-limited cap per run to stay friendly to 115)
		if cloud == nil {
			continue
		}
		if cloudOps >= maxCloudOpsPerRun {
			break
		}
		cloudOps++

		if act.Action == "delete_empty" {
			if err := cloud.DeleteVerified(ctx, p115.FileLocator{FID: act.FolderFID, CID: act.SourceCID}); err != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, act.SourcePath+": "+err.Error())
			} else {
				res.DeletedEmpty++
				s.invalidateDateCache(act.SourceCID, act.Date)
			}
			continue
		}

		histRoot, err := ensureCloudHistory()
		if err != nil {
			res.FailedCount++
			res.Errors = append(res.Errors, act.SourcePath+": "+err.Error())
			continue
		}
		yearDir, monthDir, _ := historyTargetRel(act.Category, act.Date)
		cacheKey := act.Category + "/" + yearDir + "/" + monthDir
		targetMonthCID, ok := monthFolderCache[cacheKey]
		if !ok {
			catCID, err := cloud.EnsureFolder(ctx, histRoot, act.Category)
			if err != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, act.SourcePath+": "+err.Error())
				continue
			}
			yearCID, err := cloud.EnsureFolder(ctx, catCID, yearDir)
			if err != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, act.SourcePath+": "+err.Error())
				continue
			}
			targetMonthCID, err = cloud.EnsureFolder(ctx, yearCID, monthDir)
			if err != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, act.SourcePath+": "+err.Error())
				continue
			}
			monthFolderCache[cacheKey] = targetMonthCID
		}

		if err := cloud.MoveVerified(ctx, p115.FileLocator{FID: act.FolderFID, CID: act.SourceCID}, targetMonthCID); err != nil {
			res.FailedCount++
			res.Errors = append(res.Errors, act.SourcePath+": "+err.Error())
			continue
		}
		res.MovedCount++
		s.invalidateDateCache(act.SourceCID, act.Date)
	}

	if res.MovedCount > 0 || res.DeletedEmpty > 0 {
		msg := fmt.Sprintf("日期目录整理完成：归并 %d 个旧日期目录到归档区/历史归档，清理空目录 %d 个", res.MovedCount, res.DeletedEmpty)
		if res.SkippedActive > 0 {
			msg += fmt.Sprintf("（跳过 %d 个进行中任务目录）", res.SkippedActive)
		}
		_ = s.store.RecordEvent(ctx, "system", "DATE_ORGANIZED", "organize", msg, notifyUser)
	}
	return res, nil
}

func (s *Service) invalidateDateCache(rootCID, dateStr string) {
	s.mu.Lock()
	delete(s.dateCache, rootCID+":"+dateStr)
	s.mu.Unlock()
}

func (s *Service) maybeAutoOrganize(ctx context.Context) {
	cfg := s.OrganizerConfig(ctx)
	if !cfg.AutoEnabled {
		return
	}
	now := time.Now().In(s.loc)
	key := now.Format("2006-01")
	if cfg.RetentionMode == "days" {
		key = now.Format("2006-01-02")
	}
	if cfg.LastAutoKey == key {
		return
	}
	res, err := s.runOrganizeAt(ctx, now, true, false)
	if err == nil && res.FailedCount == 0 {
		cfg.LastAutoKey = key
		_, _ = s.SaveOrganizerConfig(ctx, cfg)
	}
}
