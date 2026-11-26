package extract

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var (
	partRarRe  = regexp.MustCompile(`(?i)^(.+?)\.part(\d+)\.rar$`)
	numericRe  = regexp.MustCompile(`(?i)^(.+?\.(?:7z|zip|rar|tar))\.(\d{3})$`)
	oldRarRe   = regexp.MustCompile(`(?i)^(.+?)\.r(\d{2})$`)
	zipSplitRe = regexp.MustCompile(`(?i)^(.+?)\.z(\d{2})$`)
)

type VolumeStatus struct {
	IsArchive     bool
	IsPrimary     bool
	Complete      bool
	MissingReason string
	Members       []string
}

func IsArchiveExt(name string) bool {
	lower := strings.ToLower(name)
	for _, ext := range []string{".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".iso"} {
		if strings.HasSuffix(lower, ext) {
			return true
		}
	}
	return numericRe.MatchString(name) || oldRarRe.MatchString(name) || zipSplitRe.MatchString(name)
}

// InspectVolume checks whether filePath is an archive, whether it is the primary volume,
// and whether all companion split-volume parts are present on disk.
func InspectVolume(filePath string) VolumeStatus {
	dir := filepath.Dir(filePath)
	base := filepath.Base(filePath)

	// Case 1: foo.part01.rar / foo.part1.rar
	if m := partRarRe.FindStringSubmatch(base); len(m) == 3 {
		prefix := m[1]
		num, _ := strconv.Atoi(m[2])
		if num != 1 {
			return VolumeStatus{IsArchive: true, IsPrimary: false}
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return VolumeStatus{IsArchive: true, IsPrimary: true, Complete: false, MissingReason: "READ_DIR_FAILED"}
		}
		foundNums := make(map[int]string)
		maxNum := 1
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			if sm := partRarRe.FindStringSubmatch(e.Name()); len(sm) == 3 && strings.EqualFold(sm[1], prefix) {
				n, _ := strconv.Atoi(sm[2])
				if n > 0 {
					foundNums[n] = filepath.Join(dir, e.Name())
					if n > maxNum {
						maxNum = n
					}
				}
			}
		}
		var members []string
		for i := 1; i <= maxNum; i++ {
			p, ok := foundNums[i]
			if !ok {
				return VolumeStatus{
					IsArchive: true, IsPrimary: true, Complete: false,
					MissingReason: fmt.Sprintf("MISSING_VOLUME_PART_%d", i),
				}
			}
			members = append(members, p)
		}
		return VolumeStatus{IsArchive: true, IsPrimary: true, Complete: true, Members: members}
	}

	// Case 2: foo.7z.001 / foo.zip.001
	if m := numericRe.FindStringSubmatch(base); len(m) == 3 {
		prefix := m[1]
		num, _ := strconv.Atoi(m[2])
		if num != 1 {
			return VolumeStatus{IsArchive: true, IsPrimary: false}
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return VolumeStatus{IsArchive: true, IsPrimary: true, Complete: false, MissingReason: "READ_DIR_FAILED"}
		}
		foundNums := make(map[int]string)
		maxNum := 1
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			if sm := numericRe.FindStringSubmatch(e.Name()); len(sm) == 3 && strings.EqualFold(sm[1], prefix) {
				n, _ := strconv.Atoi(sm[2])
				if n > 0 {
					foundNums[n] = filepath.Join(dir, e.Name())
					if n > maxNum {
						maxNum = n
					}
				}
			}
		}
		var members []string
		for i := 1; i <= maxNum; i++ {
			p, ok := foundNums[i]
			if !ok {
				return VolumeStatus{
					IsArchive: true, IsPrimary: true, Complete: false,
					MissingReason: fmt.Sprintf("MISSING_VOLUME_%03d", i),
				}
			}
			members = append(members, p)
		}
		return VolumeStatus{IsArchive: true, IsPrimary: true, Complete: true, Members: members}
	}

	// Case 3: secondary old-style split volumes (.r00, .z01) are never primary
	if oldRarRe.MatchString(base) || zipSplitRe.MatchString(base) {
		return VolumeStatus{IsArchive: true, IsPrimary: false}
	}

	if !IsArchiveExt(base) {
		return VolumeStatus{IsArchive: false}
	}
	return VolumeStatus{IsArchive: true, IsPrimary: true, Complete: true, Members: []string{filePath}}
}
