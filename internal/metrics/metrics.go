package metrics

import (
	"bufio"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

type GoMemory struct {
	HeapAllocBytes  uint64 `json:"heap_alloc_bytes"`
	HeapInuseBytes  uint64 `json:"heap_inuse_bytes"`
	HeapSysBytes    uint64 `json:"heap_sys_bytes"`
	SysBytes        uint64 `json:"sys_bytes"`
	NumGC           uint32 `json:"num_gc"`
	Goroutines      int    `json:"goroutines"`
	GoMemLimitBytes int64  `json:"gomemlimit_bytes"`
}

type ProcessMemory struct {
	Available    bool    `json:"available"`
	RSSBytes     *uint64 `json:"rss_bytes"`
	PeakRSSBytes *uint64 `json:"peak_rss_bytes"`
	PrivateBytes *uint64 `json:"private_bytes"`
	PSSBytes     *uint64 `json:"pss_bytes"`
}

type ContainerMemory struct {
	Available       bool    `json:"available"`
	CgroupVersion   string  `json:"cgroup_version,omitempty"`
	UsageBytes      *uint64 `json:"usage_bytes"`
	LimitBytes      *uint64 `json:"limit_bytes"`
	AnonymousBytes  *uint64 `json:"anonymous_bytes"`
	FileCacheBytes  *uint64 `json:"file_cache_bytes"`
	WorkingSetBytes *uint64 `json:"working_set_bytes"`
}

type MemorySnapshot struct {
	Go        GoMemory        `json:"go"`
	Process   ProcessMemory   `json:"process"`
	Container ContainerMemory `json:"container"`
}

type StartupStage struct {
	Stage         string  `json:"stage"`
	AtUnixMs      int64   `json:"at_unix_ms"`
	HeapAlloc     uint64  `json:"heap_alloc_bytes"`
	RSSBytes      *uint64 `json:"rss_bytes"`
	RSSDeltaBytes *int64  `json:"rss_delta_bytes"`
}

type Collector struct {
	mu      sync.Mutex
	started time.Time
	stages  []StartupStage
	lastRSS *uint64
}

func NewCollector() *Collector {
	return &Collector{started: time.Now()}
}

func (c *Collector) UptimeSeconds() int64 {
	return int64(time.Since(c.started).Seconds())
}

func (c *Collector) MarkStartup(stage string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.stages) >= 32 {
		return
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	proc := readProcessMemory()
	var delta *int64
	if proc.RSSBytes != nil && c.lastRSS != nil {
		d := int64(*proc.RSSBytes) - int64(*c.lastRSS)
		delta = &d
	}
	if proc.RSSBytes != nil {
		v := *proc.RSSBytes
		c.lastRSS = &v
	}
	c.stages = append(c.stages, StartupStage{
		Stage:         stage,
		AtUnixMs:      time.Now().UnixMilli(),
		HeapAlloc:     ms.HeapAlloc,
		RSSBytes:      proc.RSSBytes,
		RSSDeltaBytes: delta,
	})
}

func (c *Collector) StartupStages() []StartupStage {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]StartupStage, len(c.stages))
	copy(out, c.stages)
	return out
}

func Snapshot() MemorySnapshot {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return MemorySnapshot{
		Go: GoMemory{
			HeapAllocBytes:  ms.HeapAlloc,
			HeapInuseBytes:  ms.HeapInuse,
			HeapSysBytes:    ms.HeapSys,
			SysBytes:        ms.Sys,
			NumGC:           ms.NumGC,
			Goroutines:      runtime.NumGoroutine(),
			GoMemLimitBytes: debug.SetMemoryLimit(-1),
		},
		Process:   readProcessMemory(),
		Container: readContainerMemory(),
	}
}

func readProcessMemory() ProcessMemory {
	if runtime.GOOS != "linux" {
		return ProcessMemory{Available: false}
	}
	pm := ProcessMemory{}
	if f, err := os.Open("/proc/self/status"); err == nil {
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if v, ok := parseProcKB(line, "VmRSS:"); ok {
				pm.RSSBytes = &v
				pm.Available = true
			} else if v, ok := parseProcKB(line, "VmHWM:"); ok {
				pm.PeakRSSBytes = &v
			}
		}
	}
	if f, err := os.Open("/proc/self/smaps_rollup"); err == nil {
		defer f.Close()
		var pss, privClean, privDirty uint64
		var hasPriv bool
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := sc.Text()
			if v, ok := parseProcKB(line, "Pss:"); ok {
				pss = v
				pm.PSSBytes = &pss
			} else if v, ok := parseProcKB(line, "Private_Clean:"); ok {
				privClean = v
				hasPriv = true
			} else if v, ok := parseProcKB(line, "Private_Dirty:"); ok {
				privDirty = v
				hasPriv = true
			}
		}
		if hasPriv {
			total := privClean + privDirty
			pm.PrivateBytes = &total
		}
	}
	return pm
}

func readContainerMemory() ContainerMemory {
	if runtime.GOOS != "linux" {
		return ContainerMemory{Available: false}
	}
	// Try cgroup v2 first
	if cur, ok := readUintFile("/sys/fs/cgroup/memory.current"); ok {
		cm := ContainerMemory{
			Available:     true,
			CgroupVersion: "v2",
			UsageBytes:    &cur,
		}
		if lim, ok := readUintFile("/sys/fs/cgroup/memory.max"); ok {
			cm.LimitBytes = &lim
		}
		stats := readKeyValueFile("/sys/fs/cgroup/memory.stat")
		if anon, ok := stats["anon"]; ok {
			cm.AnonymousBytes = &anon
		}
		if file, ok := stats["file"]; ok {
			cm.FileCacheBytes = &file
		}
		if inact, ok := stats["inactive_file"]; ok && cur >= inact {
			ws := cur - inact
			cm.WorkingSetBytes = &ws
		}
		return cm
	}
	// Fallback cgroup v1
	if cur, ok := readUintFile("/sys/fs/cgroup/memory/memory.usage_in_bytes"); ok {
		cm := ContainerMemory{
			Available:     true,
			CgroupVersion: "v1",
			UsageBytes:    &cur,
		}
		if lim, ok := readUintFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); ok && lim < 1<<60 {
			cm.LimitBytes = &lim
		}
		stats := readKeyValueFile("/sys/fs/cgroup/memory/memory.stat")
		if rss, ok := stats["rss"]; ok {
			cm.AnonymousBytes = &rss
		}
		if cache, ok := stats["cache"]; ok {
			cm.FileCacheBytes = &cache
		}
		if inact, ok := stats["total_inactive_file"]; ok && cur >= inact {
			ws := cur - inact
			cm.WorkingSetBytes = &ws
		}
		return cm
	}
	return ContainerMemory{Available: false}
}

func parseProcKB(line, prefix string) (uint64, bool) {
	if !strings.HasPrefix(line, prefix) {
		return 0, false
	}
	fields := strings.Fields(strings.TrimPrefix(line, prefix))
	if len(fields) == 0 {
		return 0, false
	}
	v, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, false
	}
	return v * 1024, true
}

func readUintFile(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	s := strings.TrimSpace(string(data))
	if s == "" || s == "max" {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func readKeyValueFile(path string) map[string]uint64 {
	res := make(map[string]uint64)
	f, err := os.Open(path)
	if err != nil {
		return res
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) == 2 {
			if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
				res[fields[0]] = v
			}
		}
	}
	return res
}
