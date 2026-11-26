package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
)

var (
	Version   = "0.1.0-dev"
	Commit    = "local"
	BuildTime = ""
)

type Config struct {
	ListenAddr        string
	DataDir           string
	WorkDir           string
	Timezone          string
	LogLevel          string
	NoAuth            bool
	MemLimitMB        int64
	ExtractBin        string
	ExtractMaxDictMB  int
	ExtractReserveMB  int64
	SecretKeyHex      string
}

func Load() Config {
	cfg := Config{
		ListenAddr:       envString("ARK_LISTEN_ADDR", ":5656"),
		DataDir:          envString("ARK_DATA_DIR", "data"),
		WorkDir:          envString("ARK_WORK_DIR", ""),
		Timezone:         envString("ARK_TZ", "Asia/Shanghai"),
		LogLevel:         strings.ToLower(envString("ARK_LOG_LEVEL", "info")),
		NoAuth:           envBool("ARK_NO_AUTH", true),
		MemLimitMB:       envInt64("ARK_MEM_LIMIT_MB", 48),
		ExtractBin:       envString("ARK_EXTRACT_BIN", ""),
		ExtractMaxDictMB: int(envInt64("ARK_EXTRACT_MAX_DICT_MB", 0)),
		ExtractReserveMB: envInt64("ARK_EXTRACT_RESERVE_MB", 64),
		SecretKeyHex:     strings.TrimSpace(os.Getenv("ARK_SECRET_KEY")),
	}
	if cfg.ExtractBin == "" {
		cfg.ExtractBin = detectSevenZip()
	}
	return cfg
}

func (c Config) DBPath() string {
	return filepath.Join(c.DataDir, "ark.db")
}

func (c Config) SecretKeyPath() string {
	return filepath.Join(c.DataDir, "secret.key")
}

func (c Config) EnsureDirs() error {
	if err := os.MkdirAll(c.DataDir, 0o755); err != nil {
		return err
	}
	if c.WorkDir != "" {
		for _, sub := range []string{"watch", "output", "cache", "archive"} {
			if err := os.MkdirAll(filepath.Join(c.WorkDir, sub), 0o755); err != nil {
				return err
			}
		}
	}
	return nil
}

// ApplyRuntimeTuning configures Go GC memory limits for low-memory NAS environments.
func (c Config) ApplyRuntimeTuning() {
	if os.Getenv("GOGC") == "" {
		debug.SetGCPercent(50)
	}
	if os.Getenv("GOMEMLIMIT") == "" && c.MemLimitMB > 0 {
		debug.SetMemoryLimit(c.MemLimitMB * 1024 * 1024)
	}
}

func detectSevenZip() string {
	for _, name := range []string{"7zz", "7z"} {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	if runtime.GOOS == "windows" {
		for _, p := range []string{
			`C:\Program Files\7-Zip\7z.exe`,
			`C:\Program Files (x86)\7-Zip\7z.exe`,
		} {
			if _, err := os.Stat(p); err == nil {
				return p
			}
		}
	}
	return ""
}

func envString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return parsed
}

func envInt64(key string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
