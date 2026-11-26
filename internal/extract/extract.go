package extract

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultMaxFiles      = 10000
	DefaultMaxBytes      = 64 * 1024 * 1024 * 1024 // 64 GiB
	maxMetadataBytes     = 32 * 1024 * 1024
	maxNestDepth         = 10
	maxPathDepth         = 25
	maxFilenameUTF8Bytes = 240
)

var (
	ErrExtractorUnavailable = errors.New("EXTRACTOR_UNAVAILABLE")
	ErrSpecialFileRejected  = errors.New("ARCHIVE_SPECIAL_FILE_REJECTED")
	ErrUnsafePath           = errors.New("UNSAFE_ARCHIVE_PATH")
	ErrDuplicateEntry       = errors.New("DUPLICATE_ARCHIVE_ENTRY")
	ErrFileLimit            = errors.New("ARCHIVE_FILE_LIMIT")
	ErrSizeLimit            = errors.New("ARCHIVE_SIZE_LIMIT")
	ErrDepthLimit           = errors.New("ARCHIVE_DEPTH_LIMIT")
	ErrEmptyArchive         = errors.New("ARCHIVE_HAS_NO_FILES")
	ErrPasswordOrCorrupt    = errors.New("PASSWORD_OR_CORRUPT_ARCHIVE")
	ErrDictMemoryLimit      = errors.New("ARCHIVE_DICT_EXCEEDS_MEMORY_LIMIT")
)

type Options struct {
	Binary           string
	Source           string
	OutputDir        string // Final destination directory (created atomically)
	Passwords        []string
	MaxBytes         int64
	MaxFiles         int
	MaxDictMB        int
	SquashRoot       bool
	DisableRecursion bool
	ExtractISO       bool
}

type ExtractedFile struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

type Result struct {
	Engine       string            `json:"engine"`
	PasswordUsed string            `json:"password_used,omitempty"`
	OutputPath   string            `json:"output_path"`
	Files        []ExtractedFile   `json:"files"`
	TotalBytes   int64             `json:"total_bytes"`
	NameMapping  map[string]string `json:"name_mapping,omitempty"`
}

// Engine serializes local extractions (concurrency = 1) to protect NAS memory budgets.
type Engine struct {
	mu sync.Mutex
}

func NewEngine() *Engine {
	return &Engine{}
}

func (e *Engine) Extract(ctx context.Context, opt Options) (Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.extractInternal(ctx, opt, 0)
}

func (e *Engine) extractInternal(ctx context.Context, opt Options, depth int) (Result, error) {
	if depth > maxNestDepth {
		return Result{}, ErrDepthLimit
	}
	if opt.MaxBytes <= 0 {
		opt.MaxBytes = DefaultMaxBytes
	}
	if opt.MaxFiles <= 0 {
		opt.MaxFiles = DefaultMaxFiles
	}

	parent := filepath.Dir(opt.OutputDir)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return Result{}, err
	}
	staging, err := os.MkdirTemp(parent, ".ark-stage-*")
	if err != nil {
		return Result{}, err
	}
	keepStaging := false
	defer func() {
		if !keepStaging {
			_ = os.RemoveAll(staging)
		}
	}()

	var engineName, usedPwd string
	if opt.Binary != "" {
		usedPwd, err = runSevenZipSingleShot(ctx, opt, staging)
		engineName = "7zip"
	} else {
		engineName, err = runBuiltinFallback(ctx, opt, staging)
	}
	if err != nil {
		return Result{}, err
	}

	// Stage 3: Post-process inside staging (security walk, long name mapping, squash root, optional recursion).
	nameMapping, err := sanitizeAndMapStaging(staging, opt.MaxFiles, opt.MaxBytes)
	if err != nil {
		return Result{}, err
	}

	if opt.SquashRoot {
		if err := squashSingleWrapper(staging); err != nil {
			return Result{}, err
		}
	}

	files, totalBytes, err := collectManifest(staging, opt.MaxFiles, opt.MaxBytes)
	if err != nil {
		return Result{}, err
	}
	if len(files) == 0 {
		return Result{}, ErrEmptyArchive
	}

	if !opt.DisableRecursion {
		for _, f := range append([]ExtractedFile(nil), files...) {
			nestedPath := filepath.Join(staging, filepath.FromSlash(f.Path))
			ext := strings.ToLower(filepath.Ext(nestedPath))
			if !IsArchiveExt(nestedPath) {
				continue
			}
			if ext == ".iso" && !opt.ExtractISO {
				continue
			}
			vol := InspectVolume(nestedPath)
			if !vol.IsArchive || !vol.IsPrimary || !vol.Complete {
				continue
			}
			childOut := strings.TrimSuffix(nestedPath, filepath.Ext(nestedPath))
			childOpt := opt
			childOpt.Source = nestedPath
			childOpt.OutputDir = uniqueDir(childOut)
			childOpt.MaxBytes = opt.MaxBytes - totalBytes
			childOpt.MaxFiles = opt.MaxFiles - len(files)
			if childOpt.MaxBytes <= 0 || childOpt.MaxFiles <= 0 {
				return Result{}, ErrSizeLimit
			}
			childRes, err := e.extractInternal(ctx, childOpt, depth+1)
			if err != nil {
				return Result{}, err
			}
			totalBytes += childRes.TotalBytes
			for k, v := range childRes.NameMapping {
				nameMapping[k] = v
			}
		}
		files, totalBytes, err = collectManifest(staging, opt.MaxFiles, opt.MaxBytes)
		if err != nil {
			return Result{}, err
		}
	}

	finalOut := uniqueDir(opt.OutputDir)
	if err := os.Rename(staging, finalOut); err != nil {
		return Result{}, err
	}
	keepStaging = true

	return Result{
		Engine:       engineName,
		PasswordUsed: usedPwd,
		OutputPath:   finalOut,
		Files:        files,
		TotalBytes:   totalBytes,
		NameMapping:  nameMapping,
	}, nil
}

// Stage 1 + Stage 2 for official 7-Zip (7zz):
// 1. Lightweight `7zz l -slt` preflight across password candidates (security, budget, dictionary memory check).
// 2. Single `7zz x -mmt=1 -o<staging>` extraction (no O(N^2) per-file solid-block re-decompression!).
func runSevenZipSingleShot(ctx context.Context, opt Options, staging string) (string, error) {
	candidates := make([]string, 0, len(opt.Passwords)+1)
	candidates = append(candidates, "")
	for _, p := range opt.Passwords {
		if p != "" {
			candidates = append(candidates, p)
		}
	}

	var matchedPwd string
	var preflightOK bool
	var lastErr error
	for _, pwd := range candidates {
		err := sevenZipPreflight(ctx, opt, pwd)
		if err == nil {
			matchedPwd = pwd
			preflightOK = true
			break
		}
		lastErr = err
		// Security / budget / memory limit errors should abort immediately, not try more passwords.
		if errors.Is(err, ErrSpecialFileRejected) || errors.Is(err, ErrUnsafePath) ||
			errors.Is(err, ErrDuplicateEntry) || errors.Is(err, ErrFileLimit) ||
			errors.Is(err, ErrSizeLimit) || errors.Is(err, ErrDictMemoryLimit) {
			return "", err
		}
	}
	if !preflightOK {
		if lastErr != nil {
			return "", lastErr
		}
		return "", ErrPasswordOrCorrupt
	}

	// Single-shot single-threaded extraction into isolated staging directory.
	pArg := "-p__no_password__"
	if matchedPwd != "" {
		pArg = "-p" + matchedPwd
	}
	args := []string{"x", "-mmt=1", "-y", "-spd", "-sccUTF-8", pArg, "-o" + staging, "--", opt.Source}
	cmd := exec.CommandContext(ctx, opt.Binary, args...)
	cmd.Stdin = nil
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", ErrPasswordOrCorrupt
	}
	return matchedPwd, nil
}

func sevenZipPreflight(ctx context.Context, opt Options, password string) error {
	pArg := "-p__no_password__"
	if password != "" {
		pArg = "-p" + password
	}
	probeCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(probeCtx, opt.Binary, "l", "-slt", "-ba", "-sccUTF-8", pArg, "--", opt.Source)
	cmd.Stdin = nil
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return ErrExtractorUnavailable
	}

	limited := io.LimitReader(stdout, maxMetadataBytes+1)
	raw, readErr := io.ReadAll(limited)
	waitErr := cmd.Wait()
	if readErr != nil || waitErr != nil {
		return ErrPasswordOrCorrupt
	}
	if len(raw) > maxMetadataBytes {
		return ErrSizeLimit
	}

	var totalBytes int64
	var entryCount int
	seen := make(map[string]bool)

	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	block := make(map[string]string)

	flush := func() error {
		if len(block) == 0 {
			return nil
		}
		defer func() { clear(block) }()

		if block["Symbolic Link"] != "" || block["Hard Link"] != "" || block["Reparse Point"] != "" {
			return ErrSpecialFileRejected
		}
		if mode := block["Mode"]; mode != "" && strings.ContainsAny(mode[:1], "lbcps") {
			return ErrSpecialFileRejected
		}
		for _, tok := range strings.Fields(block["Attributes"]) {
			if len(tok) > 5 && strings.ContainsAny(tok[:1], "lbcps") {
				return ErrSpecialFileRejected
			}
		}
		if opt.MaxDictMB > 0 {
			if dictMB := ParseMethodDictMB(block["Method"]); dictMB > opt.MaxDictMB {
				return fmt.Errorf("%w: %dMB > %dMB", ErrDictMemoryLimit, dictMB, opt.MaxDictMB)
			}
		}
		p, ok := block["Path"]
		if !ok || p == "" {
			return nil
		}
		rel, err := SafeRelative(p)
		if err != nil {
			return err
		}
		key := strings.ToLower(rel)
		if seen[key] {
			return ErrDuplicateEntry
		}
		seen[key] = true

		if szStr := block["Size"]; szStr != "" {
			sz, err := strconv.ParseInt(szStr, 10, 64)
			if err != nil || sz < 0 {
				return ErrPasswordOrCorrupt
			}
			totalBytes += sz
			if totalBytes > opt.MaxBytes {
				return ErrSizeLimit
			}
		}
		entryCount++
		if entryCount > opt.MaxFiles {
			return ErrFileLimit
		}
		return nil
	}

	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if idx := strings.Index(line, " = "); idx > 0 {
			block[line[:idx]] = line[idx+3:]
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if entryCount == 0 {
		return ErrEmptyArchive
	}
	return nil
}

var (
	lzmaPowRe  = regexp.MustCompile(`(?i)LZMA2?:(\d{1,2})(?:\b|$)`)
	lzmaSizeRe = regexp.MustCompile(`(?i)LZMA2?:(\d+)([kmg])`)
)

// ParseMethodDictMB estimates the decoder dictionary memory in MB from 7-Zip's Method string.
// Examples: "LZMA2:26" -> 64 MB, "LZMA2:128m" -> 128 MB, "LZMA2:24" -> 16 MB.
func ParseMethodDictMB(method string) int {
	if method == "" {
		return 0
	}
	maxMB := 0
	for _, m := range lzmaSizeRe.FindAllStringSubmatch(method, -1) {
		n, _ := strconv.Atoi(m[1])
		switch strings.ToLower(m[2]) {
		case "g":
			n *= 1024
		case "k":
			n = (n + 1023) / 1024
		}
		if n > maxMB {
			maxMB = n
		}
	}
	for _, m := range lzmaPowRe.FindAllStringSubmatch(method, -1) {
		pow, _ := strconv.Atoi(m[1])
		if pow >= 20 && pow <= 32 {
			mb := 1 << (pow - 20)
			if mb > maxMB {
				maxMB = mb
			}
		}
	}
	return maxMB
}

// Built-in pure-Go ZIP/TAR fallback when 7zz binary is not installed (also exercises full staging pipeline in unit tests).
func runBuiltinFallback(ctx context.Context, opt Options, staging string) (string, error) {
	lower := strings.ToLower(opt.Source)
	switch {
	case strings.HasSuffix(lower, ".zip"):
		return "builtin-zip", extractZipBuiltin(ctx, opt, staging)
	case strings.HasSuffix(lower, ".tar"):
		return "builtin-tar", extractTarBuiltin(ctx, opt, staging)
	default:
		return "", ErrExtractorUnavailable
	}
}

func extractZipBuiltin(ctx context.Context, opt Options, staging string) error {
	zr, err := zip.OpenReader(opt.Source)
	if err != nil {
		return ErrPasswordOrCorrupt
	}
	defer zr.Close()

	if len(zr.File) == 0 {
		return ErrEmptyArchive
	}
	if len(zr.File) > opt.MaxFiles {
		return ErrFileLimit
	}
	seen := make(map[string]bool)
	var total int64
	buf := make([]byte, 32*1024)

	for _, f := range zr.File {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if f.Mode()&os.ModeSymlink != 0 {
			return ErrSpecialFileRejected
		}
		rel, err := SafeRelative(f.Name)
		if err != nil {
			return err
		}
		key := strings.ToLower(rel)
		if seen[key] {
			return ErrDuplicateEntry
		}
		seen[key] = true

		target := filepath.Join(staging, filepath.FromSlash(rel))
		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return ErrPasswordOrCorrupt
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
		if err != nil {
			_ = rc.Close()
			return err
		}
		n, copyErr := io.CopyBuffer(out, io.LimitReader(rc, opt.MaxBytes-total+1), buf)
		_ = out.Close()
		_ = rc.Close()
		if copyErr != nil {
			return copyErr
		}
		total += n
		if total > opt.MaxBytes {
			return ErrSizeLimit
		}
	}
	return nil
}

func extractTarBuiltin(ctx context.Context, opt Options, staging string) error {
	f, err := os.Open(opt.Source)
	if err != nil {
		return err
	}
	defer f.Close()

	tr := tar.NewReader(f)
	seen := make(map[string]bool)
	var total int64
	count := 0
	buf := make([]byte, 32*1024)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return ErrPasswordOrCorrupt
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeDir {
			return ErrSpecialFileRejected
		}
		rel, err := SafeRelative(hdr.Name)
		if err != nil {
			return err
		}
		key := strings.ToLower(rel)
		if seen[key] {
			return ErrDuplicateEntry
		}
		seen[key] = true
		count++
		if count > opt.MaxFiles {
			return ErrFileLimit
		}
		target := filepath.Join(staging, filepath.FromSlash(rel))
		if hdr.Typeflag == tar.TypeDir {
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
		if err != nil {
			return err
		}
		n, copyErr := io.CopyBuffer(out, io.LimitReader(tr, opt.MaxBytes-total+1), buf)
		_ = out.Close()
		if copyErr != nil {
			return copyErr
		}
		total += n
		if total > opt.MaxBytes {
			return ErrSizeLimit
		}
	}
	if count == 0 {
		return ErrEmptyArchive
	}
	return nil
}

// SafeRelative validates and normalizes an archive entry path, rejecting traversal or absolute paths.
func SafeRelative(raw string) (string, error) {
	raw = strings.ReplaceAll(raw, "\\", "/")
	raw = strings.TrimSuffix(raw, "/")
	if raw == "" || strings.HasPrefix(raw, "/") || strings.ContainsRune(raw, 0) {
		return "", ErrUnsafePath
	}
	if len(raw) >= 2 && raw[1] == ':' {
		return "", ErrUnsafePath
	}
	parts := strings.Split(raw, "/")
	cleanParts := make([]string, 0, len(parts))
	for _, p := range parts {
		if p == "" || p == "." {
			continue
		}
		if p == ".." {
			return "", ErrUnsafePath
		}
		cleanParts = append(cleanParts, p)
	}
	if len(cleanParts) == 0 {
		return "", ErrUnsafePath
	}
	if len(cleanParts) > maxPathDepth {
		return "", ErrDepthLimit
	}
	return path.Join(cleanParts...), nil
}

// MapLongName shortens any single path component exceeding 240 UTF-8 bytes deterministically.
func MapLongName(part string) (string, bool) {
	if len(part) <= maxFilenameUTF8Bytes {
		return part, false
	}
	ext := filepath.Ext(part)
	if len(ext) > 32 {
		ext = ""
	}
	sum := sha256.Sum256([]byte(part + "\x00" + part))
	suffix := "~" + hex.EncodeToString(sum[:4]) + ext
	prefix := part
	for len(prefix)+len(suffix) > maxFilenameUTF8Bytes && len(prefix) > 0 {
		runes := []rune(prefix)
		prefix = string(runes[:len(runes)-1])
	}
	return prefix + suffix, true
}

func sanitizeAndMapStaging(staging string, maxFiles int, maxBytes int64) (map[string]string, error) {
	mapping := make(map[string]string)
	var files int
	var total int64

	type renameOp struct {
		oldPath string
		newPath string
		oldRel  string
		newRel  string
	}
	var renames []renameOp

	err := filepath.WalkDir(staging, func(p string, d os.DirEntry, err error) error {
		if err != nil || p == staging {
			return err
		}
		info, lErr := os.Lstat(p)
		if lErr != nil {
			return lErr
		}
		if info.Mode()&os.ModeSymlink != 0 || (!info.IsDir() && !info.Mode().IsRegular()) {
			return ErrSpecialFileRejected
		}
		if !info.IsDir() {
			files++
			total += info.Size()
			if files > maxFiles {
				return ErrFileLimit
			}
			if total > maxBytes {
				return ErrSizeLimit
			}
		}
		base := filepath.Base(p)
		if mapped, changed := MapLongName(base); changed {
			rel, _ := filepath.Rel(staging, p)
			newPath := filepath.Join(filepath.Dir(p), mapped)
			newRel, _ := filepath.Rel(staging, newPath)
			renames = append(renames, renameOp{
				oldPath: p, newPath: newPath,
				oldRel: filepath.ToSlash(rel), newRel: filepath.ToSlash(newRel),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Apply renames deepest-first so child paths remain valid.
	for i := len(renames) - 1; i >= 0; i-- {
		op := renames[i]
		if err := os.Rename(op.oldPath, op.newPath); err != nil {
			return nil, err
		}
		mapping[op.oldRel] = op.newRel
	}
	return mapping, nil
}

func squashSingleWrapper(staging string) error {
	for {
		entries, err := os.ReadDir(staging)
		if err != nil || len(entries) != 1 || !entries[0].IsDir() {
			return err
		}
		wrapper := filepath.Join(staging, entries[0].Name())
		tmp, err := os.MkdirTemp(filepath.Dir(staging), ".ark-squash-*")
		if err != nil {
			return err
		}
		moved := filepath.Join(tmp, "w")
		if err := os.Rename(wrapper, moved); err != nil {
			_ = os.RemoveAll(tmp)
			return err
		}
		children, err := os.ReadDir(moved)
		if err != nil {
			_ = os.RemoveAll(tmp)
			return err
		}
		for _, ch := range children {
			if err := os.Rename(filepath.Join(moved, ch.Name()), filepath.Join(staging, ch.Name())); err != nil {
				_ = os.RemoveAll(tmp)
				return err
			}
		}
		_ = os.RemoveAll(tmp)
	}
}

func collectManifest(root string, maxFiles int, maxBytes int64) ([]ExtractedFile, int64, error) {
	var files []ExtractedFile
	var total int64
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		total += info.Size()
		if total > maxBytes {
			return ErrSizeLimit
		}
		files = append(files, ExtractedFile{Path: filepath.ToSlash(rel), Size: info.Size()})
		if len(files) > maxFiles {
			return ErrFileLimit
		}
		return nil
	})
	return files, total, err
}

func uniqueDir(base string) string {
	if _, err := os.Stat(base); errors.Is(err, os.ErrNotExist) {
		return base
	}
	for i := 1; i <= 999; i++ {
		cand := fmt.Sprintf("%s-%03d", base, i)
		if _, err := os.Stat(cand); errors.Is(err, os.ErrNotExist) {
			return cand
		}
	}
	return fmt.Sprintf("%s-%d", base, time.Now().Unix())
}
