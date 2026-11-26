package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zaiwuli/Ark/internal/api"
	"github.com/zaiwuli/Ark/internal/bark"
	"github.com/zaiwuli/Ark/internal/config"
	"github.com/zaiwuli/Ark/internal/metrics"
	"github.com/zaiwuli/Ark/internal/notify"
	"github.com/zaiwuli/Ark/internal/pipeline"
	"github.com/zaiwuli/Ark/internal/store"
	"github.com/zaiwuli/Ark/internal/tgbot"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--healthcheck" {
		os.Exit(runHealthcheck())
	}
	mc := metrics.NewCollector()
	mc.MarkStartup("boot")

	cfg := config.Load()
	cfg.ApplyRuntimeTuning()

	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	if err := cfg.EnsureDirs(); err != nil {
		logger.Error("failed to initialize directories", "err", err)
		os.Exit(1)
	}
	mc.MarkStartup("config_ready")

	st, err := store.Open(cfg.DBPath(), cfg.SecretKeyPath(), cfg.SecretKeyHex)
	if err != nil {
		logger.Error("failed to open sqlite store", "err", err)
		os.Exit(1)
	}
	defer func() { _ = st.Close() }()
	mc.MarkStartup("db_ready")

	pipeSvc := pipeline.NewServiceWithOptions(st, pipeline.ServiceOptions{
		Timezone:         cfg.Timezone,
		DataDir:          cfg.DataDir,
		WorkDir:          cfg.WorkDir,
		ExtractBin:       cfg.ExtractBin,
		ExtractMaxDictMB: cfg.ExtractMaxDictMB,
		StableWindow:     15 * time.Second,
	})
	notifier := notify.NewDispatcher(st)
	barkRecv := bark.NewReceiver(st, bark.DefaultWindowDuration, func(ctx context.Context, targets []bark.Target) {
		pts := make([]pipeline.BarkTarget, 0, len(targets))
		for _, t := range targets {
			pts = append(pts, pipeline.BarkTarget{
				SourceID:     t.SourceID,
				ReportedPath: t.ReportedPath,
				RelativePath: t.RelativePath,
			})
		}
		if _, err := pipeSvc.HandleBarkTrigger(ctx, pts); err != nil {
			logger.Warn("bark-triggered cloud check failed", "err", err)
		}
	})
	defer barkRecv.Stop()

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go pipeSvc.RunLoop(runCtx, 5*time.Second)
	tgBot := tgbot.New(st, pipeSvc, notifier, mc, cfg.DBPath())
	go tgBot.Run(runCtx)
	mc.MarkStartup("scheduler_ready")

	srv := api.NewServer(cfg, st, mc, pipeSvc, notifier, barkRecv)
	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	mc.MarkStartup("http_ready")

	errCh := make(chan error, 1)
	go func() {
		logger.Info("Ark server listening",
			"addr", cfg.ListenAddr,
			"version", config.Version,
			"mem_limit_mb", cfg.MemLimitMB,
			"data_dir", cfg.DataDir,
			"work_dir", cfg.WorkDir,
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("received shutdown signal", "signal", sig.String())
	case err := <-errCh:
		logger.Error("http server stopped with error", "err", err)
		os.Exit(1)
	}

	runCancel()
	barkRecv.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
		os.Exit(1)
	}
	logger.Info("Ark stopped cleanly")
}

func runHealthcheck() int {
	cfg := config.Load()
	addr := cfg.ListenAddr
	if addr == "" || addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	client := &http.Client{Timeout: 4 * time.Second}
	resp, err := client.Get("http://" + addr + "/health/ready")
	if err != nil {
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return 0
	}
	return 1
}

func newLogger(level string) *slog.Logger {
	lvl := slog.LevelInfo
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	}
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
}
