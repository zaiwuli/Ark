package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/zaiwuli/Ark/internal/store"
)

type TelegramConfig struct {
	Enabled  bool   `json:"enabled"`
	BotToken string `json:"bot_token"`
	ChatID   string `json:"chat_id"`
	APIBase  string `json:"api_base"`  // Optional reverse proxy, defaults to https://api.telegram.org
	ProxyURL string `json:"proxy_url"` // Optional http/https/socks5 proxy
	Mode     string `json:"mode"`      // result | errors | all
}

type WebhookConfig struct {
	Enabled      bool   `json:"enabled"`
	URL          string `json:"url"`
	SecretHeader string `json:"secret_header"`
	Mode         string `json:"mode"` // result | errors | all
}

type Message struct {
	EventID    int64  `json:"event_id"`
	PipelineID string `json:"pipeline_id"`
	Kind       string `json:"kind"`
	Stage      string `json:"stage"`
	Title      string `json:"title"`
	Body       string `json:"body"`
	IsError    bool   `json:"is_error"`
	Timestamp  int64  `json:"timestamp"`
}

func buildHTTPClient(proxyURL string) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if strings.TrimSpace(proxyURL) != "" {
		u, err := url.Parse(strings.TrimSpace(proxyURL))
		if err != nil {
			return nil, fmt.Errorf("invalid proxy url: %w", err)
		}
		transport.Proxy = http.ProxyURL(u)
	}
	return &http.Client{
		Transport: transport,
		Timeout:   20 * time.Second,
	}, nil
}

func SendTelegram(ctx context.Context, cfg TelegramConfig, msg Message) error {
	if strings.TrimSpace(cfg.BotToken) == "" || strings.TrimSpace(cfg.ChatID) == "" {
		return errors.New("TELEGRAM_NOT_CONFIGURED")
	}
	base := strings.TrimRight(strings.TrimSpace(cfg.APIBase), "/")
	if base == "" {
		base = "https://api.telegram.org"
	}
	client, err := buildHTTPClient(cfg.ProxyURL)
	if err != nil {
		return err
	}

	icon := "ℹ️"
	if msg.IsError {
		icon = "❌"
	} else if strings.Contains(msg.Kind, "SUCCEEDED") || strings.Contains(msg.Kind, "COMPLETED") {
		icon = "✅"
	} else if msg.Kind == "WAITING_APPROVAL" {
		icon = "⚠️"
	} else if msg.Kind == "TEST" {
		icon = "🔔"
	}

	ts := time.Unix(msg.Timestamp, 0).Format("2006-01-02 15:04:05")
	text := fmt.Sprintf(
		"%s <b>Ark 方舟 · %s</b>\n%s\n<code>阶段: %s · %s</code>",
		icon,
		html.EscapeString(msg.Title),
		html.EscapeString(msg.Body),
		html.EscapeString(zhStage(msg.Stage)),
		ts,
	)

	payload, _ := json.Marshal(map[string]any{
		"chat_id":                  cfg.ChatID,
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	})
	endpoint := fmt.Sprintf("%s/bot%s/sendMessage", base, cfg.BotToken)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		// Never return raw URL containing bot token.
		return errors.New("TELEGRAM_REQUEST_FAILED")
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("TELEGRAM_HTTP_%d", resp.StatusCode)
	}
	return nil
}

func SendWebhook(ctx context.Context, cfg WebhookConfig, msg Message) error {
	if strings.TrimSpace(cfg.URL) == "" {
		return errors.New("WEBHOOK_NOT_CONFIGURED")
	}
	payload, _ := json.Marshal(msg)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.SecretHeader != "" {
		req.Header.Set("X-Ark-Secret", cfg.SecretHeader)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("WEBHOOK_REQUEST_FAILED")
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("WEBHOOK_HTTP_%d", resp.StatusCode)
	}
	return nil
}

func shouldSend(mode string, isError bool) bool {
	switch mode {
	case "errors":
		return isError
	case "all", "result", "":
		return true
	default:
		return true
	}
}

type Dispatcher struct {
	store *store.Store
}

func NewDispatcher(st *store.Store) *Dispatcher {
	return &Dispatcher{store: st}
}

func (d *Dispatcher) LoadTelegram(ctx context.Context) TelegramConfig {
	var cfg TelegramConfig
	_, _ = d.store.GetSetting(ctx, "notify.telegram", &cfg)
	if cfg.Mode == "" {
		cfg.Mode = "result"
	}
	return cfg
}

func (d *Dispatcher) LoadWebhook(ctx context.Context) WebhookConfig {
	var cfg WebhookConfig
	_, _ = d.store.GetSetting(ctx, "notify.webhook", &cfg)
	if cfg.Mode == "" {
		cfg.Mode = "result"
	}
	return cfg
}

func (d *Dispatcher) SendTelegramTest(ctx context.Context) error {
	cfg := d.LoadTelegram(ctx)
	return SendTelegram(ctx, cfg, Message{
		Kind:      "TEST",
		Stage:     "test",
		Title:     "Telegram 通知测试",
		Body:      "如果你看到这条消息，说明 Ark 方舟的 Telegram 通知通道已配置成功！",
		Timestamp: time.Now().Unix(),
	})
}

func (d *Dispatcher) Flush(ctx context.Context) error {
	events, err := d.store.ListPendingNotifications(ctx, time.Now().Unix(), 20)
	if err != nil || len(events) == 0 {
		return err
	}
	tgCfg := d.LoadTelegram(ctx)
	whCfg := d.LoadWebhook(ctx)

	for _, ev := range events {
		msg := toMessage(ev)
		var firstErr error
		if tgCfg.Enabled && shouldSend(tgCfg.Mode, msg.IsError) {
			if err := SendTelegram(ctx, tgCfg, msg); err != nil {
				firstErr = err
			}
		}
		if whCfg.Enabled && shouldSend(whCfg.Mode, msg.IsError) {
			if err := SendWebhook(ctx, whCfg, msg); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if firstErr != nil {
			_ = d.store.MarkNotificationRetryOrFail(ctx, ev.ID, firstErr.Error())
		} else {
			_ = d.store.MarkNotificationSent(ctx, ev.ID)
		}
	}
	return nil
}

func toMessage(ev store.TaskEvent) Message {
	isErr := strings.Contains(ev.Kind, "FAILED") || ev.Kind == "SUBMISSION_UNKNOWN" || ev.Kind == "WAITING_APPROVAL"
	titleMap := map[string]string{
		"SUCCEEDED":             "解压完成",
		"OFFLINE_SUCCEEDED":     "离线完成",
		"SEED_CLEANUP_COMPLETED": "种子清理完成",
		"SEED_CLEANUP_FAILED":   "种子清理失败",
		"FAILED":                "任务失败",
		"SUBMIT_FAILED":         "离线提交失败",
		"SUBMISSION_UNKNOWN":    "提交状态待核对",
		"WAITING_APPROVAL":       "等待确认本地兜底",
		"NO_ARCHIVE_FOUND":       "离线完成（无压缩包）",
		"DATE_ORGANIZED":         "日期目录整理完成",
		"CLOUD_EXTRACT_RETRIED":  "云解压自动重试",
	}
	title := titleMap[ev.Kind]
	if title == "" {
		title = ev.Kind
	}
	return Message{
		EventID:    ev.ID,
		PipelineID: ev.PipelineID,
		Kind:       ev.Kind,
		Stage:      ev.Stage,
		Title:      title,
		Body:       ev.Message,
		IsError:    isErr,
		Timestamp:  ev.CreatedAt,
	}
}

func zhStage(stage string) string {
	m := map[string]string{
		"offline_submit": "离线提交",
		"offline_poll":   "离线轮询",
		"seed_cleanup":   "种子清理",
		"discover":       "压缩包发现",
		"cloud_prepare":  "云解压预检",
		"cloud_inspect":  "云解压分析",
		"cloud_export":   "云解压导出",
		"cloud_progress": "云解压轮询",
		"local_fallback": "本地兜底确认",
		"cache":          "直链缓存",
		"local_extract":  "本地解压",
		"cloud_monitor":  "云端监测",
		"bark":           "Bark 联动",
		"organize":       "目录整理",
		"complete":       "处理完成",
		"test":           "连通测试",
	}
	if v, ok := m[stage]; ok {
		return v
	}
	return stage
}
