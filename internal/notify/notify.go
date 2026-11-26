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
	Enabled        bool   `json:"enabled"`
	BotToken       string `json:"bot_token"`
	ChatID         string `json:"chat_id"`
	APIBase        string `json:"api_base"`         // Optional reverse proxy, defaults to https://api.telegram.org
	ProxyURL       string `json:"proxy_url"`        // Optional http/https/socks5 proxy
	Mode           string `json:"mode"`             // result | errors | all
	BotMenuEnabled *bool  `json:"bot_menu_enabled"` // nil defaults to true when Telegram is enabled
}

func (c TelegramConfig) IsBotMenuEnabled() bool {
	if c.BotMenuEnabled == nil {
		return true
	}
	return *c.BotMenuEnabled
}

type InlineButton struct {
	Text         string `json:"text"`
	CallbackData string `json:"callback_data,omitempty"`
}

type InlineKeyboardMarkup struct {
	InlineKeyboard [][]InlineButton `json:"inline_keyboard"`
}

type WebhookConfig struct {
	Enabled      bool   `json:"enabled"`
	URL          string `json:"url"`
	SecretHeader string `json:"secret_header"`
	Mode         string `json:"mode"` // result | errors | all
}

type Message struct {
	EventID      int64  `json:"event_id"`
	PipelineID   string `json:"pipeline_id"`
	PipelineName string `json:"pipeline_name,omitempty"`
	Kind         string `json:"kind"`
	Stage        string `json:"stage"`
	Title        string `json:"title"`
	Body         string `json:"body"`
	IsError      bool   `json:"is_error"`
	Timestamp    int64  `json:"timestamp"`
}

func BuildHTTPClient(proxyURL string, timeout time.Duration) (*http.Client, error) {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
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
		Timeout:   timeout,
	}, nil
}

func buildHTTPClient(proxyURL string) (*http.Client, error) {
	return BuildHTTPClient(proxyURL, 20*time.Second)
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

	icon := eventIcon(msg.Kind, msg.IsError)
	ts := time.Unix(msg.Timestamp, 0).Format("01-02 15:04:05")

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("⛵ <b>方舟 · %s %s</b>\n", icon, html.EscapeString(msg.Title)))
	sb.WriteString("━━━━━━━━━━━━━━━\n")
	if name := strings.TrimSpace(msg.PipelineName); name != "" && !strings.Contains(msg.Body, name) {
		sb.WriteString("📄 <code>" + html.EscapeString(shortenRunes(name, 60)) + "</code>\n")
	}
	sb.WriteString("💬 " + html.EscapeString(msg.Body) + "\n\n")
	sb.WriteString(fmt.Sprintf("🏷 阶段：<code>%s</code>  ·  🕒 <code>%s</code>",
		html.EscapeString(zhStage(msg.Stage)), ts))
	text := sb.String()

	bodyObj := map[string]any{
		"chat_id":                  cfg.ChatID,
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	}
	if cfg.IsBotMenuEnabled() && msg.PipelineID != "" && msg.PipelineID != "system" {
		if msg.Kind == "WAITING_APPROVAL" {
			bodyObj["reply_markup"] = InlineKeyboardMarkup{
				InlineKeyboard: [][]InlineButton{
					{
						{Text: "🗜️ 转本地 7-Zip 解压", CallbackData: "loc:" + msg.PipelineID},
						{Text: "🔁 重试云解压", CallbackData: "rty:" + msg.PipelineID},
					},
					{
						{Text: "⏸️ 暂停任务", CallbackData: "can:" + msg.PipelineID},
						{Text: "🏠 打开主菜单", CallbackData: "menu"},
					},
				},
			}
		} else if msg.IsError {
			bodyObj["reply_markup"] = InlineKeyboardMarkup{
				InlineKeyboard: [][]InlineButton{
					{
						{Text: "🔁 立即重试", CallbackData: "rty:" + msg.PipelineID},
						{Text: "📋 查看失败列表", CallbackData: "tasks:failed"},
					},
				},
			}
		}
	}
	payload, _ := json.Marshal(bodyObj)
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

func eventTier(kind string) string {
	switch {
	case strings.Contains(kind, "FAILED") || kind == "SUBMISSION_UNKNOWN" || kind == "WAITING_APPROVAL" || kind == "ARCHIVE_MOVE_WARN":
		return "error"
	case kind == "SUCCEEDED" || kind == "OFFLINE_SUCCEEDED" || kind == "NO_ARCHIVE_FOUND" || kind == "SEED_CLEANUP_COMPLETED" || kind == "DATE_ORGANIZED" || kind == "TEST":
		return "result"
	default:
		return "progress"
	}
}

func shouldSend(mode, kind string) bool {
	tier := eventTier(kind)
	switch mode {
	case "errors":
		return tier == "error"
	case "all":
		return true
	case "result", "":
		return tier == "error" || tier == "result"
	default:
		return tier == "error" || tier == "result"
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
		Body:      "如果你看到这条消息，说明方舟的 Telegram 通知通道已配置成功！",
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
		if ev.PipelineID != "" && ev.PipelineID != "system" {
			if p, err := d.store.GetPipeline(ctx, ev.PipelineID); err == nil && strings.TrimSpace(p.Name) != "" {
				msg.PipelineName = p.Name
			}
		}
		var firstErr error
		attempted := false
		if tgCfg.Enabled && shouldSend(tgCfg.Mode, ev.Kind) {
			attempted = true
			if err := SendTelegram(ctx, tgCfg, msg); err != nil {
				firstErr = err
			}
		}
		if whCfg.Enabled && shouldSend(whCfg.Mode, ev.Kind) {
			attempted = true
			if err := SendWebhook(ctx, whCfg, msg); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if firstErr != nil {
			_ = d.store.MarkNotificationRetryOrFail(ctx, ev.ID, firstErr.Error())
		} else if attempted {
			_ = d.store.MarkNotificationSent(ctx, ev.ID)
		} else {
			_ = d.store.MarkNotificationSkipped(ctx, ev.ID)
		}
	}
	return nil
}

func toMessage(ev store.TaskEvent) Message {
	isErr := eventTier(ev.Kind) == "error"
	titleMap := map[string]string{
		"SUCCEEDED":              "解压完成",
		"OFFLINE_SUCCEEDED":      "离线完成",
		"SEED_CLEANUP_COMPLETED": "种子清理完成",
		"SEED_CLEANUP_FAILED":    "种子清理失败",
		"FAILED":                 "任务失败",
		"SUBMIT_FAILED":          "离线提交失败",
		"SUBMISSION_UNKNOWN":     "提交状态待核对",
		"WAITING_APPROVAL":       "等待确认本地兜底",
		"NO_ARCHIVE_FOUND":       "离线完成（无压缩包）",
		"DATE_ORGANIZED":         "日期目录整理完成",
		"CLOUD_EXTRACT_RETRIED":  "云解压自动重试",
		"BARK_WINDOW_FIRED":      "Bark 触发扫描",
		"BARK_TARGET_CHECKED":    "Bark 扫描完成",
		"BARK_PATH_FAILED":       "Bark 路径解析失败",
		"CLOUD_SOURCE_SCANNED":   "云监控目录扫描",
		"DISCOVERED":             "发现新压缩包",
		"SUBMITTED":              "离线已提交",
		"ARCHIVE_HANDOFF":        "转交云解压",
		"CLOUD_EXPORT_STARTED":   "开始 115 云解压",
		"AUTO_LOCAL_FALLBACK":    "自动切换本地解压",
		"LOCAL_FALLBACK_APPROVED": "已批准本地兜底",
		"LOCAL_EXTRACT_STARTED":  "开始本地解压",
		"ARCHIVE_MOVE_WARN":      "原包归档异常",
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

func eventIcon(kind string, isErr bool) string {
	switch kind {
	case "SUCCEEDED":
		return "🎉"
	case "OFFLINE_SUCCEEDED":
		return "✅"
	case "NO_ARCHIVE_FOUND":
		return "📂"
	case "SEED_CLEANUP_COMPLETED":
		return "🧹"
	case "DATE_ORGANIZED":
		return "🗂️"
	case "WAITING_APPROVAL", "ARCHIVE_MOVE_WARN":
		return "⚠️"
	case "SUBMISSION_UNKNOWN":
		return "🔍"
	case "BARK_WINDOW_FIRED":
		return "📡"
	case "BARK_TARGET_CHECKED":
		return "🔎"
	case "BARK_PATH_FAILED":
		return "🚨"
	case "CLOUD_SOURCE_SCANNED":
		return "☁️"
	case "DISCOVERED":
		return "📦"
	case "SUBMITTED":
		return "🚀"
	case "ARCHIVE_HANDOFF":
		return "📤"
	case "CLOUD_EXPORT_STARTED":
		return "⚡"
	case "CLOUD_EXTRACT_RETRIED":
		return "🔁"
	case "AUTO_LOCAL_FALLBACK", "LOCAL_FALLBACK_APPROVED":
		return "📥"
	case "LOCAL_EXTRACT_STARTED":
		return "🗜️"
	case "TEST":
		return "🔔"
	default:
		if isErr {
			return "❌"
		}
		return "ℹ️"
	}
}

func shortenRunes(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max-1]) + "…"
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
