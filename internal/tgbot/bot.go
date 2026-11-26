package tgbot

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/zaiwuli/Ark/internal/links"
	"github.com/zaiwuli/Ark/internal/metrics"
	"github.com/zaiwuli/Ark/internal/notify"
	"github.com/zaiwuli/Ark/internal/pipeline"
	"github.com/zaiwuli/Ark/internal/store"
	"github.com/zaiwuli/Ark/internal/torrent"
)

// Bot provides an ultra-low-memory (<1 MB) interactive Telegram menu & command controller
// strictly restricted to the configured ChatID, reusing the user's custom APIBase & ProxyURL.
type Bot struct {
	store    *store.Store
	pipeline *pipeline.Service
	notifier *notify.Dispatcher
	metrics  *metrics.Collector
	dbPath   string

	lastMenuSig string
	offset      int64
}

func New(st *store.Store, pipe *pipeline.Service, notifier *notify.Dispatcher, mc *metrics.Collector, dbPath string) *Bot {
	return &Bot{
		store:    st,
		pipeline: pipe,
		notifier: notifier,
		metrics:  mc,
		dbPath:   dbPath,
	}
}

type tgUpdate struct {
	UpdateID      int64            `json:"update_id"`
	Message       *tgMessage       `json:"message"`
	CallbackQuery *tgCallbackQuery `json:"callback_query"`
}

type tgChat struct {
	ID int64 `json:"id"`
}

type tgDocument struct {
	FileID   string `json:"file_id"`
	FileName string `json:"file_name"`
	FileSize int64  `json:"file_size"`
}

type tgMessage struct {
	MessageID int64       `json:"message_id"`
	Date      int64       `json:"date"`
	Chat      tgChat      `json:"chat"`
	Text      string      `json:"text"`
	Caption   string      `json:"caption"`
	Document  *tgDocument `json:"document"`
}

type tgCallbackQuery struct {
	ID      string     `json:"id"`
	Message *tgMessage `json:"message"`
	Data    string     `json:"data"`
}

// Run starts the single-goroutine long-polling loop until ctx is cancelled.
func (b *Bot) Run(ctx context.Context) {
	backoff := 5 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		cfg := b.notifier.LoadTelegram(ctx)
		if !cfg.Enabled || !cfg.IsBotMenuEnabled() || strings.TrimSpace(cfg.BotToken) == "" || strings.TrimSpace(cfg.ChatID) == "" {
			if !sleepCtx(ctx, 5*time.Second) {
				return
			}
			continue
		}

		sig := apiBase(cfg) + "|" + cfg.BotToken
		if b.lastMenuSig != sig {
			if err := b.registerCommands(ctx, cfg); err == nil {
				b.lastMenuSig = sig
			}
		}

		updates, err := b.getUpdates(ctx, cfg, b.offset, 25)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Warn("telegram bot poll error", "err", err.Error())
			if !sleepCtx(ctx, backoff) {
				return
			}
			if backoff < 30*time.Second {
				backoff += 5 * time.Second
			}
			continue
		}
		backoff = 5 * time.Second

		nowUnix := time.Now().Unix()
		for _, upd := range updates {
			if upd.UpdateID >= b.offset {
				b.offset = upd.UpdateID + 1
			}
			// Skip stale messages older than 5 minutes after restart.
			if upd.Message != nil && upd.Message.Date > 0 && nowUnix-upd.Message.Date > 300 {
				continue
			}
			b.ProcessUpdate(ctx, cfg, upd)
		}
	}
}

func (b *Bot) registerCommands(ctx context.Context, cfg notify.TelegramConfig) error {
	cmds := []map[string]string{
		{"command": "menu", "description": "打开方舟主控台"},
		{"command": "status", "description": "查看运行状态与任务"},
		{"command": "tasks", "description": "查看最近任务列表"},
		{"command": "scan", "description": "手动触发目录扫描"},
		{"command": "clean", "description": "整理昨日目录与缓存维护"},
		{"command": "settings", "description": "切换解压规则与通知范围"},
		{"command": "pwd", "description": "追加解压密码（/pwd 密码）"},
	}
	_, err := b.callAPI(ctx, cfg, "setMyCommands", map[string]any{"commands": cmds}, 15*time.Second)
	return err
}

func (b *Bot) getUpdates(ctx context.Context, cfg notify.TelegramConfig, offset int64, timeoutSec int) ([]tgUpdate, error) {
	payload := map[string]any{
		"offset":          offset,
		"timeout":         timeoutSec,
		"limit":           20,
		"allowed_updates": []string{"message", "callback_query"},
	}
	raw, err := b.callAPI(ctx, cfg, "getUpdates", payload, time.Duration(timeoutSec+15)*time.Second)
	if err != nil {
		return nil, err
	}
	var resp struct {
		OK     bool       `json:"ok"`
		Result []tgUpdate `json:"result"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, errors.New("TELEGRAM_INVALID_UPDATES")
	}
	return resp.Result, nil
}

// ProcessUpdate handles a single Telegram message or inline callback query.
// Strictly ignores any chat other than cfg.ChatID.
func (b *Bot) ProcessUpdate(ctx context.Context, cfg notify.TelegramConfig, upd tgUpdate) {
	allowedChat := strings.TrimSpace(cfg.ChatID)
	if allowedChat == "" {
		return
	}
	if upd.CallbackQuery != nil {
		cb := upd.CallbackQuery
		if cb.Message == nil || strconv.FormatInt(cb.Message.Chat.ID, 10) != allowedChat {
			return
		}
		b.handleCallback(ctx, cfg, cb)
		return
	}
	if upd.Message != nil {
		msg := upd.Message
		if strconv.FormatInt(msg.Chat.ID, 10) != allowedChat {
			return
		}
		b.handleMessage(ctx, cfg, msg)
	}
}

func (b *Bot) handleMessage(ctx context.Context, cfg notify.TelegramConfig, msg *tgMessage) {
	// 1. Direct .torrent or .txt document upload -> parse locally and queue 115 offline download.
	if msg.Document != nil {
		lowerName := strings.ToLower(strings.TrimSpace(msg.Document.FileName))
		switch {
		case strings.HasSuffix(lowerName, ".torrent"):
			b.handleTorrentDoc(ctx, cfg, msg.Chat.ID, msg.Document)
			return
		case strings.HasSuffix(lowerName, ".txt"):
			b.handleTextDoc(ctx, cfg, msg.Chat.ID, msg.Document)
			return
		}
	}

	text := strings.TrimSpace(msg.Text)
	if text == "" {
		text = strings.TrimSpace(msg.Caption)
	}
	if text == "" {
		return
	}

	// 2. Slash commands
	if strings.HasPrefix(text, "/") {
		fields := strings.Fields(text)
		cmd := strings.ToLower(fields[0])
		if at := strings.IndexByte(cmd, '@'); at > 0 {
			cmd = cmd[:at]
		}
		arg := strings.TrimSpace(strings.TrimPrefix(text, fields[0]))

		switch cmd {
		case "/start", "/help", "/menu":
			txt, kb := b.renderMainMenu(ctx)
			_ = b.sendMessage(ctx, cfg, msg.Chat.ID, txt, kb)
		case "/status":
			txt, kb := b.renderStatus(ctx)
			_ = b.sendMessage(ctx, cfg, msg.Chat.ID, txt, kb)
		case "/tasks":
			filter := "all"
			if arg == "active" || arg == "attention" || arg == "failed" || arg == "completed" {
				filter = arg
			}
			txt, kb := b.renderTasks(ctx, filter)
			_ = b.sendMessage(ctx, cfg, msg.Chat.ID, txt, kb)
		case "/scan":
			txt, kb := b.renderScanMenu(ctx)
			_ = b.sendMessage(ctx, cfg, msg.Chat.ID, txt, kb)
		case "/clean":
			txt, kb := b.renderCleanMenu(ctx)
			_ = b.sendMessage(ctx, cfg, msg.Chat.ID, txt, kb)
		case "/pause":
			_ = b.pipeline.SetPaused(ctx, true)
			kb := &notify.InlineKeyboardMarkup{InlineKeyboard: [][]notify.InlineButton{
				{{Text: "▶️ 恢复调度", CallbackData: "sched:toggle"}, {Text: "返回主菜单", CallbackData: "menu"}},
			}}
			_ = b.sendMessage(ctx, cfg, msg.Chat.ID, "⏸️ <b>后台任务调度已暂停</b>\n新任务暂不会自动执行。", kb)
		case "/resume":
			_ = b.pipeline.SetPaused(ctx, false)
			kb := &notify.InlineKeyboardMarkup{InlineKeyboard: [][]notify.InlineButton{
				{{Text: "⏸️ 暂停调度", CallbackData: "sched:toggle"}, {Text: "返回主菜单", CallbackData: "menu"}},
			}}
			_ = b.sendMessage(ctx, cfg, msg.Chat.ID, "▶️ <b>后台任务调度已恢复</b>\n队列中的任务将继续自动执行。", kb)
		case "/pwd":
			b.handlePwdCommand(ctx, cfg, msg.Chat.ID, arg)
		case "/rules", "/settings":
			txt, kb := b.renderRules(ctx)
			_ = b.sendMessage(ctx, cfg, msg.Chat.ID, txt, kb)
		case "/retry":
			n, err := b.store.RetryAllFailed(ctx)
			if err != nil {
				_ = b.sendMessage(ctx, cfg, msg.Chat.ID, "❌ 批量重试失败，请稍后再试。", nil)
				return
			}
			kb := &notify.InlineKeyboardMarkup{InlineKeyboard: [][]notify.InlineButton{
				{{Text: "📋 查看任务", CallbackData: "tasks:all"}, {Text: "返回主菜单", CallbackData: "menu"}},
			}}
			_ = b.sendMessage(ctx, cfg, msg.Chat.ID, fmt.Sprintf("🔁 已重新入队 <b>%d</b> 条失败任务。", n), kb)
		default:
			txt, kb := b.renderMainMenu(ctx)
			_ = b.sendMessage(ctx, cfg, msg.Chat.ID, "未识别该指令，已为你打开主控台：\n\n"+txt, kb)
		}
		return
	}

	// 3. Direct link drop (magnet / ed2k / http / ftp) -> auto submit 115 offline download!
	parsed, _, err := links.Extract(text)
	if err == nil && len(parsed) > 0 {
		res, impErr := b.pipeline.ImportText(ctx, "tgbot", "", text, true)
		if impErr != nil {
			_ = b.sendMessage(ctx, cfg, msg.Chat.ID, "❌ 离线提交失败："+html.EscapeString(zhDomainErr(impErr)), nil)
			return
		}
		kb := &notify.InlineKeyboardMarkup{InlineKeyboard: [][]notify.InlineButton{
			{{Text: "📋 查看任务", CallbackData: "tasks:all"}, {Text: "🏠 主控台", CallbackData: "menu"}},
		}}
		out := fmt.Sprintf("✅ <b>离线链接已接收</b>\n• 新入队：<b>%d</b> 条\n• 重复忽略：%d 条\n• 无效跳过：%d 条\n<code>完成后自动发现并按日期云解压</code>",
			res.Queued, res.Duplicates, res.Invalid)
		_ = b.sendMessage(ctx, cfg, msg.Chat.ID, out, kb)
		return
	}

	// Friendly hint for other plain messages
	kb := &notify.InlineKeyboardMarkup{InlineKeyboard: [][]notify.InlineButton{
		{{Text: "🏠 打开方舟主控台", CallbackData: "menu"}},
	}}
	_ = b.sendMessage(ctx, cfg, msg.Chat.ID, "💡 直接发送<b>磁链 / 电驴 / HTTP 链接</b>、<b>.txt 链接文件</b>或 <b>.torrent 种子文件</b>即可离线下载；或点击下方打开主控台。", kb)
}

func (b *Bot) handleTextDoc(ctx context.Context, cfg notify.TelegramConfig, chatID int64, doc *tgDocument) {
	if doc.FileSize > torrent.MaxTorrentBytes {
		_ = b.sendMessage(ctx, cfg, chatID, "❌ 文本文件超过 2 MB 上限，无法处理。", nil)
		return
	}
	raw, err := b.downloadFile(ctx, cfg, doc.FileID)
	if err != nil {
		_ = b.sendMessage(ctx, cfg, chatID, "❌ 下载文本文件失败，请检查反代/代理连接。", nil)
		return
	}
	content := strings.TrimPrefix(string(raw), "\xef\xbb\xbf")
	parsed, _, parseErr := links.Extract(content)
	if parseErr != nil || len(parsed) == 0 {
		_ = b.sendMessage(ctx, cfg, chatID, fmt.Sprintf("❌ 未在 <code>%s</code> 中识别到可用的磁链 / 电驴 / HTTP 链接。", html.EscapeString(doc.FileName)), nil)
		return
	}
	res, err := b.pipeline.ImportText(ctx, "tgbot", "", content, true)
	if err != nil {
		_ = b.sendMessage(ctx, cfg, chatID, "❌ 文本离线提交失败："+html.EscapeString(zhDomainErr(err)), nil)
		return
	}
	kb := &notify.InlineKeyboardMarkup{InlineKeyboard: [][]notify.InlineButton{
		{{Text: "📋 查看任务", CallbackData: "tasks:all"}, {Text: "🏠 主控台", CallbackData: "menu"}},
	}}
	out := fmt.Sprintf("✅ <b>文本文件已解析并提交离线</b>\n文件：<code>%s</code>\n• 新入队：<b>%d</b> 条\n• 重复忽略：%d 条\n• 无效跳过：%d 条",
		html.EscapeString(doc.FileName), res.Queued, res.Duplicates, res.Invalid)
	_ = b.sendMessage(ctx, cfg, chatID, out, kb)
}

func (b *Bot) handleTorrentDoc(ctx context.Context, cfg notify.TelegramConfig, chatID int64, doc *tgDocument) {
	if doc.FileSize > torrent.MaxTorrentBytes {
		_ = b.sendMessage(ctx, cfg, chatID, "❌ 种子文件超过 2 MB 上限，无法处理。", nil)
		return
	}
	raw, err := b.downloadFile(ctx, cfg, doc.FileID)
	if err != nil {
		_ = b.sendMessage(ctx, cfg, chatID, "❌ 下载种子文件失败，请检查反代/代理连接。", nil)
		return
	}
	res, err := b.pipeline.ImportTorrent(ctx, "tgbot", "", raw, true)
	if err != nil {
		_ = b.sendMessage(ctx, cfg, chatID, "❌ 种子离线提交失败："+html.EscapeString(zhDomainErr(err)), nil)
		return
	}
	kb := &notify.InlineKeyboardMarkup{InlineKeyboard: [][]notify.InlineButton{
		{{Text: "📋 查看任务", CallbackData: "tasks:all"}, {Text: "🏠 主控台", CallbackData: "menu"}},
	}}
	out := fmt.Sprintf("✅ <b>种子已解析并提交离线</b>\n文件：<code>%s</code>\n• 新入队：<b>%d</b> 条 · 重复：%d 条",
		html.EscapeString(doc.FileName), res.Queued, res.Duplicates)
	_ = b.sendMessage(ctx, cfg, chatID, out, kb)
}

func (b *Bot) handlePwdCommand(ctx context.Context, cfg notify.TelegramConfig, chatID int64, arg string) {
	existing := b.pipeline.ArchivePasswords(ctx)
	if arg == "" {
		_ = b.sendMessage(ctx, cfg, chatID,
			fmt.Sprintf("🔐 当前密码本共有 <b>%d</b> 条加密保存的解压密码。\n\n追加用法：<code>/pwd 新密码</code>（支持空格或换行一次添加多个）", len(existing)), nil)
		return
	}
	parts := strings.Fields(arg)
	merged := append(append([]string(nil), existing...), parts...)
	if err := b.pipeline.SaveArchivePasswords(ctx, merged); err != nil {
		_ = b.sendMessage(ctx, cfg, chatID, "❌ 保存密码失败，请稍后重试。", nil)
		return
	}
	after := b.pipeline.ArchivePasswords(ctx)
	added := len(after) - len(existing)
	_ = b.sendMessage(ctx, cfg, chatID,
		fmt.Sprintf("🔐 密码本已更新：新增 <b>%d</b> 条，当前共 <b>%d</b> 条（本地加密保存）。", added, len(after)), nil)
}

func (b *Bot) handleCallback(ctx context.Context, cfg notify.TelegramConfig, cb *tgCallbackQuery) {
	data := cb.Data
	chatID := cb.Message.Chat.ID
	msgID := cb.Message.MessageID

	switch {
	case data == "menu":
		_ = b.answerCallback(ctx, cfg, cb.ID, "")
		txt, kb := b.renderMainMenu(ctx)
		_ = b.editMessage(ctx, cfg, chatID, msgID, txt, kb)

	case data == "status":
		_ = b.answerCallback(ctx, cfg, cb.ID, "已刷新")
		txt, kb := b.renderStatus(ctx)
		_ = b.editMessage(ctx, cfg, chatID, msgID, txt, kb)

	case strings.HasPrefix(data, "tasks:"):
		filter := strings.TrimPrefix(data, "tasks:")
		_ = b.answerCallback(ctx, cfg, cb.ID, "")
		txt, kb := b.renderTasks(ctx, filter)
		_ = b.editMessage(ctx, cfg, chatID, msgID, txt, kb)

	case data == "scan":
		_ = b.answerCallback(ctx, cfg, cb.ID, "")
		txt, kb := b.renderScanMenu(ctx)
		_ = b.editMessage(ctx, cfg, chatID, msgID, txt, kb)

	case data == "confirm_scan:deep":
		_ = b.answerCallback(ctx, cfg, cb.ID, "")
		depth := b.pipeline.P115Config().ScanDepth()
		msg := fmt.Sprintf("⚠️ <b>确认按深度扫描 115 云目录？</b>\n\n当前配置遍历深度：<b>%d 层</b>（可在网页设置 1~5 层）。\n将扫描所有已启用的 115 云监控目录并接管未处理过的压缩包；如只想扫今天的新包，请点「仅扫今日」。", depth)
		kb := &notify.InlineKeyboardMarkup{InlineKeyboard: [][]notify.InlineButton{
			{{Text: "✅ 确认按深度扫描", CallbackData: "do_scan:deep"}, {Text: "☁️ 改用仅扫今日", CallbackData: "do_scan:today"}},
			{{Text: "🔙 返回扫描菜单", CallbackData: "scan"}},
		}}
		_ = b.editMessage(ctx, cfg, chatID, msgID, msg, kb)

	case strings.HasPrefix(data, "do_scan:"):
		target := strings.TrimPrefix(data, "do_scan:")
		_ = b.answerCallback(ctx, cfg, cb.ID, "正在扫描...")
		var msg string
		switch target {
		case "local":
			n, err := b.pipeline.ScanLocalNow(ctx)
			if err != nil {
				msg = "❌ 本地目录扫描失败：" + html.EscapeString(err.Error())
			} else {
				msg = fmt.Sprintf("✅ 本地监控目录扫描完成，新接管 <b>%d</b> 个稳定压缩包。", n)
			}
		case "today", "cloud":
			n, err := b.pipeline.ScanCloudSourcesWithOptions(ctx, nil, true, 0)
			if err != nil {
				msg = "❌ 115 云目录今日扫描失败：" + html.EscapeString(zhDomainErr(err))
			} else {
				msg = fmt.Sprintf("✅ 115 云目录（仅今日模式）扫描完成，新接管 <b>%d</b> 个压缩包。", n)
			}
		case "deep":
			depth := b.pipeline.P115Config().ScanDepth()
			n, err := b.pipeline.ScanCloudSourcesWithOptions(ctx, nil, false, depth)
			if err != nil {
				msg = "❌ 115 云目录深度扫描失败：" + html.EscapeString(zhDomainErr(err))
			} else {
				msg = fmt.Sprintf("✅ 115 云目录（深度 %d 层）扫描完成，新接管 <b>%d</b> 个压缩包。", depth, n)
			}
		case "life":
			n, err := b.pipeline.TriggerLifeCheck(ctx)
			if err != nil {
				msg = "❌ 生活事件检查失败：" + html.EscapeString(zhDomainErr(err))
			} else {
				msg = fmt.Sprintf("✅ 生活事件增量检查完成，发现 <b>%d</b> 个压缩包。", n)
			}
		}
		kb := &notify.InlineKeyboardMarkup{InlineKeyboard: [][]notify.InlineButton{
			{{Text: "📋 查看任务", CallbackData: "tasks:all"}, {Text: "🔍 返回扫描", CallbackData: "scan"}},
			{{Text: "🏠 主控台", CallbackData: "menu"}},
		}}
		_ = b.editMessage(ctx, cfg, chatID, msgID, msg, kb)

	case data == "clean":
		_ = b.answerCallback(ctx, cfg, cb.ID, "")
		txt, kb := b.renderCleanMenu(ctx)
		_ = b.editMessage(ctx, cfg, chatID, msgID, txt, kb)

	case data == "do_clean:cache":
		_ = b.answerCallback(ctx, cfg, cb.ID, "正在回收缓存...")
		res, err := b.pipeline.ReclaimCache(ctx)
		msg := "❌ 缓存回收失败"
		if err == nil {
			msg = fmt.Sprintf("🧹 <b>本地缓存回收完成</b>\n• 释放空间：<b>%s</b>\n• 清理文件/目录：%d 项\n• 跳过活跃任务：%d 个",
				fmtBytes(res.FreedBytes), res.RemovedItems, res.SkippedActive)
		}
		kb := &notify.InlineKeyboardMarkup{InlineKeyboard: [][]notify.InlineButton{
			{{Text: "返回空间维护", CallbackData: "clean"}, {Text: "🏠 主控台", CallbackData: "menu"}},
		}}
		_ = b.editMessage(ctx, cfg, chatID, msgID, msg, kb)

	case data == "do_clean:org_preview":
		_ = b.answerCallback(ctx, cfg, cb.ID, "正在扫描昨日及以前目录...")
		plan, err := b.pipeline.PreviewOrganizeWithMode(ctx, true)
		if err != nil {
			_ = b.editMessage(ctx, cfg, chatID, msgID, "❌ 扫描预览失败："+html.EscapeString(err.Error()), backHomeKB())
			return
		}
		var sb strings.Builder
		sb.WriteString("🗂 <b>手动整理预览（昨日及以前）</b>\n")
		sb.WriteString(fmt.Sprintf("• 待折叠归档：<b>%d</b> 个旧日期目录\n• 待清理空壳：<b>%d</b> 个\n", plan.MoveCount, plan.EmptyCount))
		if plan.SkippedActive > 0 {
			sb.WriteString(fmt.Sprintf("• 自动避让活跃任务：%d 个\n", plan.SkippedActive))
		}
		if len(plan.Actions) > 0 {
			sb.WriteString("\n示例条目：\n")
			limit := len(plan.Actions)
			if limit > 6 {
				limit = 6
			}
			for _, a := range plan.Actions[:limit] {
				scope := "本地"
				if a.Scope == "cloud" {
					scope = "115"
				}
				act := "折叠"
				if a.Action == "delete_empty" {
					act = "清空气壳"
				}
				sb.WriteString(fmt.Sprintf("· [%s] %s/%s → %s\n", scope, html.EscapeString(a.Category), a.Date, act))
			}
			if len(plan.Actions) > limit {
				sb.WriteString(fmt.Sprintf("…共 %d 项\n", len(plan.Actions)))
			}
		} else {
			sb.WriteString("\n当前没有昨日及以前的旧日期目录需要整理。")
		}
		rows := [][]notify.InlineButton{}
		if len(plan.Actions) > 0 {
			rows = append(rows, []notify.InlineButton{{Text: "✅ 确认执行整理", CallbackData: "do_clean:org_run"}})
		}
		rows = append(rows, []notify.InlineButton{{Text: "返回维护", CallbackData: "clean"}, {Text: "🏠 主控台", CallbackData: "menu"}})
		_ = b.editMessage(ctx, cfg, chatID, msgID, sb.String(), &notify.InlineKeyboardMarkup{InlineKeyboard: rows})

	case data == "do_clean:org_run":
		_ = b.answerCallback(ctx, cfg, cb.ID, "正在整理...")
		res, err := b.pipeline.RunOrganizeWithMode(ctx, false, true)
		msg := "❌ 目录整理失败"
		if err == nil {
			msg = fmt.Sprintf("✅ <b>日期目录整理完成</b>\n• 已归并进历史归档：<b>%d</b> 个\n• 已清理空目录：<b>%d</b> 个\n• 跳过活跃任务：%d 个 · 失败：%d 个",
				res.MovedCount, res.DeletedEmpty, res.SkippedActive, res.FailedCount)
		}
		_ = b.editMessage(ctx, cfg, chatID, msgID, msg, backHomeKB())

	case data == "do_clean:clear_done":
		_ = b.answerCallback(ctx, cfg, cb.ID, "正在清空...")
		n, err := b.store.ClearPipelines(ctx, "terminal")
		msg := "❌ 清空记录失败"
		if err == nil {
			msg = fmt.Sprintf("🧹 已清空 <b>%d</b> 条已结束任务记录（不会删除任何真实文件）。", n)
		}
		_ = b.editMessage(ctx, cfg, chatID, msgID, msg, backHomeKB())

	case data == "sched:toggle":
		next := !b.pipeline.IsPaused()
		_ = b.pipeline.SetPaused(ctx, next)
		if next {
			_ = b.answerCallback(ctx, cfg, cb.ID, "已暂停后台调度")
		} else {
			_ = b.answerCallback(ctx, cfg, cb.ID, "已恢复后台调度")
		}
		txt, kb := b.renderMainMenu(ctx)
		_ = b.editMessage(ctx, cfg, chatID, msgID, txt, kb)

	case data == "retry_all":
		n, _ := b.store.RetryAllFailed(ctx)
		_ = b.answerCallback(ctx, cfg, cb.ID, fmt.Sprintf("已重试 %d 条失败任务", n))
		txt, kb := b.renderTasks(ctx, "all")
		_ = b.editMessage(ctx, cfg, chatID, msgID, fmt.Sprintf("🔁 已重新入队 <b>%d</b> 条失败任务。\n\n", n)+txt, kb)

	case data == "rules":
		_ = b.answerCallback(ctx, cfg, cb.ID, "")
		txt, kb := b.renderRules(ctx)
		_ = b.editMessage(ctx, cfg, chatID, msgID, txt, kb)

	case strings.HasPrefix(data, "set_orig:"):
		act := strings.TrimPrefix(data, "set_orig:")
		rule := b.pipeline.ArchiveRule(ctx)
		rule.OriginalAction = act
		_, _ = b.pipeline.SaveArchiveRule(ctx, rule)
		_ = b.answerCallback(ctx, cfg, cb.ID, "原包处理规则已更新")
		txt, kb := b.renderRules(ctx)
		_ = b.editMessage(ctx, cfg, chatID, msgID, txt, kb)

	case strings.HasPrefix(data, "set_fail:"):
		act := strings.TrimPrefix(data, "set_fail:")
		rule := b.pipeline.ArchiveRule(ctx)
		rule.CloudFailureAction = act
		_, _ = b.pipeline.SaveArchiveRule(ctx, rule)
		_ = b.answerCallback(ctx, cfg, cb.ID, "云解压失败策略已更新")
		txt, kb := b.renderRules(ctx)
		_ = b.editMessage(ctx, cfg, chatID, msgID, txt, kb)

	case strings.HasPrefix(data, "set_notify:"):
		mode := strings.TrimPrefix(data, "set_notify:")
		if mode != "errors" && mode != "result" && mode != "all" {
			_ = b.answerCallback(ctx, cfg, cb.ID, "无效的通知范围")
			return
		}
		cur := b.notifier.LoadTelegram(ctx)
		cur.Mode = mode
		_ = b.store.SetSetting(ctx, "notify.telegram", cur, true)
		_ = b.answerCallback(ctx, cfg, cb.ID, "通知推送范围已切换")
		txt, kb := b.renderRules(ctx)
		_ = b.editMessage(ctx, cfg, chatID, msgID, txt, kb)

	case strings.HasPrefix(data, "loc:"):
		pipeID := strings.TrimPrefix(data, "loc:")
		p, err := b.pipeline.ConfirmLocalFallback(ctx, pipeID)
		if err != nil {
			_ = b.answerCallback(ctx, cfg, cb.ID, "该任务已不在待确认状态")
			return
		}
		_ = b.answerCallback(ctx, cfg, cb.ID, "已批准转本地 7-Zip 解压")
		_ = b.sendMessage(ctx, cfg, chatID,
			fmt.Sprintf("✅ <b>已批准本地兜底</b>\n任务：<code>%s</code>\n正在通过 115 直链缓存到本地 7-Zip 解压。", html.EscapeString(p.Name)), backHomeKB())

	case strings.HasPrefix(data, "rty:"):
		pipeID := strings.TrimPrefix(data, "rty:")
		p, err := b.store.RetryPipeline(ctx, pipeID)
		if err != nil {
			_ = b.answerCallback(ctx, cfg, cb.ID, "当前状态不可重试")
			return
		}
		_ = b.answerCallback(ctx, cfg, cb.ID, "任务已重新入队")
		_ = b.sendMessage(ctx, cfg, chatID,
			fmt.Sprintf("🔁 <b>已重新加入处理队列</b>\n任务：<code>%s</code>", html.EscapeString(p.Name)), backHomeKB())

	case strings.HasPrefix(data, "can:"):
		pipeID := strings.TrimPrefix(data, "can:")
		p, err := b.store.CancelPipeline(ctx, pipeID)
		if err != nil {
			_ = b.answerCallback(ctx, cfg, cb.ID, "当前状态不可暂停")
			return
		}
		_ = b.answerCallback(ctx, cfg, cb.ID, "任务已暂停")
		_ = b.sendMessage(ctx, cfg, chatID,
			fmt.Sprintf("⏸️ <b>已暂停任务</b>\n任务：<code>%s</code>\n可随时在任务列表点击继续。", html.EscapeString(p.Name)), backHomeKB())

	default:
		_ = b.answerCallback(ctx, cfg, cb.ID, "")
	}
}

func (b *Bot) renderMainMenu(ctx context.Context) (string, *notify.InlineKeyboardMarkup) {
	paused := b.pipeline.IsPaused()
	schedLabel := "⏸️ 暂停后台调度"
	schedState := "🟢 运行中"
	if paused {
		schedLabel = "▶️ 恢复后台调度"
		schedState = "🟡 已暂停"
	}
	stats, _ := b.store.MaintenanceStats(ctx, b.dbPath)
	txt := fmt.Sprintf(
		"⛵ <b>方舟 · 快捷主控台</b>\n"+
			"━━━━━━━━━━━━━━━\n"+
			"• 调度状态：%s\n"+
			"• 任务概况：进行中 <b>%d</b> · 失败 <b>%d</b> · 完成 <b>%d</b>\n\n"+
			"💡 直接发送<b>磁链 / 电驴 / 链接</b>、<b>.txt 文件</b>或 <b>.torrent 种子</b>即可提交 115 离线。",
		schedState, stats.ActivePipelines, stats.FailedPipelines, stats.CompletedPipelines,
	)
	kb := &notify.InlineKeyboardMarkup{
		InlineKeyboard: [][]notify.InlineButton{
			{{Text: "📊 状态与任务", CallbackData: "status"}, {Text: "🔍 触发扫描", CallbackData: "scan"}},
			{{Text: "🧹 整理与维护", CallbackData: "clean"}, {Text: "⚙️ 规则与通知", CallbackData: "rules"}},
			{{Text: schedLabel, CallbackData: "sched:toggle"}},
		},
	}
	return txt, kb
}

func (b *Bot) renderStatus(ctx context.Context) (string, *notify.InlineKeyboardMarkup) {
	stats, _ := b.store.MaintenanceStats(ctx, b.dbPath)
	cs := b.pipeline.CacheSummary()
	mem := metrics.Snapshot()

	rssBytes := int64(mem.Go.HeapAllocBytes)
	if mem.Process.Available && mem.Process.RSSBytes != nil && *mem.Process.RSSBytes > 0 {
		rssBytes = int64(*mem.Process.RSSBytes)
	}
	cgText := "本机模式"
	if mem.Container.Available && mem.Container.UsageBytes != nil {
		cgText = fmtBytes(int64(*mem.Container.UsageBytes))
		if mem.Container.LimitBytes != nil && *mem.Container.LimitBytes > 0 {
			cgText += " / " + fmtBytes(int64(*mem.Container.LimitBytes))
		}
	}
	sched := "🟢 运行中"
	if b.pipeline.IsPaused() {
		sched = "🟡 已暂停"
	}
	up := int64(0)
	if b.metrics != nil {
		up = b.metrics.UptimeSeconds()
	}

	txt := fmt.Sprintf(
		"📊 <b>方舟 · 运行状态</b>\n"+
			"• 后台调度：%s\n"+
			"• 进程内存：<b>%s</b>（协程 %d）\n"+
			"• 容器内存：%s\n"+
			"• 在线时长：%s\n"+
			"• 本地缓存：%s（%d 个文件）\n"+
			"• 数据库体积：%s\n"+
			"• 任务统计：活跃 %d · 失败 %d · 完成 %d",
		sched, fmtBytes(rssBytes), mem.Go.Goroutines, cgText, fmtDuration(up),
		fmtBytes(cs.CacheBytes), cs.CacheFiles, fmtBytes(stats.DBBytes),
		stats.ActivePipelines, stats.FailedPipelines, stats.CompletedPipelines,
	)
	kb := &notify.InlineKeyboardMarkup{
		InlineKeyboard: [][]notify.InlineButton{
			{{Text: "📋 全部任务", CallbackData: "tasks:all"}, {Text: "⏳ 进行中", CallbackData: "tasks:active"}},
			{{Text: "⚠️ 待确认", CallbackData: "tasks:attention"}, {Text: "❌ 失败项", CallbackData: "tasks:failed"}},
			{{Text: "🔄 刷新状态", CallbackData: "status"}, {Text: "🔁 重试全部失败", CallbackData: "retry_all"}},
			{{Text: "🏠 返回主控台", CallbackData: "menu"}},
		},
	}
	return txt, kb
}

func (b *Bot) renderTasks(ctx context.Context, filter string) (string, *notify.InlineKeyboardMarkup) {
	all, _ := b.store.ListPipelines(ctx, "", "", 60)
	filtered := make([]store.Pipeline, 0, len(all))
	for _, p := range all {
		switch filter {
		case "active":
			if p.State == "completed" || strings.Contains(p.State, "fail") || p.State == "cancelled" || p.State == "wait_local_confirm" {
				continue
			}
		case "attention":
			if p.State != "wait_local_confirm" && p.State != "submission_unknown" && p.State != "cancelled" {
				continue
			}
		case "failed":
			if !strings.Contains(p.State, "fail") {
				continue
			}
		}
		filtered = append(filtered, p)
	}

	titleMap := map[string]string{"all": "全部", "active": "进行中", "attention": "待确认/暂停", "failed": "失败"}
	fTitle := titleMap[filter]
	if fTitle == "" {
		fTitle = "全部"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📋 <b>任务列表 · %s（共 %d 条）</b>\n", fTitle, len(filtered)))
	if len(filtered) == 0 {
		sb.WriteString("\n暂无符合条件的任务记录。")
	} else {
		limit := len(filtered)
		if limit > 8 {
			limit = 8
		}
		for i, p := range filtered[:limit] {
			sb.WriteString(fmt.Sprintf("\n%d. %s <b>%s</b>\n    <code>%s</code>",
				i+1, stateEmoji(p.State), html.EscapeString(zhState(p.State)), html.EscapeString(shorten(p.Name, 36))))
		}
	}

	rows := [][]notify.InlineButton{
		{
			{Text: "全部", CallbackData: "tasks:all"},
			{Text: "进行中", CallbackData: "tasks:active"},
			{Text: "待确认", CallbackData: "tasks:attention"},
			{Text: "失败", CallbackData: "tasks:failed"},
		},
	}
	// Offer up to 2 direct quick buttons for tasks waiting local fallback approval.
	quick := 0
	for _, p := range filtered {
		if p.State == "wait_local_confirm" && quick < 2 {
			rows = append(rows, []notify.InlineButton{
				{Text: "⬇️ 转本地：" + shorten(p.Name, 14), CallbackData: "loc:" + p.ID},
				{Text: "🔁 重试云解压", CallbackData: "rty:" + p.ID},
			})
			quick++
		}
	}
	rows = append(rows, []notify.InlineButton{
		{Text: "🔁 重试全部失败", CallbackData: "retry_all"},
		{Text: "🏠 主控台", CallbackData: "menu"},
	})
	return sb.String(), &notify.InlineKeyboardMarkup{InlineKeyboard: rows}
}

func (b *Bot) renderScanMenu(ctx context.Context) (string, *notify.InlineKeyboardMarkup) {
	depth := b.pipeline.P115Config().ScanDepth()
	txt := fmt.Sprintf(
		"🔍 <b>手动触发扫描</b>\n"+
			"━━━━━━━━━━━━━━━\n"+
			"• <b>仅扫今日（推荐）</b>：只进当天日期目录，极低请求数防风控\n"+
			"• <b>按深度扫描</b>：按当前配置 <b>%d 层</b> 遍历全部云监控目录",
		depth,
	)
	kb := &notify.InlineKeyboardMarkup{
		InlineKeyboard: [][]notify.InlineButton{
			{{Text: "☁️ 扫云目录（仅今日）", CallbackData: "do_scan:today"}},
			{{Text: "📂 扫本地监控目录", CallbackData: "do_scan:local"}},
			{{Text: fmt.Sprintf("⚠️ 按深度扫描云目录（%d 层）", depth), CallbackData: "confirm_scan:deep"}},
			{{Text: "🏠 返回主控台", CallbackData: "menu"}},
		},
	}
	return txt, kb
}

func (b *Bot) renderCleanMenu(ctx context.Context) (string, *notify.InlineKeyboardMarkup) {
	stats, _ := b.store.MaintenanceStats(ctx, b.dbPath)
	cs := b.pipeline.CacheSummary()
	txt := fmt.Sprintf(
		"🧹 <b>整理与空间维护</b>\n"+
			"━━━━━━━━━━━━━━━\n"+
			"• 本地下载缓存：<b>%s</b>（%d 个文件）\n"+
			"• 已结束任务：%d 条完成 / %d 条失败\n"+
			"• 数据库体积：%s\n\n"+
			"<code>手动整理将归档昨日及以前的日期目录，活跃任务自动避让</code>",
		fmtBytes(cs.CacheBytes), cs.CacheFiles, stats.CompletedPipelines, stats.FailedPipelines, fmtBytes(stats.DBBytes),
	)
	kb := &notify.InlineKeyboardMarkup{
		InlineKeyboard: [][]notify.InlineButton{
			{{Text: "🗂 整理昨日及以前（预览）", CallbackData: "do_clean:org_preview"}},
			{{Text: "🗑️ 回收本地缓存", CallbackData: "do_clean:cache"}, {Text: "🧾 清空已结束记录", CallbackData: "do_clean:clear_done"}},
			{{Text: "🏠 返回主控台", CallbackData: "menu"}},
		},
	}
	return txt, kb
}

func (b *Bot) renderRules(ctx context.Context) (string, *notify.InlineKeyboardMarkup) {
	rule := b.pipeline.ArchiveRule(ctx)
	tgCfg := b.notifier.LoadTelegram(ctx)
	origMap := map[string]string{
		"archive": "移入「归档区/原包留存」",
		"keep":    "保留在原目录不动",
		"delete":  "解压后删除原包",
	}
	failMap := map[string]string{
		"confirm_local": "等待手动确认本地兜底",
		"auto_local":    "自动切换本地 7-Zip 解压",
		"move_failed":   "移入「归档区/异常待查」",
	}
	notifyMap := map[string]string{
		"errors": "仅异常与待确认",
		"result": "完成与异常（推荐）",
		"all":    "全部任务与扫描事件",
	}
	txt := fmt.Sprintf(
		"⚙️ <b>规则与通知设置</b>\n"+
			"━━━━━━━━━━━━━━━\n"+
			"• 解压后原包：<b>%s</b>\n"+
			"• 云解压失败：<b>%s</b>\n"+
			"• 通知推送范围：<b>%s</b>\n"+
			"<code>点击下方按钮一键切换，立即生效</code>",
		origMap[rule.OriginalAction], failMap[rule.CloudFailureAction], notifyMap[tgCfg.Mode],
	)
	mark := func(cur, val, label string) string {
		if cur == val {
			return "✓ " + label
		}
		return label
	}
	kb := &notify.InlineKeyboardMarkup{
		InlineKeyboard: [][]notify.InlineButton{
			{
				{Text: mark(rule.OriginalAction, "archive", "原包归档"), CallbackData: "set_orig:archive"},
				{Text: mark(rule.OriginalAction, "keep", "保留原包"), CallbackData: "set_orig:keep"},
				{Text: mark(rule.OriginalAction, "delete", "删除原包"), CallbackData: "set_orig:delete"},
			},
			{
				{Text: mark(rule.CloudFailureAction, "confirm_local", "待确认本地"), CallbackData: "set_fail:confirm_local"},
				{Text: mark(rule.CloudFailureAction, "auto_local", "自动本地"), CallbackData: "set_fail:auto_local"},
				{Text: mark(rule.CloudFailureAction, "move_failed", "移入异常"), CallbackData: "set_fail:move_failed"},
			},
			{
				{Text: mark(tgCfg.Mode, "errors", "仅异常"), CallbackData: "set_notify:errors"},
				{Text: mark(tgCfg.Mode, "result", "完成与异常"), CallbackData: "set_notify:result"},
				{Text: mark(tgCfg.Mode, "all", "全部事件"), CallbackData: "set_notify:all"},
			},
			{{Text: "🏠 返回主控台", CallbackData: "menu"}},
		},
	}
	return txt, kb
}

func backHomeKB() *notify.InlineKeyboardMarkup {
	return &notify.InlineKeyboardMarkup{
		InlineKeyboard: [][]notify.InlineButton{
			{{Text: "🏠 返回主菜单", CallbackData: "menu"}},
		},
	}
}

func (b *Bot) sendMessage(ctx context.Context, cfg notify.TelegramConfig, chatID int64, text string, kb *notify.InlineKeyboardMarkup) error {
	body := map[string]any{
		"chat_id":                  chatID,
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	}
	if kb != nil {
		body["reply_markup"] = kb
	}
	_, err := b.callAPI(ctx, cfg, "sendMessage", body, 20*time.Second)
	return err
}

func (b *Bot) editMessage(ctx context.Context, cfg notify.TelegramConfig, chatID, msgID int64, text string, kb *notify.InlineKeyboardMarkup) error {
	body := map[string]any{
		"chat_id":                  chatID,
		"message_id":               msgID,
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	}
	if kb != nil {
		body["reply_markup"] = kb
	}
	_, err := b.callAPI(ctx, cfg, "editMessageText", body, 20*time.Second)
	if err != nil {
		// Fallback to sendMessage if original message cannot be edited (e.g. unchanged text or too old).
		return b.sendMessage(ctx, cfg, chatID, text, kb)
	}
	return nil
}

func (b *Bot) answerCallback(ctx context.Context, cfg notify.TelegramConfig, callbackID, text string) error {
	body := map[string]any{"callback_query_id": callbackID}
	if text != "" {
		body["text"] = text
	}
	_, err := b.callAPI(ctx, cfg, "answerCallbackQuery", body, 10*time.Second)
	return err
}

func (b *Bot) downloadFile(ctx context.Context, cfg notify.TelegramConfig, fileID string) ([]byte, error) {
	raw, err := b.callAPI(ctx, cfg, "getFile", map[string]any{"file_id": fileID}, 20*time.Second)
	if err != nil {
		return nil, err
	}
	var resp struct {
		OK     bool `json:"ok"`
		Result struct {
			FilePath string `json:"file_path"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil || !resp.OK || resp.Result.FilePath == "" {
		return nil, errors.New("TELEGRAM_GETFILE_FAILED")
	}
	client, err := notify.BuildHTTPClient(cfg.ProxyURL, 30*time.Second)
	if err != nil {
		return nil, err
	}
	fileURL := fmt.Sprintf("%s/file/bot%s/%s", apiBase(cfg), cfg.BotToken, strings.TrimLeft(resp.Result.FilePath, "/"))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return nil, err
	}
	httpResp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("TELEGRAM_FILE_DOWNLOAD_FAILED")
	}
	defer httpResp.Body.Close()
	if httpResp.StatusCode < 200 || httpResp.StatusCode >= 300 {
		return nil, fmt.Errorf("TELEGRAM_FILE_HTTP_%d", httpResp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(httpResp.Body, torrent.MaxTorrentBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > torrent.MaxTorrentBytes {
		return nil, torrent.ErrSizeLimit
	}
	return data, nil
}

func (b *Bot) callAPI(ctx context.Context, cfg notify.TelegramConfig, method string, payload any, timeout time.Duration) ([]byte, error) {
	client, err := notify.BuildHTTPClient(cfg.ProxyURL, timeout)
	if err != nil {
		return nil, err
	}
	bodyBytes, _ := json.Marshal(payload)
	endpoint := fmt.Sprintf("%s/bot%s/%s", apiBase(cfg), cfg.BotToken, method)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, errors.New("TELEGRAM_REQUEST_BUILD_FAILED")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		// Never expose raw URL containing bot token.
		return nil, errors.New("TELEGRAM_API_REQUEST_FAILED")
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if err != nil {
		return nil, errors.New("TELEGRAM_READ_FAILED")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("TELEGRAM_HTTP_%d", resp.StatusCode)
	}
	return raw, nil
}

func apiBase(cfg notify.TelegramConfig) string {
	base := strings.TrimRight(strings.TrimSpace(cfg.APIBase), "/")
	if base == "" {
		return "https://api.telegram.org"
	}
	return base
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func zhState(st string) string {
	m := map[string]string{
		"queued":              "排队中",
		"submitting":          "提交中",
		"submitted":           "已提交离线",
		"downloading":         "离线下载中",
		"discover":            "扫描产物",
		"seed_cleanup":        "种子清理中",
		"cloud_prepare":       "云预检",
		"cloud_inspect":       "云分析中",
		"cloud_export":        "云导出中",
		"cloud_progress":      "云解压中",
		"wait_local_confirm":  "待确认本地兜底",
		"caching":             "直链缓存中",
		"local_extract":       "本地解压中",
		"completed":           "已完成",
		"cancelled":           "已暂停",
		"failed":              "失败",
		"submit_failed":       "提交失败",
		"seed_cleanup_failed": "清理失败",
		"submission_unknown":  "待核对",
	}
	if v, ok := m[st]; ok {
		return v
	}
	return st
}

func stateEmoji(st string) string {
	switch {
	case st == "completed":
		return "✅"
	case strings.Contains(st, "fail"):
		return "❌"
	case st == "wait_local_confirm" || st == "submission_unknown":
		return "⚠️"
	case st == "cancelled":
		return "⏸️"
	default:
		return "⏳"
	}
}

func zhDomainErr(err error) string {
	switch {
	case errors.Is(err, pipeline.ErrP115NotConfigured):
		return "请先在网页「系统设置 → 115 与云目录」配置 115 Cookie"
	case errors.Is(err, pipeline.ErrCloudRootRequired):
		return "请先在网页「系统设置 → 115 与云目录」初始化云根目录"
	case errors.Is(err, torrent.ErrSizeLimit):
		return "种子文件超过 2 MB 上限"
	case errors.Is(err, torrent.ErrV2Unsupported):
		return "暂不支持纯 BT v2 种子"
	case errors.Is(err, torrent.ErrInvalid):
		return "种子文件格式无效"
	default:
		return err.Error()
	}
}

func shorten(s string, maxRunes int) string {
	r := []rune(s)
	if len(r) <= maxRunes {
		return s
	}
	return string(r[:maxRunes-1]) + "…"
}

func fmtBytes(b int64) string {
	if b <= 0 {
		return "0 B"
	}
	switch {
	case b >= 1073741824:
		return fmt.Sprintf("%.2f GB", float64(b)/1073741824)
	case b >= 1048576:
		return fmt.Sprintf("%.1f MB", float64(b)/1048576)
	case b >= 1024:
		return fmt.Sprintf("%.0f KB", float64(b)/1024)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func fmtDuration(sec int64) string {
	if sec < 60 {
		return fmt.Sprintf("%d 秒", sec)
	}
	m := sec / 60
	if m < 60 {
		return fmt.Sprintf("%d 分钟", m)
	}
	h := m / 60
	if h < 24 {
		return fmt.Sprintf("%d 小时 %d 分", h, m%60)
	}
	return fmt.Sprintf("%d 天 %d 小时", h/24, h%24)
}
