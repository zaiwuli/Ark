/* Ark Operations Console — 100% Chinese Labels, Paginated Table, Detail Drawer, iOS PWA */
const state = {
  tab: 'tasks',
  taskKind: '',
  statFilter: '',
  page: 1,
  pageSize: 20,
  pipelines: [],
  sourcesMap: {},
  polling: true,
  schedulerPaused: false,
  selectedFile: null,
  timer: null,
};

const TAB_TITLES = {
  tasks: '任务中心',
  import: '离线导入',
  sources: '来源与规则',
  organize: '目录整理中心',
  logs: '工作日志',
  settings: '系统设置',
};

/* Complete Chinese translation dictionaries — zero raw English codes in UI or logs */
const STAGE_ZH = {
  offline_submit: '提交离线任务',
  offline_poll: '等待 115 离线下载',
  discover: '扫描离线产物',
  seed_cleanup: '种子下载后清理',
  cloud_prepare: '云解压预检',
  cloud_inspect: '检查压缩包内容',
  cloud_export: '云端导出文件',
  cloud_progress: '云端解压中',
  local_fallback: '等待确认本地兜底',
  cache: '直链缓存原包',
  local_extract: '本地 7-Zip 解压',
  complete: '处理完成',
  cancelled: '已暂停',
  cloud_monitor: '云端监测',
  bark: 'Bark 联动触发',
  organize: '目录整理',
  test: '通道测试',
};

const EVENT_KIND_ZH = {
  REGISTERED: '任务登记',
  SUBMITTED: '已提交离线',
  SUBMIT_FAILED: '离线提交失败',
  SUBMISSION_UNKNOWN: '提交待核对',
  OFFLINE_SUCCEEDED: '离线下载完成',
  ARCHIVE_HANDOFF: '转交云解压',
  NO_ARCHIVE_FOUND: '无压缩包',
  DISCOVERED: '发现压缩包',
  CLOUD_EXPORT_STARTED: '启动云解压',
  CLOUD_EXTRACT_RETRIED: '云解压自动重试',
  CLOUD_SOURCE_SCANNED: '扫描云监控目录',
  ORIGINAL_ARCHIVED: '原包移入归档区',
  ORIGINAL_DELETED: '已清理原包',
  ARCHIVE_MOVE_WARN: '原包归档提醒',
  AUTO_LOCAL_FALLBACK: '自动切换本地',
  WAITING_APPROVAL: '待确认本地兜底',
  LOCAL_FALLBACK_APPROVED: '已确认本地兜底',
  CACHE_READY: '直链缓存完成',
  LOCAL_EXTRACT_STARTED: '开始本地解压',
  SEED_CLEANUP_STARTED: '开始种子清理',
  SEED_FILE_DELETED: '清理文件',
  SEED_CLEANUP_COMPLETED: '种子清理完成',
  SEED_CLEANUP_FAILED: '种子清理失败',
  CLOUD_ROOT_INITIALIZED: '云目录初始化',
  LIFE_MONITOR_CHECK: '生活事件检查',
  BARK_WINDOW_FIRED: 'Bark 窗口触发',
  BARK_TARGET_CHECKED: 'Bark 定向扫描',
  BARK_PATH_FAILED: 'Bark 路径解析失败',
  CACHE_RECLAIMED: '缓存回收',
  DATE_ORGANIZED: '日期目录整理',
  RETRIED: '恢复 / 重试',
  CANCELLED: '已暂停任务',
  SCHEDULER_TOGGLED: '调度开关',
  SUCCEEDED: '处理成功',
  FAILED: '处理失败',
};

const ERROR_CODE_ZH = {
  REMOTE_OFFLINE_FAILED: '115 离线下载失败（可能死种、无源或链接失效）',
  REMOTE_REJECTED: '115 服务端拒绝了本次请求',
  REMOTE_UNAVAILABLE: '115 网络连接异常，暂时无法访问',
  REMOTE_RATE_LIMIT: '触发 115 频率限制，已自动退避保护',
  REMOTE_CIRCUIT_OPEN: '115 触发风控熔断，已暂停请求等待恢复',
  AUTH_OR_CHALLENGE_REQUIRED: '115 登录已失效或需要人工验证，请更新 Cookie',
  CID_NOT_VERIFIED: '云根目录编号（CID）不存在或无权访问，请核对 115 目录链接中的 cid 数字（根目录填 0）',
  INVALID_REMOTE_RESPONSE: '115 返回了非预期响应（常见原因：Cookie 格式不完整、登录已失效或触发风控验证）',
  INVALID_FILE_LIST: '读取 115 目录文件列表失败，请检查 Cookie 是否有效',
  AMBIGUOUS_FOLDER: '目标目录下存在多个同名文件夹，为防冲突已停止，请先清理重名目录',
  FOLDER_CREATE_UNKNOWN: '创建 115 文件夹结果未返回目录编号，请稍后重试',
  REQUEST_BUILD_FAILED: '构造 115 请求失败，请检查 Cookie 中是否包含换行符等非法字符',
  PARTIAL_SUBMISSION_REQUIRES_RECONCILIATION: '批量提交部分超时，正在核对 115 离线列表防止重复提交',
  SUBMISSION_NOT_FOUND: '核对 115 离线列表未发现该任务，可安全重试',
  CLOUD_PREPARE_FAILED: '115 云解压预检失败',
  CLOUD_INSPECT_FAILED: '115 云解压分析失败',
  CLOUD_PASSWORD_FAILED: '云解压密码错误，密码本中未命中有效密码',
  CLOUD_OUTPUT_DIR_FAILED: '创建云解压输出目录失败',
  CLOUD_EXPORT_FAILED: '提交云解压导出失败',
  CLOUD_EXTRACT_FAILED: '115 云解压执行失败（疑似加密文件名或非常规分卷）',
  CACHE_DOWNLOAD_FAILED: '115 直链下载缓存失败',
  LOCAL_EXTRACT_FAILED: '本地 7-Zip 解压失败',
  PASSWORD_OR_CORRUPT_ARCHIVE: '压缩包密码错误、分卷缺失或文件已损坏',
  ARCHIVE_DICT_EXCEEDS_MEMORY_LIMIT: '压缩包字典过大，超出设定的低内存安全阈值（已拦截防内存溢出）',
  ARCHIVE_SPECIAL_FILE_REJECTED: '压缩包包含不安全的软链接或特殊文件，已安全拒绝',
  UNSAFE_ARCHIVE_PATH: '压缩包包含非法路径穿越字符，已安全拦截',
  DUPLICATE_ARCHIVE_ENTRY: '压缩包存在重名冲突条目，已停止解压',
  ARCHIVE_FILE_LIMIT: '压缩包内文件数量超过安全上限',
  ARCHIVE_SIZE_LIMIT: '压缩包解压体积超过安全上限',
  EXTRACTOR_UNAVAILABLE: '本机未检测到 7-Zip 解压组件',
  SEED_CLEANUP_FAILED: '种子下载成功，但后置清理未完成',
  SEED_RESOURCE_DIRECTORY_NOT_UNIQUE: '云端未找到唯一的种子同名资源目录，为防误删已停止清理',
  UNSAFE_SEED_ROOT: '目标目录属于受保护根目录，已拒绝清理',
  CLOUD_ROOT_REQUIRED: '请先在「系统设置 → 115 与云目录」初始化云根目录',
  '115_CONNECTION_REQUIRED': '请先在「系统设置 → 115 与云目录」配置 115 Cookie',
  NO_LINKS: '未识别到有效的磁链、电驴或 HTTP/FTP 链接',
  INVALID_TORRENT: '种子文件格式无效或内容不完整',
  TORRENT_V2_UNSUPPORTED: '暂不支持纯 BT v2 种子，请使用 BT v1 或混合种子',
  TORRENT_SIZE_LIMIT: '种子文件超过 2 MB 上限',
  TORRENT_IMPORT_SEPARATELY: '种子文件与粘贴链接请分别提交',
  IDEMPOTENCY_KEY_CONFLICT: '重复请求键内容不一致',
  TELEGRAM_NOT_CONFIGURED: '请先填写并保存 Telegram 机器人令牌与会话编号',
  TELEGRAM_REQUEST_FAILED: '连接 Telegram 接口失败，请检查反代地址或代理设置',
  LOGIN_REQUIRED: '当前已启用令牌保护，请携带操作令牌访问',
  INVALID_TOKEN: '接口令牌无效或已撤销',
};

const NOTIFY_STATUS_ZH = {
  none: '无需通知',
  pending: '等待推送',
  sent: '已推送',
  failed: '推送失败',
};

const ROLE_ZH = {
  admin: '操作权限',
  readonly: '只读权限',
};

const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (m) => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
}[m]));

function zhStage(s) { return STAGE_ZH[s] || s || '处理中'; }
function zhEventKind(k) { return EVENT_KIND_ZH[k] || k || '系统事件'; }
function zhNotifyStatus(s) { return NOTIFY_STATUS_ZH[s] || s || '无需通知'; }
function zhRole(r) { return ROLE_ZH[r] || r || '-'; }

function zhError(code, msg) {
  let text = String(msg || '').trim();
  const codeText = ERROR_CODE_ZH[code] || '';
  if (!text) return codeText || (code ? '操作未能完成，请查看详情' : '');
  // Replace any known raw English codes embedded inside message text
  Object.keys(ERROR_CODE_ZH).forEach((k) => {
    if (text.includes(k)) {
      text = text.replaceAll(k, ERROR_CODE_ZH[k]);
    }
  });
  text = text.replace(/^[:：\s]+/, '');
  if (codeText && !text.includes(codeText.slice(0, 6))) {
    // Prefer clear Chinese explanation without duplicating
    return /^[A-Za-z0-9_ :.-]+$/.test(text) ? codeText : text;
  }
  return text;
}

const fmtSize = (b) => {
  if (!b || b <= 0) return '-';
  if (b >= 1073741824) return (b / 1073741824).toFixed(2) + ' GB';
  if (b >= 1048576) return (b / 1048576).toFixed(1) + ' MB';
  return (b / 1024).toFixed(0) + ' KB';
};
const fmtMB = (b) => (b == null ? '--' : (b / 1048576).toFixed(1) + ' MB');
const fmtTime = (u) => (u ? new Date(u * 1000).toLocaleString('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false }) : '--');
const fmtFullTime = (u) => (u ? new Date(u * 1000).toLocaleString('zh-CN', { hour12: false }) : '--');
const fmtUptime = (sec) => {
  if (sec == null) return '--';
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h > 0) return `${h} 小时 ${m} 分`;
  if (m > 0) return `${m} 分 ${s} 秒`;
  return `${s} 秒`;
};

/* Theme & Dynamic iOS Status Bar Theme Color */
function initTheme() { applyTheme(localStorage.getItem('ark_theme') || 'dark'); }
function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  localStorage.setItem('ark_theme', theme);
  const label = document.getElementById('theme-label');
  if (label) label.textContent = theme === 'dark' ? '浅色' : '深色';
  const meta = document.getElementById('meta-theme-color');
  if (meta) meta.setAttribute('content', theme === 'dark' ? '#0b1120' : '#f1f5f9');
}
function toggleTheme() {
  applyTheme((document.documentElement.getAttribute('data-theme') || 'dark') === 'dark' ? 'light' : 'dark');
}

let toastTimer = null;
function showToast(msg) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.remove('show'), 3600);
}
function showAlert(id, msg) {
  const el = document.getElementById(id);
  if (!el) return;
  el.className = 'alert show alert-err';
  el.querySelector('.alert-text').textContent = msg;
}
function hideAlert(id) {
  const el = document.getElementById(id);
  if (el) el.className = 'alert';
}

/* Primary & Secondary Navigation */
function switchTab(tab) {
  state.tab = tab;
  document.querySelectorAll('.view').forEach((v) => v.classList.toggle('active', v.id === 'view-' + tab));
  document.querySelectorAll('.nav-item, .m-tab').forEach((n) => n.classList.toggle('active', n.dataset.tab === tab));
  document.getElementById('page-title').textContent = TAB_TITLES[tab] || '方舟';

  if (tab === 'tasks') loadPipelines();
  if (tab === 'sources') { loadSources(); loadArchiveRule(); }
  if (tab === 'organize') { loadOrganizeCenter(); loadOrganizerConfig(); }
  if (tab === 'logs') loadLogs();
  if (tab === 'settings') { loadP115(); loadTelegram(); loadWebhook(); loadPasswords(); loadTokens(); refreshTelemetry(true); }
}

function switchSubTab(viewName, paneId, btn) {
  const view = document.getElementById('view-' + viewName);
  if (!view) return;
  view.querySelectorAll('.sub-pane').forEach((p) => p.classList.toggle('active', p.id === paneId));
  view.querySelectorAll('.sub-tab').forEach((b) => b.classList.toggle('active', b === btn || b.dataset.sub === paneId));
  if (paneId === 'set-status') { refreshTelemetry(true); loadMaintenance(); }
  if (paneId === 'org-auto-pane') loadOrganizerConfig();
  if (paneId === 'org-center-pane') loadOrganizeCenter();
  if (paneId === 'archive-rule-pane') loadArchiveRule();
}

function openSettingsStatus() {
  switchTab('settings');
  const btn = document.querySelector('#view-settings .sub-tab[data-sub="set-status"]');
  switchSubTab('settings', 'set-status', btn);
}

function togglePolling() {
  state.polling = !state.polling;
  document.getElementById('live-dot').classList.toggle('paused', !state.polling);
  document.getElementById('poll-btn').textContent = state.polling ? '暂停刷新' : '恢复刷新';
  document.getElementById('live-text').textContent = state.polling ? '实时同步' : '已暂停';
  if (state.polling) refreshTelemetry(true);
}

/* Telemetry */
async function refreshTelemetry(force = false) {
  if (!force && (!state.polling || document.hidden)) return;
  try {
    const [st, mt] = await Promise.all([
      fetch('/api/status').then((r) => r.json()),
      fetch('/api/v1/system/metrics').then((r) => r.json()),
    ]);
    document.getElementById('brand-ver').textContent = `版本 ${st.version}`;
    const g = mt.memory.go, p = mt.memory.process, c = mt.memory.container;
    const mainBytes = p.available ? p.rss_bytes : g.sys_bytes;
    const pct = Math.min(100, Math.round((mainBytes / (100 * 1048576)) * 100));

    document.getElementById('sb-mem-val').textContent = `${fmtMB(mainBytes)} / 100 MB`;
    const fill = document.getElementById('sb-mem-fill');
    fill.style.width = `${Math.max(4, pct)}%`;
    fill.className = 'progress-fill' + (pct >= 85 ? ' err' : pct >= 60 ? ' warn' : '');

    document.getElementById('kpi-mem').textContent = p.available ? fmtMB(p.rss_bytes) : fmtMB(g.heap_alloc_bytes);
    document.getElementById('kpi-mem-sub').textContent = `堆内存 ${fmtMB(g.heap_alloc_bytes)} · 系统申请 ${fmtMB(g.sys_bytes)} · ${g.goroutines} 协程`;
    document.getElementById('kpi-cgroup').textContent = c.available ? fmtMB(c.usage_bytes) : '本机直跑模式';
    document.getElementById('kpi-cgroup-sub').textContent = c.available
      ? `上限 ${c.limit_bytes ? fmtMB(c.limit_bytes) : '无限制'} · 匿名内存 ${fmtMB(c.anonymous_bytes)}`
      : '部署到绿联 Docker 容器后显示';

    const ex = st.components.seven_zip;
    document.getElementById('kpi-7z').textContent = ex.available ? `7-Zip ${ex.version}` : '内建 ZIP/TAR';
    document.getElementById('kpi-7z-sub').textContent = ex.available ? '按需启动 · 空闲 0 MB' : '容器镜像默认内置 7-Zip 26.01';
    document.getElementById('kpi-up').textContent = fmtUptime(st.uptime_seconds);
    document.getElementById('kpi-up-sub').textContent = `数据库 ${st.database_ready ? '正常' : '异常'} · ${st.auth.no_auth_mode ? '内网免登录' : '令牌保护'}`;
    state.schedulerPaused = !!st.scheduler_paused;
    renderSchedulerBtn();
    document.getElementById('live-text').textContent = state.schedulerPaused
      ? '后台调度已暂停'
      : `同步于 ${new Date().toLocaleTimeString('zh-CN', { hour12: false })}`;
  } catch {
    document.getElementById('live-text').textContent = '连接中断';
  }
}

/* Task Center — Paginated Precision Table + Mobile Compact Cards + Detail Drawer */
function stateCategory(st) {
  if (st === 'completed') return 'completed';
  if (st.includes('fail')) return 'failed';
  if (st === 'wait_local_confirm' || st === 'submission_unknown' || st === 'cancelled') return 'attention';
  return 'active';
}

const SHORT_ERR_ZH = {
  REMOTE_OFFLINE_FAILED: '无源或死种下载失败',
  REMOTE_REJECTED: '115 拒绝提交',
  REMOTE_UNAVAILABLE: '115 连接异常',
  REMOTE_RATE_LIMIT: '触发限流已退避',
  REMOTE_CIRCUIT_OPEN: '风控熔断暂停中',
  AUTH_OR_CHALLENGE_REQUIRED: 'Cookie 已失效',
  PARTIAL_SUBMISSION_REQUIRES_RECONCILIATION: '提交超时待核对',
  SUBMISSION_NOT_FOUND: '未入列可重试',
  CLOUD_PREPARE_FAILED: '云预检失败',
  CLOUD_INSPECT_FAILED: '云分析失败',
  CLOUD_PASSWORD_FAILED: '云密码不匹配',
  CLOUD_EXTRACT_FAILED: '云解压失败，待转本地',
  CACHE_DOWNLOAD_FAILED: '直链缓存失败',
  LOCAL_EXTRACT_FAILED: '本地解压失败',
  PASSWORD_OR_CORRUPT_ARCHIVE: '密码错误或包损坏',
  ARCHIVE_DICT_EXCEEDS_MEMORY_LIMIT: '字典超内存上限已拦截',
  SEED_CLEANUP_FAILED: '下载成功，清理失败',
  SEED_RESOURCE_DIRECTORY_NOT_UNIQUE: '未定位唯一种子目录',
};

function zhShortReason(code, msg) {
  if (msg && msg.includes('ARCHIVE_DICT_EXCEEDS_MEMORY_LIMIT')) return '字典超内存上限已拦截';
  if (msg && msg.includes('PASSWORD_OR_CORRUPT_ARCHIVE')) return '密码错误或包损坏';
  if (msg && msg.includes('SEED_RESOURCE_DIRECTORY_NOT_UNIQUE')) return '未定位唯一种子目录';
  if (SHORT_ERR_ZH[code]) return SHORT_ERR_ZH[code];
  const full = zhError(code, msg);
  return full.length > 22 ? full.slice(0, 22) + '…' : full;
}

function stateInfo(st) {
  const map = {
    queued: ['排队中', 'badge-muted', 1],
    submitting: ['提交中', 'badge-info', 1],
    submitted: ['已提交', 'badge-info', 2],
    downloading: ['下载中', 'badge-info', 2],
    discover: ['找产物', 'badge-info', 2],
    seed_cleanup: ['清理中', 'badge-info', 3],
    cloud_prepare: ['云预检', 'badge-info', 3],
    cloud_inspect: ['分析中', 'badge-info', 3],
    cloud_export: ['云导出', 'badge-info', 3],
    cloud_progress: ['云解压中', 'badge-info', 3],
    wait_local_confirm: ['待转本地', 'badge-warn', 3],
    caching: ['缓存中', 'badge-info', 3],
    local_extract: ['本地解压', 'badge-info', 4],
    completed: ['已完成', 'badge-ok', 4],
    cancelled: ['已暂停', 'badge-warn', 1],
    failed: ['失败', 'badge-err', 4],
    submit_failed: ['提交失败', 'badge-err', 1],
    seed_cleanup_failed: ['清理失败', 'badge-err', 3],
    submission_unknown: ['待核对', 'badge-warn', 1],
  };
  return map[st] || ['处理中', 'badge-muted', 1];
}

function kindLabel(k) {
  return { offline: '115 离线', cloud_archive: '云端解压', local_archive: '本地监控' }[k] || '任务';
}

function renderStageDots(st) {
  const [, , idx] = stateInfo(st);
  const isErr = st.includes('fail');
  const isWarn = st === 'wait_local_confirm' || st === 'submission_unknown';
  const isDone = st === 'completed';
  let dots = '<span class="stage-dots" aria-hidden="true">';
  for (let i = 1; i <= 4; i++) {
    let cls = 'stage-dot';
    if (isDone || i < idx) cls += ' done';
    else if (i === idx) cls += isErr ? ' err' : (isWarn ? ' warn' : ' active');
    dots += `<span class="${cls}"></span>`;
  }
  return dots + '</span>';
}

function setTaskKind(kind, btn) {
  state.taskKind = kind;
  state.page = 1;
  document.querySelectorAll('#view-tasks .sub-tab').forEach((b) => b.classList.toggle('active', b === btn));
  renderPipelines();
}

function setStatFilter(cat, fromSelect = false) {
  state.statFilter = (!fromSelect && state.statFilter === cat) ? '' : cat;
  state.page = 1;
  document.getElementById('filter-state').value = state.statFilter;
  document.querySelectorAll('.stat-tile').forEach((t) => t.classList.toggle('active', t.dataset.stat === state.statFilter));
  renderPipelines();
}

function onSearchChange() { state.page = 1; renderPipelines(); }
function onPageSizeChange() {
  state.pageSize = Number(document.getElementById('page-size').value) || 20;
  state.page = 1;
  renderPipelines();
}
function changePage(delta) {
  state.page += delta;
  renderPipelines();
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

async function loadPipelines() {
  const data = await fetch('/api/v1/pipelines?limit=500').then((r) => r.json());
  state.pipelines = data.items || [];
  document.getElementById('nav-task-count').textContent = state.pipelines.length;
  renderPipelines();
}

function renderPipelines() {
  const byKind = state.pipelines.filter((p) => {
    if (state.taskKind === 'offline') return p.kind === 'offline';
    if (state.taskKind === 'extract') return p.kind === 'cloud_archive' || p.kind === 'local_archive';
    return true;
  });

  const counts = { active: 0, attention: 0, failed: 0, completed: 0 };
  byKind.forEach((p) => { counts[stateCategory(p.state)]++; });
  document.getElementById('st-active').textContent = counts.active;
  document.getElementById('st-attention').textContent = counts.attention;
  document.getElementById('st-failed').textContent = counts.failed;
  document.getElementById('st-completed').textContent = counts.completed;

  const q = (document.getElementById('task-search').value || '').trim().toLowerCase();
  const filtered = byKind.filter((p) => {
    if (state.statFilter && stateCategory(p.state) !== state.statFilter) return false;
    if (q && !String(p.name || '').toLowerCase().includes(q)) return false;
    return true;
  });

  const total = filtered.length;
  const totalPages = Math.max(1, Math.ceil(total / state.pageSize));
  if (state.page > totalPages) state.page = totalPages;
  if (state.page < 1) state.page = 1;
  const start = (state.page - 1) * state.pageSize;
  const pageItems = filtered.slice(start, start + state.pageSize);

  const tbody = document.getElementById('pipe-tbody');
  const mlist = document.getElementById('pipe-mobile');
  const emptyEl = document.getElementById('pipe-empty');
  const pager = document.getElementById('pipe-pager');

  if (!total) {
    tbody.innerHTML = '';
    mlist.innerHTML = '';
    emptyEl.style.display = 'block';
    pager.style.display = 'none';
    return;
  }

  emptyEl.style.display = 'none';
  pager.style.display = 'flex';
  document.getElementById('pager-info').textContent = `共 ${total} 条，当前显示第 ${start + 1}–${Math.min(start + pageItems.length, total)} 条`;
  document.getElementById('pager-num').textContent = `${state.page} / ${totalPages}`;
  document.getElementById('pg-prev').disabled = state.page <= 1;
  document.getElementById('pg-next').disabled = state.page >= totalPages;

  const trashSVG = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/></svg>`;

  // Desktop Precision Table (5 clean columns, zero duplication)
  tbody.innerHTML = pageItems.map((p) => {
    const [stText, stClass] = stateInfo(p.state);
    const shortReason = (p.error_code || p.error_message) ? zhShortReason(p.error_code, p.error_message) : '';
    const reasonCls = p.state.includes('fail') ? 'task-cell-err-inline' : 'task-cell-warn-inline';
    return `
      <tr>
        <td>
          <button type="button" class="task-cell-btn" title="${esc(p.name)}" onclick="openDetail('${p.id}')">
            <span class="task-cell-title">${esc(p.name)}</span>
            <span class="task-cell-sub">
              <span>${esc(kindLabel(p.kind))}</span>
              ${shortReason ? `<span>·</span><span class="${reasonCls}">${esc(shortReason)}</span>` : ''}
            </span>
          </button>
        </td>
        <td style="white-space:nowrap;">${renderStageDots(p.state)}<span class="badge ${stClass}">${esc(stText)}</span></td>
        <td class="mono-num">${fmtSize(p.size)}</td>
        <td class="mono-num">${fmtTime(p.created_at)}</td>
        <td style="text-align:right;">
          <div class="row-ops">
            ${p.state === 'wait_local_confirm' ? `<button type="button" class="btn btn-warn btn-sm" onclick="confirmLocal('${p.id}')">转本地</button>` : ''}
            ${(p.state.includes('fail') || p.state === 'cancelled') ? `<button type="button" class="btn btn-secondary btn-sm" onclick="retryPipe('${p.id}')">${p.state === 'cancelled' ? '继续' : '重试'}</button>` : ''}
            ${(p.state !== 'completed' && p.state !== 'cancelled' && !p.state.includes('fail')) ? `<button type="button" class="btn btn-secondary btn-sm" onclick="cancelPipe('${p.id}')">暂停</button>` : ''}
            <button type="button" class="icon-del" title="删除记录" aria-label="删除记录" onclick="deletePipe('${p.id}')">${trashSVG}</button>
          </div>
        </td>
      </tr>`;
  }).join('');

  // Mobile Compact Cards (tap row for detail sheet)
  mlist.innerHTML = pageItems.map((p) => {
    const [stText, stClass] = stateInfo(p.state);
    const shortReason = (p.error_code || p.error_message) ? zhShortReason(p.error_code, p.error_message) : '';
    const reasonCls = p.state.includes('fail') ? 'task-cell-err-inline' : 'task-cell-warn-inline';
    return `
      <div class="m-row">
        <div class="m-row-top" onclick="openDetail('${p.id}')">
          <div class="m-row-title">${esc(p.name)}</div>
          <span class="badge ${stClass}" style="flex-shrink:0;">${esc(stText)}</span>
        </div>
        <div class="m-row-meta" onclick="openDetail('${p.id}')">
          <span>${esc(kindLabel(p.kind))}</span>
          ${p.size ? `<span>· ${fmtSize(p.size)}</span>` : ''}
          <span>· ${fmtTime(p.created_at)}</span>
          ${shortReason ? `<span class="${reasonCls}">· ${esc(shortReason)}</span>` : ''}
        </div>
        ${(p.state !== 'completed') ? `
          <div class="m-row-actions">
            ${p.state === 'wait_local_confirm' ? `<button type="button" class="btn btn-warn btn-sm" onclick="confirmLocal('${p.id}')">转本地解压</button>` : ''}
            ${(p.state.includes('fail') || p.state === 'cancelled') ? `<button type="button" class="btn btn-secondary btn-sm" onclick="retryPipe('${p.id}')">${p.state === 'cancelled' ? '继续' : '重试'}</button>` : ''}
            ${(p.state !== 'cancelled' && !p.state.includes('fail')) ? `<button type="button" class="btn btn-secondary btn-sm" onclick="cancelPipe('${p.id}')">暂停</button>` : ''}
            <button type="button" class="btn btn-danger btn-sm" onclick="deletePipe('${p.id}')">删除</button>
          </div>` : ''}
      </div>`;
  }).join('');
}

/* Detail Drawer */
function renderStepper(pipe) {
  const [, , stepIdx] = stateInfo(pipe.state);
  const labels = pipe.kind === 'local_archive'
    ? ['发现稳定包', '安全预检', '本地解压', '发布完成']
    : ['任务登记', '离线与发现', '云解压/兜底', '处理完成'];
  const isErr = pipe.state.includes('fail');
  const isWarn = pipe.state === 'wait_local_confirm' || pipe.state === 'submission_unknown';
  const isDoneAll = pipe.state === 'completed';

  return `<div class="stepper">${labels.map((lb, idx) => {
    const n = idx + 1;
    let cls = 'step';
    if (isDoneAll || n < stepIdx) cls += ' done';
    else if (n === stepIdx) cls += isErr ? ' err' : (isWarn ? ' warn' : ' active');
    return `<div class="${cls}"><span>${lb}</span><span class="step-bar"></span></div>`;
  }).join('')}</div>`;
}

async function openDetail(id) {
  document.getElementById('drawer-mask').classList.add('open');
  document.getElementById('detail-drawer').classList.add('open');
  const body = document.getElementById('drawer-body');
  body.innerHTML = '<div class="field-help">正在加载任务详情...</div>';

  try {
    const d = await fetch(`/api/v1/pipelines/${encodeURIComponent(id)}`).then((r) => r.json());
    const p = d.item;
    const evs = d.events || [];
    const [stText, stClass] = stateInfo(p.state);
    const errText = (p.error_code || p.error_message) ? zhError(p.error_code, p.error_message) : '';

    body.innerHTML = `
      <div>
        <div style="font-size:15px;font-weight:700;word-break:break-word;margin-bottom:8px;">${esc(p.name)}</div>
        <div class="toolbar">
          <span class="badge badge-muted">${esc(kindLabel(p.kind))}</span>
          <span class="badge ${stClass}"><span class="badge-dot"></span>${esc(stText)}</span>
        </div>
      </div>

      ${renderStepper(p)}

      ${errText ? `<div class="alert show alert-err" style="margin:0;"><span>${esc(errText)}</span></div>` : ''}

      <div class="toolbar">
        ${p.state === 'wait_local_confirm' ? `<button type="button" class="btn btn-warn btn-sm" onclick="confirmLocal('${p.id}');closeDetail();">确认下载到本地解压</button>` : ''}
        ${(p.state.includes('fail') || p.state === 'cancelled') ? `<button type="button" class="btn btn-primary btn-sm" onclick="retryPipe('${p.id}');closeDetail();">${p.state === 'cancelled' ? '继续执行' : '立即重试'}</button>` : ''}
        ${(p.state !== 'completed' && p.state !== 'cancelled' && !p.state.includes('fail')) ? `<button type="button" class="btn btn-secondary btn-sm" onclick="cancelPipe('${p.id}');closeDetail();">暂停任务</button>` : ''}
        <button type="button" class="btn btn-danger btn-sm" onclick="deletePipe('${p.id}');closeDetail();">删除任务记录</button>
      </div>

      <div class="detail-kv">
        <div class="kv-item"><span>当前阶段</span><strong>${esc(zhStage(p.stage))}</strong></div>
        <div class="kv-item"><span>文件体积</span><strong>${fmtSize(p.size)}</strong></div>
        <div class="kv-item"><span>创建时间</span><strong>${fmtFullTime(p.created_at)}</strong></div>
        <div class="kv-item"><span>最近更新</span><strong>${fmtFullTime(p.updated_at)}</strong></div>
        ${p.target_date ? `<div class="kv-item"><span>日期目录</span><strong>${esc(p.target_date)}</strong></div>` : ''}
        ${p.target_cid ? `<div class="kv-item"><span>目标云目录</span><strong>${esc(p.target_cid)}</strong></div>` : ''}
      </div>

      <div>
        <div style="font-size:13px;font-weight:600;margin-bottom:10px;">处理时间线（共 ${evs.length} 条）</div>
        <div class="tl-list">
          ${evs.length ? evs.map((e) => `
            <div class="tl-item">
              <div class="tl-item-head">
                <span class="badge badge-muted">${esc(zhEventKind(e.kind))}</span>
                <span>${esc(zhStage(e.stage))}</span>
                <span>· ${fmtFullTime(e.created_at)}</span>
              </div>
              <div class="tl-item-msg">${esc(zhError('', e.message))}</div>
            </div>
          `).join('') : '<div class="field-help">暂无事件记录</div>'}
        </div>
      </div>`;
  } catch {
    body.innerHTML = '<div class="alert show alert-err"><span>加载任务详情失败，请稍后重试</span></div>';
  }
}

function closeDetail() {
  document.getElementById('drawer-mask').classList.remove('open');
  document.getElementById('detail-drawer').classList.remove('open');
}

async function confirmLocal(id) {
  const r = await fetch(`/api/v1/pipelines/${encodeURIComponent(id)}/confirm-local`, { method: 'POST' });
  if (r.ok) showToast('已批准本地兜底，进入直链缓存队列');
  else showAlert('tasks-alert', '操作失败：当前任务不处于待确认状态');
  loadPipelines();
}
async function retryPipe(id) {
  const r = await fetch(`/api/v1/pipelines/${encodeURIComponent(id)}/retry`, { method: 'POST' });
  if (r.ok) showToast('任务已重新加入处理队列');
  else showAlert('tasks-alert', '当前状态不可重试');
  loadPipelines();
}
async function cancelPipe(id) {
  const r = await fetch(`/api/v1/pipelines/${encodeURIComponent(id)}/cancel`, { method: 'POST' });
  if (r.ok) showToast('任务已暂停（可随时点“继续”恢复或直接删除）');
  else showAlert('tasks-alert', '当前任务状态不可暂停');
  loadPipelines();
}
async function deletePipe(id) {
  let r = await fetch(`/api/v1/pipelines/${encodeURIComponent(id)}`, { method: 'DELETE' });
  if (r.status === 409) {
    if (!confirm('该任务正在执行中，是否强制移除队列记录？（绝不会删除 115 云端或本地真实文件）')) return;
    r = await fetch(`/api/v1/pipelines/${encodeURIComponent(id)}?force=1`, { method: 'DELETE' });
  }
  if (r.ok) showToast('已移除任务记录（不会删除真实文件）');
  else showAlert('tasks-alert', '删除任务记录失败');
  loadPipelines();
}
function renderSchedulerBtn() {
  const btn = document.getElementById('sched-pause-btn');
  if (!btn) return;
  btn.textContent = state.schedulerPaused ? '恢复调度' : '暂停调度';
  btn.className = state.schedulerPaused ? 'btn btn-warn btn-sm' : 'btn btn-secondary btn-sm';
}
async function toggleScheduler(btn) {
  btn.disabled = true;
  try {
    const endpoint = state.schedulerPaused ? '/api/v1/scheduler/resume' : '/api/v1/scheduler/pause';
    const r = await fetch(endpoint, { method: 'POST' });
    if (r.ok) {
      const d = await r.json();
      state.schedulerPaused = !!d.scheduler_paused;
      renderSchedulerBtn();
      showToast(state.schedulerPaused ? '已暂停后台任务调度' : '已恢复后台任务调度');
      refreshTelemetry(true);
    }
  } finally { btn.disabled = false; }
}
async function triggerCloudScan(btn) {
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/monitor/cloud-scan', { method: 'POST' });
    const d = await r.json();
    if (r.ok) {
      showToast(`115 云监控目录扫描完成，新接管 ${d.adopted ?? 0} 个压缩包`);
      loadPipelines();
    } else {
      showAlert('tasks-alert', zhError(d.error?.code, d.error?.message));
    }
  } finally { btn.disabled = false; }
}
async function triggerLocalScan(btn) {
  btn.disabled = true;
  try {
    const d = await fetch('/api/v1/monitor/local-scan', { method: 'POST' }).then((r) => r.json());
    showToast(`本地目录扫描完成，新接管 ${d.adopted ?? 0} 个稳定压缩包`);
    loadPipelines();
  } finally { btn.disabled = false; }
}
async function triggerLifeCheck(btn) {
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/monitor/life-check', { method: 'POST' });
    const d = await r.json();
    if (r.ok) showToast(`生活事件检查完成，发现 ${d.archives_found ?? 0} 个压缩包`);
    else showAlert('tasks-alert', zhError(d.error?.code, d.error?.message));
  } finally { btn.disabled = false; }
}

/* Unified Offline Import */
function setSelectedFile(file) {
  state.selectedFile = file || null;
  const wrap = document.getElementById('file-chip-wrap');
  const nameEl = document.getElementById('file-chip-name');
  const submitBtn = document.getElementById('imp-submit-btn');
  if (!file) {
    wrap.style.display = 'none';
    submitBtn.textContent = '提交离线任务';
    return;
  }
  wrap.style.display = 'inline-flex';
  nameEl.textContent = `${file.name} (${(file.size / 1024).toFixed(1)} KB)`;
  submitBtn.textContent = file.name.toLowerCase().endsWith('.torrent') ? '导入种子' : '导入文本并提交';
}
function clearSelectedFile() {
  setSelectedFile(null);
  document.getElementById('imp-file').value = '';
}

async function submitUnifiedImport(btn) {
  hideAlert('import-alert');
  const text = document.getElementById('imp-text').value.trim();
  const autoExtract = document.getElementById('imp-auto').checked;
  const file = state.selectedFile;

  if (!text && !file) return showAlert('import-alert', '请粘贴链接，或点击「上传种子 / 文本」选择文件');
  btn.disabled = true;
  try {
    let r;
    if (file && file.name.toLowerCase().endsWith('.torrent')) {
      if (text) return showAlert('import-alert', '种子文件与粘贴链接请分别提交，避免混淆');
      const fd = new FormData();
      fd.append('file', file);
      fd.append('auto_extract', autoExtract ? 'true' : 'false');
      r = await fetch('/api/v1/offline/import', { method: 'POST', body: fd });
    } else {
      let combined = text;
      if (file) {
        const fileTxt = await file.text();
        combined = combined ? (combined + '\n' + fileTxt) : fileTxt;
      }
      r = await fetch('/api/v1/offline/import', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text: combined, auto_extract: autoExtract }),
      });
    }
    const d = await r.json();
    if (!r.ok) return showAlert('import-alert', zhError(d.error?.code, d.error?.message));
    document.getElementById('imp-text').value = '';
    clearSelectedFile();
    showToast(`导入已受理：新增 ${d.queued} 条 · 重复 ${d.duplicates} 条 · 无效 ${d.invalid} 条`);
  } finally { btn.disabled = false; }
}

/* Sources & Seed Rules */
function formatSourceConfig(src) {
  try {
    const c = JSON.parse(src.config_json || '{}');
    if (src.kind === 'local') return c.path || '-';
    return c.cid ? `云目录编号 ${c.cid}` : '-';
  } catch { return src.config_json || '-'; }
}

let cachedSources = [];
let editingSourceId = '';

async function loadSources() {
  const d = await fetch('/api/v1/sources').then((r) => r.json());
  const items = d.items || [];
  cachedSources = items;
  state.sourcesMap = {};
  items.forEach((s) => { state.sourcesMap[s.id] = s.name; });

  const tbody = document.getElementById('src-list');
  tbody.innerHTML = items.length ? items.map((s) => `
    <tr>
      <td><strong>${esc(s.name)}</strong></td>
      <td><span class="badge ${s.kind === 'local' ? 'badge-info' : 'badge-ok'}">${s.kind === 'local' ? '本地目录' : '115 云目录'}</span></td>
      <td><code>${esc(formatSourceConfig(s))}</code></td>
      <td>${s.bark_prefix ? `<code>${esc(s.bark_prefix)}</code>` : '<span class="field-help">未配置</span>'}</td>
      <td style="text-align:right;white-space:nowrap;">
        <button type="button" class="btn btn-secondary btn-sm" onclick="editSource('${s.id}')">编辑</button>
        <button type="button" class="btn btn-danger btn-sm" onclick="delSource('${s.id}')">删除</button>
      </td>
    </tr>
  `).join('') : '<tr><td colspan="5" class="field-help">暂无自定义来源（系统默认自动监控工作目录下的「本地监控」文件夹）</td></tr>';
}

function editSource(id) {
  const s = cachedSources.find((x) => x.id === id);
  if (!s) return;
  editingSourceId = s.id;
  document.getElementById('src-name').value = s.name || '';
  document.getElementById('src-kind').value = s.kind || 'local';
  try {
    const c = JSON.parse(s.config_json || '{}');
    document.getElementById('src-target').value = s.kind === 'local' ? (c.path || '') : (c.cid || '');
  } catch {
    document.getElementById('src-target').value = '';
  }
  document.getElementById('src-bark').value = s.bark_prefix || '';
  const sb = document.getElementById('src-submit-btn');
  const cb = document.getElementById('src-cancel-btn');
  if (sb) sb.textContent = '保存修改';
  if (cb) cb.style.display = 'inline-flex';
  document.getElementById('src-name').focus();
}

function cancelEditSource() {
  editingSourceId = '';
  document.getElementById('src-name').value = '';
  document.getElementById('src-target').value = '';
  document.getElementById('src-bark').value = '';
  const sb = document.getElementById('src-submit-btn');
  const cb = document.getElementById('src-cancel-btn');
  if (sb) sb.textContent = '添加监控来源';
  if (cb) cb.style.display = 'none';
}

async function loadArchiveRule() {
  try {
    const d = await fetch('/api/v1/settings/archive-rule').then((r) => r.json());
    if (document.getElementById('ar-orig')) document.getElementById('ar-orig').value = d.original_action || 'archive';
    if (document.getElementById('ar-fail')) document.getElementById('ar-fail').value = d.cloud_failure_action || 'confirm_local';
  } catch {}
}

async function saveArchiveRule(btn) {
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/settings/archive-rule', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        original_action: document.getElementById('ar-orig').value,
        cloud_failure_action: document.getElementById('ar-fail').value,
      }),
    });
    if (r.ok) showToast('解压与归档规则已保存');
    else showToast('规则保存失败');
  } finally { btn.disabled = false; }
}

async function addSource(btn) {
  hideAlert('src-alert');
  const name = document.getElementById('src-name').value.trim();
  const kind = document.getElementById('src-kind').value;
  const target = document.getElementById('src-target').value.trim();
  const bark_prefix = document.getElementById('src-bark').value.trim();
  if (!name || !target) return showAlert('src-alert', '请填写来源名称和监控路径 / 云目录编号');
  btn.disabled = true;
  try {
    const cfg = kind === 'local' ? { path: target, squash_root: true, disable_recursion: true } : { cid: target };
    const payload = { name, kind, enabled: true, bark_prefix, config_json: JSON.stringify(cfg) };
    if (editingSourceId) payload.id = editingSourceId;
    const r = await fetch('/api/v1/sources', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!r.ok) return showAlert('src-alert', '保存监控来源失败');
    const wasEdit = !!editingSourceId;
    cancelEditSource();
    showToast(wasEdit ? '监控来源已更新' : '监控来源已添加');
    loadSources();
  } finally { btn.disabled = false; }
}

async function delSource(id) {
  await fetch('/api/v1/sources/' + encodeURIComponent(id), { method: 'DELETE' });
  showToast('已删除监控来源（保留真实文件）');
  loadSources();
}

async function loadSeedRule() {
  const d = await fetch('/api/v1/rules?scope=seed').then((r) => r.json());
  const item = (d.items || [])[0];
  if (!item) return;
  const c = JSON.parse(item.config_json || '{}');
  document.getElementById('seed-en').checked = !!c.enabled;
  document.getElementById('seed-empty').checked = c.empty_dirs !== false;
  document.getElementById('seed-auto-ext').checked = !!c.auto_extract;
  document.getElementById('seed-mode').value = c.mode || 'exclude';
  document.getElementById('seed-scope').value = c.scope || 'path';
  document.getElementById('seed-exts').value = (c.extensions || []).join(', ');
  document.getElementById('seed-kws').value = (c.keywords || []).join(', ');
  document.getElementById('seed-min').value = c.minimum != null ? Math.round(c.minimum / 1048576) : '';
  document.getElementById('seed-max').value = c.maximum != null ? Math.round(c.maximum / 1048576) : '';
}

async function saveSeedRule(btn) {
  btn.disabled = true;
  try {
    const minStr = document.getElementById('seed-min').value.trim();
    const maxStr = document.getElementById('seed-max').value.trim();
    const cfg = {
      enabled: document.getElementById('seed-en').checked,
      dry_run: false,
      empty_dirs: document.getElementById('seed-empty').checked,
      auto_extract: document.getElementById('seed-auto-ext').checked,
      mode: document.getElementById('seed-mode').value,
      scope: document.getElementById('seed-scope').value,
      extensions: document.getElementById('seed-exts').value.split(/[,，\s]+/).map((x) => x.trim()).filter(Boolean),
      keywords: document.getElementById('seed-kws').value.split(/[,，]+/).map((x) => x.trim()).filter(Boolean),
      minimum: minStr !== '' ? Math.round(Number(minStr) * 1048576) : null,
      maximum: maxStr !== '' ? Math.round(Number(maxStr) * 1048576) : null,
    };
    const r = await fetch('/api/v1/rules', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: 'seed-default', name: '种子下载清理规则', scope: 'seed', enabled: true, config_json: JSON.stringify(cfg) }),
    });
    if (r.ok) showToast('种子清理规则已保存');
  } finally { btn.disabled = false; }
}

/* Logs (100% Chinese) */
async function loadLogs() {
  const [ev, bk, src] = await Promise.all([
    fetch('/api/v1/events?limit=80').then((r) => r.json()),
    fetch('/api/v1/bark/messages?limit=50').then((r) => r.json()),
    fetch('/api/v1/sources').then((r) => r.json()),
  ]);
  const srcName = {};
  (src.items || []).forEach((s) => { srcName[s.id] = s.name; });

  const evItems = ev.items || [];
  document.getElementById('ev-list').innerHTML = evItems.length ? evItems.map((e) => {
    const cls = e.kind.includes('FAIL') ? 'badge-err' : (e.kind.includes('SUCC') || e.kind.includes('COMPLETED')) ? 'badge-ok' : e.kind.includes('WAIT') || e.kind.includes('UNKNOWN') ? 'badge-warn' : 'badge-muted';
    return `
      <tr>
        <td class="mono-num">${fmtTime(e.created_at)}</td>
        <td><span class="badge ${cls}">${esc(zhEventKind(e.kind))}</span></td>
        <td style="white-space:nowrap;">${esc(zhStage(e.stage))}</td>
        <td>${esc(zhError('', e.message))}</td>
        <td><span class="badge badge-muted">${esc(zhNotifyStatus(e.notify_status))}</span></td>
      </tr>`;
  }).join('') : '<tr><td colspan="5" class="field-help">暂无运行事件</td></tr>';

  const bkItems = bk.items || [];
  document.getElementById('bark-list').innerHTML = bkItems.length ? bkItems.map((b) => `
    <tr>
      <td class="mono-num">${fmtTime(b.created_at)}</td>
      <td><strong>${esc(b.title || '无标题')}</strong></td>
      <td style="white-space:pre-line;">${esc(b.body || '-')}</td>
      <td>${b.matched_source_id ? `<span class="badge badge-ok">${esc(srcName[b.matched_source_id] || '已匹配云目录')}</span>` : '<span class="badge badge-muted">未命中前缀</span>'}</td>
      <td>${b.status === 'dispatched' ? '<span class="badge badge-ok">已触发云扫描</span>' : '<span class="badge badge-info">90 秒窗口合并中</span>'}</td>
    </tr>
  `).join('') : '<tr><td colspan="5" class="field-help">尚未收到 Bark 联动信号</td></tr>';
}

/* Settings */
async function loadP115() {
  const d = await fetch('/api/v1/settings/p115').then((r) => r.json());
  document.getElementById('p115-life').checked = !!d.life_monitor_enabled;
  const depthEl = document.getElementById('p115-scan-depth');
  if (depthEl) depthEl.value = String(d.cloud_scan_depth || 1);
  const rootInput = document.getElementById('p115-root');
  if (d.cloud_root_cid || !rootInput.value.trim()) {
    rootInput.value = d.cloud_root_cid || '';
  }
  document.getElementById('p115-status').innerHTML = `
    <span class="badge ${d.has_cookie ? 'badge-ok' : 'badge-warn'}">${d.has_cookie ? '已保存加密凭据' : '未配置凭据'}</span>
    <span class="field-help">离线下载=${esc(d.offline_root_cid || '未设')} · 解压输出=${esc(d.cloud_output_cid || '未设')} · 归档区=${esc(d.cloud_archive_zone_cid || d.cloud_archive_cid || '未设')}</span>`;
}
async function saveP115(btn) {
  hideAlert('p115-alert');
  btn.disabled = true;
  try {
    const depthEl = document.getElementById('p115-scan-depth');
    const r = await fetch('/api/v1/settings/p115', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        cookie: document.getElementById('p115-cookie').value.trim(),
        cloud_root_cid: document.getElementById('p115-root').value.trim(),
        life_monitor_enabled: document.getElementById('p115-life').checked,
        cloud_scan_depth: depthEl ? Number(depthEl.value) || 1 : 1,
      }),
    });
    const d = await r.json().catch(() => ({}));
    document.getElementById('p115-cookie').value = '';
    if (!r.ok) return showAlert('p115-alert', zhError(d.error?.code, d.error?.message) || '保存 115 配置失败');
    showToast('115 连接配置已加密保存');
    loadP115();
  } finally { btn.disabled = false; }
}
async function initCloudRoot(btn) {
  hideAlert('p115-alert');
  const root_cid = document.getElementById('p115-root').value.trim();
  if (!root_cid) return showAlert('p115-alert', '请先输入云根目录编号（CID，网盘根目录可填 0）');
  btn.disabled = true;
  try {
    // If the user pasted a Cookie without clicking Save first, save it automatically before initializing.
    const pendingCookie = document.getElementById('p115-cookie').value.trim();
    if (pendingCookie) {
      const sr = await fetch('/api/v1/settings/p115', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          cookie: pendingCookie,
          cloud_root_cid: root_cid,
          life_monitor_enabled: document.getElementById('p115-life').checked,
        }),
      });
      if (!sr.ok) {
        const sd = await sr.json().catch(() => ({}));
        return showAlert('p115-alert', zhError(sd.error?.code, sd.error?.message) || '保存 115 凭据失败');
      }
      document.getElementById('p115-cookie').value = '';
    }
    const r = await fetch('/api/v1/settings/p115/init-root', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ root_cid }),
    });
    const d = await r.json();
    if (!r.ok) return showAlert('p115-alert', zhError(d.error?.code, d.error?.message));
    showToast('已初始化「离线下载 / 解压输出 / 归档区（原包留存、异常待查、历史归档）」');
    loadP115();
  } finally { btn.disabled = false; }
}

async function loadTelegram() {
  const d = await fetch('/api/v1/settings/telegram').then((r) => r.json());
  document.getElementById('tg-chat').value = d.chat_id || '';
  document.getElementById('tg-base').value = d.api_base || '';
  document.getElementById('tg-proxy').value = d.proxy_url || '';
  document.getElementById('tg-mode').value = d.mode || 'result';
  document.getElementById('tg-enabled').checked = !!d.enabled;
  const menuEl = document.getElementById('tg-bot-menu');
  if (menuEl) menuEl.checked = d.bot_menu_enabled !== false;
  document.getElementById('tg-badge').textContent = d.has_bot_token ? '已保存加密令牌' : '未配置令牌';
  document.getElementById('tg-badge').className = 'badge ' + (d.has_bot_token ? 'badge-ok' : 'badge-muted');
}
async function saveTelegram(btn) {
  hideAlert('tg-alert');
  btn.disabled = true;
  try {
    const menuEl = document.getElementById('tg-bot-menu');
    const r = await fetch('/api/v1/settings/telegram', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: document.getElementById('tg-enabled').checked,
        bot_menu_enabled: menuEl ? menuEl.checked : true,
        bot_token: document.getElementById('tg-token').value.trim(),
        chat_id: document.getElementById('tg-chat').value.trim(),
        api_base: document.getElementById('tg-base').value.trim(),
        proxy_url: document.getElementById('tg-proxy').value.trim(),
        mode: document.getElementById('tg-mode').value,
      }),
    });
    document.getElementById('tg-token').value = '';
    if (!r.ok) return showAlert('tg-alert', 'Telegram 配置保存失败');
    showToast('Telegram 通知配置已保存');
    loadTelegram();
  } finally { btn.disabled = false; }
}
async function testTelegram(btn) {
  hideAlert('tg-alert');
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/settings/telegram/test', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) return showAlert('tg-alert', zhError(d.error?.code, d.error?.message));
    showToast('测试消息已发出，请查收 Telegram');
  } finally { btn.disabled = false; }
}

async function loadWebhook() {
  const d = await fetch('/api/v1/settings/webhook').then((r) => r.json());
  document.getElementById('wh-url').value = d.url || '';
  document.getElementById('wh-mode').value = d.mode || 'result';
  document.getElementById('wh-enabled').checked = !!d.enabled;
}
async function saveWebhook(btn) {
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/settings/webhook', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        enabled: document.getElementById('wh-enabled').checked,
        url: document.getElementById('wh-url').value.trim(),
        secret_header: document.getElementById('wh-secret').value.trim(),
        mode: document.getElementById('wh-mode').value,
      }),
    });
    document.getElementById('wh-secret').value = '';
    if (r.ok) showToast('通用回调通知配置已保存');
  } finally { btn.disabled = false; }
}

async function loadPasswords() {
  const d = await fetch('/api/v1/settings/passwords').then((r) => r.json());
  document.getElementById('pwd-list').value = (d.items || []).join('\n');
}
async function savePasswords(btn) {
  btn.disabled = true;
  try {
    const items = document.getElementById('pwd-list').value.split(/\r?\n/).map((x) => x.trim()).filter(Boolean);
    const r = await fetch('/api/v1/settings/passwords', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ items }),
    });
    if (r.ok) showToast(`已加密保存 ${items.length} 条解压密码`);
  } finally { btn.disabled = false; }
}

async function loadTokens() {
  document.getElementById('api-import-endpoint').textContent = `${location.origin}/api/v1/offline/import`;
  document.getElementById('bark-endpoint').textContent = `${location.origin}/bark`;
  const d = await fetch('/api/v1/tokens').then((r) => r.json());
  const items = d.items || [];
  document.getElementById('tok-list').innerHTML = items.length ? items.map((t) => `
    <tr>
      <td><strong>${esc(t.name)}</strong></td>
      <td><span class="badge ${t.role === 'admin' ? 'badge-ok' : 'badge-info'}">${esc(zhRole(t.role))}</span></td>
      <td><code>${esc(t.prefix)}...</code></td>
      <td>${esc(t.note || '-')}</td>
      <td style="text-align:right;"><button type="button" class="btn btn-danger btn-sm" onclick="delToken('${t.id}')">撤销</button></td>
    </tr>
  `).join('') : '<tr><td colspan="5" class="field-help">暂无接口令牌</td></tr>';
}
async function createToken(btn) {
  hideAlert('tok-alert');
  const name = document.getElementById('tok-name').value.trim();
  if (!name) return showAlert('tok-alert', '请输入令牌名称');
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/tokens', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name,
        note: document.getElementById('tok-note').value.trim(),
        role: document.getElementById('tok-role').value,
      }),
    });
    const d = await r.json();
    if (!r.ok) return showAlert('tok-alert', zhError(d.error?.code, d.error?.message));
    document.getElementById('new-token-wrap').style.display = 'flex';
    document.getElementById('new-token-val').textContent = d.token;
    document.getElementById('tok-name').value = '';
    document.getElementById('tok-note').value = '';
    showToast('新令牌已生成（仅展示这一次，请及时保存）');
    loadTokens();
  } finally { btn.disabled = false; }
}
async function delToken(id) {
  await fetch('/api/v1/tokens/' + encodeURIComponent(id), { method: 'DELETE' });
  showToast('接口令牌已撤销');
  loadTokens();
}

/* Organize Center — Async 5-in-1 Scan, Live Logs, Custom CID/Path Targets, Batch Exec */
const ocState = {
  toolTab: 'all',
  plan: null,
  checkedIds: new Set(),
  pollTimer: null,
  lastPlanTs: 0,
};

const OC_TOOL_LABEL = {
  date_archive: ['日期归档', 'badge-info'],
  rename: ['洗名改名', 'badge-info'],
  duplicate: ['SHA1查重', 'badge-warn'],
  junk: ['垃圾清理', 'badge-err'],
  classify: ['文件归类', 'badge-ok'],
  flatten: ['套娃解包', 'badge-ok'],
  empty_dir: ['空目录', 'badge-muted'],
};

let ocWorkbenchCfg = null;

async function loadOrganizeCenter() {
  try {
    const [srcResp, customResp, st, wb] = await Promise.all([
      fetch('/api/v1/sources').then((r) => r.json()).catch(() => ({ items: [] })),
      fetch('/api/v1/organizer/targets').then((r) => r.json()).catch(() => ({ items: [] })),
      fetch('/api/v1/organizer/center/state').then((r) => r.json()).catch(() => null),
      fetch('/api/v1/organizer/workbench').then((r) => r.json()).catch(() => null),
    ]);
    if (wb) {
      ocWorkbenchCfg = wb;
      applyWorkbenchToUI(wb);
    }
    const prevChecked = new Set(Array.from(document.querySelectorAll('.oc-target-cb:checked')).map((e) => e.value));
    const hasPrev = document.querySelectorAll('.oc-target-cb').length > 0;

    const sysList = [
      { key: 'sys:cloud:offline', label: '[115] 离线下载', def: true },
      { key: 'sys:cloud:output', label: '[115] 解压输出', def: true },
      { key: 'sys:cloud:archive', label: '[115] 原包留存', def: true },
      { key: 'sys:local:output', label: '[本地] 解压输出', def: true },
      { key: 'sys:local:archive', label: '[本地] 原包留存', def: false },
      { key: 'sys:local:watch', label: '[本地] 本地监控', def: false },
    ];
    const sources = (srcResp.items || []).map((s) => ({
      key: 'src:' + s.id,
      label: `[${s.kind === 'cloud' ? '115监控' : '本地监控'}] ${s.name}`,
      def: false,
    }));
    const customs = (customResp.items || []).map((c) => ({
      key: 'custom:' + c.id,
      label: `[${c.kind === 'cloud' ? '115自定义' : '本地自定义'}] ${c.name} (${c.depth || 1}层)`,
      def: true,
    }));

    const allTargets = [...sysList, ...sources, ...customs];
    const wrap = document.getElementById('oc-target-checks');
    if (wrap) {
      wrap.innerHTML = allTargets.map((t) => {
        const checked = hasPrev ? prevChecked.has(t.key) : t.def;
        return `<label class="check-label"><input type="checkbox" class="oc-target-cb" value="${esc(t.key)}" ${checked ? 'checked' : ''} /> ${esc(t.label)}</label>`;
      }).join('');
    }

    const customWrap = document.getElementById('oc-custom-list');
    if (customWrap) {
      const items = customResp.items || [];
      customWrap.innerHTML = items.length ? `
        <div class="table-wrap"><table>
          <thead><tr><th>名称</th><th>类型</th><th>CID / 路径</th><th>深度</th><th style="text-align:right;">操作</th></tr></thead>
          <tbody>${items.map((c) => `
            <tr>
              <td><strong>${esc(c.name)}</strong></td>
              <td><span class="badge ${c.kind === 'cloud' ? 'badge-ok' : 'badge-info'}">${c.kind === 'cloud' ? '115 云' : '本地'}</span></td>
              <td><code>${esc(c.target)}</code></td>
              <td>${c.depth || 1} 层</td>
              <td style="text-align:right;"><button type="button" class="btn btn-danger btn-sm" onclick="deleteOrganizeTarget('${esc(c.id)}')">删除</button></td>
            </tr>`).join('')}
          </tbody>
        </table></div>` : '<div class="field-help">尚未保存自定义整理目录。</div>';
    }

    if (st) renderCenterState(st);
  } catch {}
}

async function addOrganizeTarget(btn) {
  const name = document.getElementById('ot-name').value.trim();
  const kind = document.getElementById('ot-kind').value;
  const target = document.getElementById('ot-target').value.trim();
  const depth = Number(document.getElementById('ot-depth').value) || 1;
  if (!name || !target) return showToast('请填写目录名称与 CID 或路径');
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/organizer/targets', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, kind, target, depth }),
    });
    if (!r.ok) return showToast('保存自定义目标失败');
    document.getElementById('ot-name').value = '';
    document.getElementById('ot-target').value = '';
    showToast('已保存整理目标：' + name);
    loadOrganizeCenter();
  } finally { btn.disabled = false; }
}

async function deleteOrganizeTarget(id) {
  await fetch('/api/v1/organizer/targets/' + encodeURIComponent(id), { method: 'DELETE' });
  showToast('已删除自定义目标');
  loadOrganizeCenter();
}

function applyWorkbenchToUI(wb) {
  const setVal = (id, v) => { const el = document.getElementById(id); if (el) el.value = v ?? ''; };
  const setChk = (id, v) => { const el = document.getElementById(id); if (el) el.checked = !!v; };
  setVal('oc-junk-exts', wb.junk_exts);
  setVal('oc-junk-kws', wb.junk_keywords);
  setVal('oc-junk-kwmb', wb.junk_keyword_max_mb ?? 20);
  setVal('oc-junk-mb', wb.junk_max_mb ?? 0);
  setVal('oc-junk-safe', wb.junk_safe_exts);
  setVal('oc-dup-policy', wb.dedup_keep_policy || 'cleanest');
  setChk('oc-ren-ads', wb.rename_clean_ads !== false);
  setVal('oc-ren-find', wb.rename_find);
  setVal('oc-ren-replace', wb.rename_replace);
  setChk('oc-ren-regex', wb.rename_regex);
  setVal('oc-ren-prefix', wb.rename_prefix);
  setVal('oc-cls-mode', wb.classify_mode || 'type');
  const blInfo = document.getElementById('oc-sha1-bl-info');
  if (blInfo) {
    const n = (wb.junk_sha1_blacklist || []).length;
    blInfo.textContent = n ? `已拉黑 ${n} 条广告 SHA1` : '';
  }
}

function collectWorkbenchPayload() {
  return {
    junk_exts: document.getElementById('oc-junk-exts')?.value.trim() || '',
    junk_keywords: document.getElementById('oc-junk-kws')?.value.trim() || '',
    junk_keyword_max_mb: Number(document.getElementById('oc-junk-kwmb')?.value) || 0,
    junk_max_mb: Number(document.getElementById('oc-junk-mb')?.value) || 0,
    junk_safe_exts: document.getElementById('oc-junk-safe')?.value.trim() || '',
    junk_sha1_blacklist: ocWorkbenchCfg?.junk_sha1_blacklist || [],
    dedup_keep_policy: document.getElementById('oc-dup-policy')?.value || 'cleanest',
    rename_clean_ads: !!document.getElementById('oc-ren-ads')?.checked,
    rename_find: document.getElementById('oc-ren-find')?.value || '',
    rename_replace: document.getElementById('oc-ren-replace')?.value || '',
    rename_regex: !!document.getElementById('oc-ren-regex')?.checked,
    rename_prefix: document.getElementById('oc-ren-prefix')?.value || '',
    classify_mode: document.getElementById('oc-cls-mode')?.value || 'type',
  };
}

async function saveWorkbenchRules(btn) {
  if (btn) btn.disabled = true;
  try {
    const r = await fetch('/api/v1/organizer/workbench', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(collectWorkbenchPayload()),
    });
    if (r.ok) {
      ocWorkbenchCfg = await r.json();
      applyWorkbenchToUI(ocWorkbenchCfg);
      if (btn) showToast('整理规则已保存');
    }
  } finally { if (btn) btn.disabled = false; }
}

async function blacklistItemSHA1(sha1, btn) {
  if (!sha1) return;
  if (btn) btn.disabled = true;
  try {
    const r = await fetch('/api/v1/organizer/workbench/blacklist-sha1', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sha1 }),
    });
    if (r.ok) {
      ocWorkbenchCfg = await r.json();
      applyWorkbenchToUI(ocWorkbenchCfg);
      showToast('已加入广告 SHA1 黑名单，后续扫描自动拦截');
    }
  } finally { if (btn) btn.disabled = false; }
}

async function undoCenterRename(btn) {
  if (!confirm('确认撤销上一次批量改名，恢复所有原文件名？')) return;
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/organizer/center/undo-rename', { method: 'POST' });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) return showToast(zhError(d.error?.code, d.error?.message) || '撤销失败');
    showToast(`已恢复 ${d.restored || 0} 个原文件名`);
    pollCenterState();
  } finally { btn.disabled = false; }
}

async function startCenterScan(btn) {
  const target_keys = Array.from(document.querySelectorAll('.oc-target-cb:checked')).map((el) => el.value);
  if (!target_keys.length) return showToast('请先至少勾选一个要扫描的目录');
  btn.disabled = true;
  try {
    const wb = collectWorkbenchPayload();
    await fetch('/api/v1/organizer/workbench', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(wb),
    }).catch(() => {});
    const r = await fetch('/api/v1/organizer/center/scan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        target_keys,
        enable_date_archive: document.getElementById('oc-tool-date').checked,
        enable_rename: document.getElementById('oc-tool-rename').checked,
        enable_duplicate: document.getElementById('oc-tool-dup').checked,
        enable_junk: document.getElementById('oc-tool-junk').checked,
        enable_classify: document.getElementById('oc-tool-cls').checked,
        enable_flatten: document.getElementById('oc-tool-flat').checked,
        enable_empty_dir: document.getElementById('oc-tool-empty').checked,
        ...wb,
      }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) return showToast(zhError(d.error?.code, d.error?.message) || '启动扫描失败');
    showToast('后台扫描已启动');
    startCenterPolling();
  } finally { btn.disabled = false; }
}

async function startCenterExec(btn) {
  const ids = Array.from(ocState.checkedIds);
  if (!ids.length) return showToast('请先勾选要执行的条目');
  if (!confirm(`确认执行已勾选的 ${ids.length} 项整理操作？（115 删除默认移入回收站，批量 50 项防风控）`)) return;
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/organizer/center/exec', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ item_ids: ids }),
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) return showToast(zhError(d.error?.code, d.error?.message) || '启动执行失败');
    showToast('后台批量整理已启动');
    startCenterPolling();
  } finally { btn.disabled = false; }
}

async function cancelCenterJob(btn) {
  btn.disabled = true;
  try {
    await fetch('/api/v1/organizer/center/cancel', { method: 'POST' });
    showToast('已发送停止指令');
  } finally { btn.disabled = false; }
}

function startCenterPolling() {
  clearInterval(ocState.pollTimer);
  pollCenterState();
  ocState.pollTimer = setInterval(pollCenterState, 1500);
}

async function pollCenterState() {
  try {
    const st = await fetch('/api/v1/organizer/center/state').then((r) => r.json());
    renderCenterState(st);
    if (st.status !== 'running') {
      clearInterval(ocState.pollTimer);
      ocState.pollTimer = null;
    }
  } catch {}
}

function renderCenterState(st) {
  const wrap = document.getElementById('oc-progress-wrap');
  const cancelBtn = document.getElementById('oc-cancel-btn');
  const scanBtn = document.getElementById('oc-scan-btn');
  if (!wrap) return;

  const active = st.status === 'running';
  if (st.status && st.status !== 'idle') {
    wrap.style.display = 'block';
    document.getElementById('oc-stage-label').textContent = st.stage_label || '处理中…';
    const badge = document.getElementById('oc-status-badge');
    const stMap = { running: ['运行中', 'badge-info'], done: ['已完成', 'badge-ok'], cancelled: ['已停止', 'badge-warn'], failed: ['失败', 'badge-err'] };
    const [lbl, cls] = stMap[st.status] || ['空闲', 'badge-muted'];
    badge.textContent = lbl;
    badge.className = 'badge ' + cls;
    const pct = st.progress_total > 0 ? Math.min(100, Math.round((st.progress_cur / st.progress_total) * 100)) : (active ? 15 : 100);
    document.getElementById('oc-progress-fill').style.width = pct + '%';
    const logEl = document.getElementById('oc-logs');
    logEl.textContent = (st.logs || []).join('\n');
    logEl.scrollTop = logEl.scrollHeight;
  }
  if (cancelBtn) cancelBtn.style.display = active ? 'inline-flex' : 'none';
  if (scanBtn) scanBtn.disabled = active;
  if (active && !ocState.pollTimer) startCenterPolling();

  const planPanel = document.getElementById('oc-plan-panel');
  if (st.plan && Array.isArray(st.plan.items)) {
    ocState.plan = st.plan;
    if (st.plan.generated_at && st.plan.generated_at !== ocState.lastPlanTs) {
      ocState.lastPlanTs = st.plan.generated_at;
      ocState.checkedIds = new Set(st.plan.items.map((x) => x.id));
    } else {
      const validIds = new Set(st.plan.items.map((x) => x.id));
      for (const id of Array.from(ocState.checkedIds)) {
        if (!validIds.has(id)) ocState.checkedIds.delete(id);
      }
    }
    planPanel.style.display = 'block';
    const undoBtn = document.getElementById('oc-undo-btn');
    if (undoBtn) {
      undoBtn.style.display = st.undo_rename_count > 0 ? 'inline-flex' : 'none';
      undoBtn.textContent = `↩️ 撤销改名 (${st.undo_rename_count || 0})`;
    }
    const counts = st.plan.counts || {};
    document.getElementById('oc-cnt-all').textContent = st.plan.items.length;
    ['date_archive', 'rename', 'duplicate', 'junk', 'classify', 'flatten', 'empty_dir'].forEach((k) => {
      const el = document.getElementById('oc-cnt-' + k);
      if (el) el.textContent = counts[k] || 0;
    });
    renderCenterPlanTable();
  }
}

function switchCenterToolTab(tool, btn) {
  ocState.toolTab = tool;
  document.querySelectorAll('#oc-tool-tabs .sub-tab').forEach((b) => b.classList.toggle('active', b === btn));
  renderCenterPlanTable();
}

function toggleCenterItem(id, checked) {
  if (checked) ocState.checkedIds.add(id);
  else ocState.checkedIds.delete(id);
  updateCenterExecBtn();
}

function toggleCenterAll(checked) {
  if (!ocState.plan) return;
  const items = ocState.plan.items.filter((x) => ocState.toolTab === 'all' || x.tool === ocState.toolTab);
  items.forEach((x) => {
    if (checked) ocState.checkedIds.add(x.id);
    else ocState.checkedIds.delete(x.id);
  });
  renderCenterPlanTable();
}

function toggleOcRuleFields() {
  const showRen = !!document.getElementById('oc-tool-rename')?.checked;
  const showDup = !!document.getElementById('oc-tool-dup')?.checked;
  const showJunk = !!document.getElementById('oc-tool-junk')?.checked;
  const showCls = !!document.getElementById('oc-tool-cls')?.checked;
  const wrap = document.getElementById('oc-extra-opts');
  if (wrap) wrap.style.display = (showRen || showDup || showJunk || showCls) ? 'block' : 'none';
  const show = (id, on) => { const el = document.getElementById(id); if (el) el.style.display = on ? 'flex' : 'none'; };
  show('oc-ren-ads-wrap', showRen);
  show('oc-ren-pref-wrap', showRen);
  show('oc-ren-find-wrap', showRen);
  show('oc-ren-rep-wrap', showRen);
  show('oc-dup-policy-wrap', showDup);
  show('oc-junk-exts-wrap', showJunk);
  show('oc-junk-kws-wrap', showJunk);
  show('oc-junk-kwmb-wrap', showJunk);
  show('oc-junk-mb-wrap', showJunk);
  show('oc-junk-safe-wrap', showJunk);
  show('oc-cls-mode-wrap', showCls);
}

function updateCenterExecBtn() {
  const btn = document.getElementById('oc-exec-btn');
  if (btn) btn.textContent = `执行 ${ocState.checkedIds.size} 项`;
}

function renderCenterPlanTable() {
  const tbody = document.getElementById('oc-plan-list');
  if (!tbody || !ocState.plan) return;
  const items = ocState.plan.items.filter((x) => ocState.toolTab === 'all' || x.tool === ocState.toolTab);
  tbody.innerHTML = items.length ? items.map((it) => {
    const [tlbl, tcls] = OC_TOOL_LABEL[it.tool] || [it.tool, 'badge-muted'];
    const checked = ocState.checkedIds.has(it.id) ? 'checked' : '';
    const blBtn = it.sha1 && (it.tool === 'junk' || it.tool === 'duplicate')
      ? ` <button type="button" class="btn btn-ghost btn-sm" style="padding:1px 6px;font-size:11px;" onclick="blacklistItemSHA1('${esc(it.sha1)}', this)">拉黑SHA1</button>`
      : '';
    return `
      <tr>
        <td><input type="checkbox" ${checked} onchange="toggleCenterItem('${esc(it.id)}', this.checked)" /></td>
        <td><span class="badge ${tcls}">${tlbl}</span></td>
        <td><span class="badge ${it.scope === 'cloud' ? 'badge-ok' : 'badge-info'}">${it.scope === 'cloud' ? '115' : '本地'}</span> ${esc(it.target_name)}</td>
        <td><strong>${esc(it.name)}</strong>${it.size ? ` <span class="field-help">(${fmtSize(it.size)})</span>` : ''}</td>
        <td>${esc(it.detail)}${blBtn}</td>
        <td><code>${esc(it.dest_label)}</code></td>
      </tr>`;
  }).join('') : '<tr><td colspan="6" class="field-help">当前分类下暂无待整理项</td></tr>';
  updateCenterExecBtn();
}

/* Date-Folder Organizer */
function toggleRetentionDays() {
  const mode = document.getElementById('org-mode')?.value || 'current_month';
  const wrap = document.getElementById('org-days-wrap');
  if (wrap) wrap.style.display = mode === 'days' ? 'flex' : 'none';
}

async function loadOrganizerConfig() {
  try {
    const [d, srcResp] = await Promise.all([
      fetch('/api/v1/organizer/config').then((r) => r.json()),
      fetch('/api/v1/sources').then((r) => r.json()).catch(() => ({ items: [] })),
    ]);
    if (!document.getElementById('org-mode')) return;
    document.getElementById('org-mode').value = d.retention_mode || 'current_month';
    document.getElementById('org-days').value = d.retention_days || 14;
    const hasCloudCats = Array.isArray(d.cloud_categories);
    const cloudCats = hasCloudCats ? d.cloud_categories : (d.include_cloud !== false ? ['offline', 'output', 'archive'] : []);
    document.getElementById('org-c-offline').checked = cloudCats.includes('offline');
    document.getElementById('org-c-output').checked = cloudCats.includes('output');
    document.getElementById('org-c-archive').checked = cloudCats.includes('archive');

    const hasLocalCats = Array.isArray(d.local_categories);
    const localCats = hasLocalCats ? d.local_categories : (d.include_local !== false ? ['output', 'archive'] : []);
    document.getElementById('org-l-watch').checked = localCats.includes('watch');
    document.getElementById('org-l-output').checked = localCats.includes('output');
    document.getElementById('org-l-archive').checked = localCats.includes('archive');

    const extraWrap = document.getElementById('org-extra-sources');
    if (extraWrap) {
      const selectedExtra = new Set(Array.isArray(d.extra_source_ids) ? d.extra_source_ids : []);
      const sources = srcResp.items || [];
      extraWrap.innerHTML = sources.length
        ? sources.map((s) => `
          <label class="check-label">
            <input type="checkbox" class="org-extra-cb" value="${esc(s.id)}" ${selectedExtra.has(s.id) ? 'checked' : ''} />
            [${s.kind === 'cloud' ? '115 云' : '本地'}] ${esc(s.name)}
          </label>`).join('')
        : '<span class="field-help">暂无额外添加的监控来源（可在「来源与规则」中添加）</span>';
    }

    document.getElementById('org-empty').checked = d.clean_empty !== false;
    document.getElementById('org-auto').checked = !!d.auto_enabled;
    toggleRetentionDays();
  } catch {}
}

async function saveOrganizerConfig(btn) {
  btn.disabled = true;
  try {
    const cloud_categories = [];
    if (document.getElementById('org-c-offline').checked) cloud_categories.push('offline');
    if (document.getElementById('org-c-output').checked) cloud_categories.push('output');
    if (document.getElementById('org-c-archive').checked) cloud_categories.push('archive');

    const local_categories = [];
    if (document.getElementById('org-l-watch').checked) local_categories.push('watch');
    if (document.getElementById('org-l-output').checked) local_categories.push('output');
    if (document.getElementById('org-l-archive').checked) local_categories.push('archive');

    const extra_source_ids = Array.from(document.querySelectorAll('.org-extra-cb:checked')).map((el) => el.value);

    const r = await fetch('/api/v1/organizer/config', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        retention_mode: document.getElementById('org-mode').value,
        retention_days: Math.max(1, Number(document.getElementById('org-days').value) || 14),
        include_cloud: cloud_categories.length > 0,
        include_local: local_categories.length > 0,
        cloud_categories,
        local_categories,
        extra_source_ids,
        clean_empty: document.getElementById('org-empty').checked,
        auto_enabled: document.getElementById('org-auto').checked,
      }),
    });
    if (r.ok) showToast('日期目录整理策略已保存');
    else showToast('整理策略保存失败');
  } finally { btn.disabled = false; }
}

async function previewOrganize(btn) {
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/organizer/preview', { method: 'POST' });
    const d = await r.json();
    if (!r.ok) return showToast('扫描预览失败');
    const wrap = document.getElementById('org-preview-wrap');
    const summary = document.getElementById('org-preview-summary');
    const tbody = document.getElementById('org-preview-list');
    wrap.style.display = 'block';
    const items = d.actions || [];
    summary.textContent = `扫描完成：待归并 ${d.move_count || 0} 个旧日期目录，待清理空目录 ${d.empty_count || 0} 个`
      + (d.skipped_active ? `（已自动跳过 ${d.skipped_active} 个进行中任务目录）` : '');
    tbody.innerHTML = items.length ? items.map((a) => `
      <tr>
        <td><span class="badge ${a.scope === 'cloud' ? 'badge-ok' : 'badge-info'}">${a.scope === 'cloud' ? '115 云端' : '本地 NAS'}</span></td>
        <td>${esc(a.category)}</td>
        <td class="mono-num"><strong>${esc(a.date)}</strong></td>
        <td><span class="badge ${a.action === 'delete_empty' ? 'badge-warn' : 'badge-muted'}">${a.action === 'delete_empty' ? '清理空壳' : '折叠归档'}</span></td>
        <td><code>${esc(a.target_path)}</code></td>
      </tr>
    `).join('') : '<tr><td colspan="5" class="field-help">当前没有超过保留期的旧日期目录需要整理</td></tr>';
  } finally { btn.disabled = false; }
}

async function runOrganize(btn) {
  if (!confirm('确认将超过保留期的旧日期目录归并到「归档区/历史归档」，并按策略清理空目录？（进行中任务目录会自动跳过）')) return;
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/organizer/run', { method: 'POST' });
    const d = await r.json();
    if (r.ok) {
      showToast(`整理完成：归并 ${d.moved_count || 0} 个，清理空目录 ${d.deleted_empty || 0} 个`);
      previewOrganize({ disabled: false });
    } else {
      showToast('执行整理失败');
    }
  } finally { btn.disabled = false; }
}

/* Maintenance, Clear & Cache Reclaim */
async function loadMaintenance() {
  try {
    const d = await fetch('/api/v1/maintenance/summary').then((r) => r.json());
    const st = d.stats || {};
    const elCache = document.getElementById('mnt-cache');
    if (!elCache) return;
    elCache.textContent = `${fmtSize(d.cache_bytes || 0)}（${d.cache_files || 0} 个文件）`;
    document.getElementById('mnt-db').textContent = fmtSize(st.db_bytes || 0);
    document.getElementById('mnt-pipes').textContent = `${st.completed_pipelines || 0} 条完成 / ${st.failed_pipelines || 0} 条失败`;
    document.getElementById('mnt-logs').textContent = `${st.event_count || 0} 条事件 / ${st.bark_count || 0} 条 Bark`;
  } catch {}
}

async function reclaimCache(btn) {
  if (!confirm('确认回收本地下载缓存与残留临时解压目录？（自动跳过正在进行的任务，不删除已解压成品）')) return;
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/maintenance/reclaim-cache', { method: 'POST' });
    const d = await r.json();
    if (r.ok) {
      showToast(`已回收 ${fmtSize(d.freed_bytes || 0)}（清理 ${d.removed_items || 0} 项）`);
      loadMaintenance();
    } else {
      showToast('缓存回收失败');
    }
  } finally { btn.disabled = false; }
}

async function retryAllFailed(btn) {
  if (!confirm('确认将所有失败任务一键重新入队重试？')) return;
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/pipelines/retry-failed', { method: 'POST' });
    const d = await r.json();
    if (r.ok) {
      showToast(`已重新入队 ${d.retried || 0} 条失败任务`);
      loadPipelines();
    }
  } finally { btn.disabled = false; }
}

async function clearPipelines(scope, btn) {
  const tip = scope === 'completed'
    ? '确认清空所有「已完成」的任务记录？（仅清理记录，不删除真实文件）'
    : scope === 'all'
      ? '确认清空「全部」任务记录（含排队中、待确认和已暂停任务）？（仅清空队列记录，绝不删除 115 或本地真实文件）'
      : '确认清空所有「已结束（完成+失败+已暂停）」的任务记录？（进行中任务会保留，且不删除真实文件）';
  if (!confirm(tip)) return;
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/pipelines/clear', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ scope }),
    });
    const d = await r.json();
    if (r.ok) {
      showToast(`已清空 ${d.deleted || 0} 条任务记录`);
      loadPipelines();
      loadMaintenance();
    }
  } finally { btn.disabled = false; }
}

async function clearCurrentLogs(btn) {
  const isBark = document.getElementById('log-bark-pane')?.classList.contains('active');
  if (!confirm(isBark ? '确认清空所有 Bark 联动信号记录？' : '确认清空所有历史运行事件日志？')) return;
  btn.disabled = true;
  try {
    const url = isBark ? '/api/v1/bark/messages' : '/api/v1/events';
    const r = await fetch(url, { method: 'DELETE' });
    const d = await r.json();
    if (r.ok) {
      showToast(`已清空 ${d.deleted || 0} 条记录`);
      loadLogs();
      loadMaintenance();
    }
  } finally { btn.disabled = false; }
}

async function clearAllLogs(btn) {
  if (!confirm('确认清空全部运行事件日志与 Bark 信号记录？')) return;
  btn.disabled = true;
  try {
    await Promise.all([
      fetch('/api/v1/events', { method: 'DELETE' }),
      fetch('/api/v1/bark/messages', { method: 'DELETE' }),
    ]);
    showToast('已清空全部事件日志与 Bark 记录');
    loadMaintenance();
  } finally { btn.disabled = false; }
}

async function vacuumDB(btn) {
  btn.disabled = true;
  try {
    const r = await fetch('/api/v1/maintenance/vacuum', { method: 'POST' });
    if (r.ok) {
      showToast('数据库空间已整理压缩');
      loadMaintenance();
    }
  } finally { btn.disabled = false; }
}
async function copyText(id) {
  const txt = document.getElementById(id).textContent.trim();
  if (!txt) return;
  try {
    await navigator.clipboard.writeText(txt);
    showToast('已复制到剪贴板');
  } catch { showToast('复制失败，请手动选中复制'); }
}

document.addEventListener('DOMContentLoaded', () => {
  initTheme();
  const fi = document.getElementById('imp-file');
  fi.addEventListener('change', () => setSelectedFile(fi.files[0]));
  const dropPanel = document.getElementById('import-drop-panel');
  ['dragenter', 'dragover'].forEach((ev) => dropPanel.addEventListener(ev, (e) => e.preventDefault()));
  dropPanel.addEventListener('drop', (e) => {
    e.preventDefault();
    const f = e.dataTransfer?.files?.[0];
    if (f) setSelectedFile(f);
  });

  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeDetail(); });
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && state.polling) refreshTelemetry();
  });

  refreshTelemetry(true);
  loadPipelines();
  state.timer = setInterval(() => {
    refreshTelemetry();
    if (state.tab === 'tasks' && state.polling && !document.hidden) loadPipelines();
  }, 5000);
});
