# Ark（方舟）

面向低内存 NAS（绿联 UGOS / 群晖 / Linux Docker）和本机环境的轻量级云盘离线下载、云解压、本地 7-Zip 解压与自动化管理工具。

## 核心特点

- **极低内存占用**：纯 Go 单静态二进制 + 纯 Go SQLite（WAL，限 2 MB 页缓存）+ 内置 `GOMEMLIMIT=48MiB` 与 `GOGC=50`，空闲常驻仅 **15–25 MB**，总 Docker 镜像约 **35 MB**。
- **不依赖 CloudDrive2**：原生 Go 轻量协议层，按需实现所需接口，内置串行限流、熔断与操作幂等日志。
- **安全高效本地解压**：官方 **7-Zip 26.01 (`7zz`)** 三段式架构（只读预检 + 单次单线程 `-mmt=1` 解压到隔离 staging + 原子发布），按需拉起、空闲占 **0 MB**，支持字典大小内存预检防 OOM。
- **统一任务流水线**：离线下载、云端监测、本地/NAS 目录监控解压、种子下载后清理收口为统一状态机与单张任务卡片。
- **现代外部集成**：兼容第三方离线导入 API（Bearer Token + `Idempotency-Key`）、Bark 90 秒合并窗口联动、**Telegram Bot 通知（支持国内反代地址与代理）** 与通用 Webhook。

## 快速部署（绿联 NAS Docker Compose）

参见 [deploy/compose.ugreen.yaml](deploy/compose.ugreen.yaml)：

```bash
docker compose -f deploy/compose.ugreen.yaml up -d
```

打开 `http://<NAS地址>:5656` 即可访问后台与实时内存监控面板。

## 本机开发与测试

需要 Go 1.24+：

```bash
go mod tidy
go test ./...
go run ./cmd/ark
```

## 当前开发进度

- [x] **Phase 1**：纯 Go 项目骨架、SQLite 9 张核心表与加密存储、API 令牌鉴权、Telegram 通知配置、进程/容器内存诊断接口、极简嵌入前端、多阶段 Dockerfile、绿联 NAS Compose 与 GitHub Actions CI。
- [x] **Phase 2**：原生 Go 115 协议层、本地 `.torrent` 转磁链、离线导入与批次 API、固定窗口/生活事件监测与种子下载后清理。
- [x] **Phase 3**：云解压 4 步状态机、115 直链流式缓存、三段式 7-Zip 本地解压引擎与本地/NAS 监控目录扫描。
- [x] **Phase 4**：Bark 联动触发、Telegram Bot（支持国内反代与代理）/ Webhook 实际推送引擎与完整全流程任务卡片 Web UI。
