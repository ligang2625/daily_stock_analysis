# 盘中监控系统 — proceedings

## 一、架构总览

### 核心流程

```
盘中快照（每 30 分钟，10:00–14:00）
  实时行情 → 阈值比对(_compare_with_thresholds) → SQLite 持久化

14:20 决策汇总
  SQLite 读事件 → LLM Prompt 生成 + 调用 → Email 发送
```

### 阈值判断逻辑

优先级（高→低）：stop_loss < secondary_buy < ideal_buy < take_profit

事件类型：break_stop_loss / enter_secondary_buy / enter_ideal_buy / near_buy_zone / enter_take_profit / price_only / unusual_volume(量比≥3.0)

### SQLite 表

- `intraday_events` — 盘中事件
- `intraday_snapshots` — 快照记录 (status: completed/partial/failed)
- `intraday_monitor_state` — KV 状态存储

### 昨日分析加载优先级

1. SQLite `AnalysisHistory` 热字段 (ideal_buy/secondary_buy/stop_loss/take_profit)
2. 回退：`raw_result` JSON → 递归搜索 sniper_points/battle_plan

---

## 二、运行模式

### A: 常驻进程 (`--schedule`)

```bash
python main.py --schedule --no-run-immediately
```
scheduler 轮询，进程约 5h (9:50-14:40)。6 次快照 + 1 次决策。

### B: GitHub Actions 分次触发（推荐）

```bash
python main.py --intraday-snapshot    # 单次快照
python main.py --intraday-decision    # 决策汇总
```
每次独立进程，约 2 分钟。`actions/cache@v4` 跨 run 持久化 `data/`。

---

## 三、配置

### 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `INTRADAY_MONITOR_ENABLED` | 启用盘中监控 | - |
| `INTRADAY_MONITOR_STOCKS` | 监控股票（逗号分隔） | - |
| `INTRADAY_MONITOR_DECISION_TIME` | LLM 决策时间 | `14:20` |
| `INTRADAY_LEGACY_FALLBACK_ENABLED` | 旧数据兼容 fallback | `false` |
| `INTRADAY_RESET_ON_START` | 启动清空当天事件 | `false` |
| `INTRADAY_CN_BATCH_FIRST` | CN 批量预取优先 | `true` |
| `INTRADAY_CN_BATCH_THRESHOLD` | CN 批量触发阈值 | `3` |
| `INTRADAY_HK_BATCH_PRIMARY_TIMEOUT` | HK 主批量超时 | `20.0` |
| `INTRADAY_HK_BATCH_FALLBACK_TIMEOUT` | HK 备用批量超时 | `60.0` |
| `INTRADAY_SNAPSHOT_LOCK_TTL` | snapshot 锁 TTL | `600` |
| `INTRADAY_DECISION_LOCK_TTL` | decision 锁 TTL | `300` |
| `INTRADAY_LLM_MAX_TOKENS` | LLM max tokens | `8192` |
| `INTRADAY_DECISION_EXPECTED_SCOPE` | decision 期望覆盖范围 | `events` |
| `INTRADAY_DECISION_COMPLETENESS_ENABLED` | 决策完整性校验 | `true` |
| `INTRADAY_DECISION_COMPLETION_RETRY` | 截断续写重试次数 | `1` |
| `INTRADAY_DECISION_COMPLETION_USE_FALLBACK` | 续写使用备用 LLM | `true` |
| `INTRADAY_DECISION_APPEND_RAW_FOR_MISSING` | 续写失败追加 raw summary | `true` |
| `INTRADAY_EMAIL_BODY_MODE` | 邮件正文模式 | `full` |
| `INTRADAY_EMAIL_BODY_MAX_CHARS` | 邮件正文最大字符数 | `60000` |
| `INTRADAY_EMAIL_ATTACH_FULL_REPORT` | 附件完整报告 | `true` |
| `INTRADAY_DECISION_STRICT_COMPLETENESS` | 严格模式: 缺失阻断 OFFICIAL_DECISION | `false` |
| `INTRADAY_UNKNOWN_MARKET_POLICY` | 未知市场策略 | `skip` |
| `INTRADAY_CALENDAR_FAIL_OPEN` | 日历不可用是否放行 | `false` |
| `INTRADAY_FORCE_RUN` | 非交易日强制执行 | `false` |
| `INTRADAY_SYSTEM_PROMPT` | 盘中 system prompt | - |
| `INTRADAY_MARKET_SNAPSHOT_ENABLED` | 大盘指数快照 | `true` |
| `INTRADAY_MIN_MARKET_INDEX_COVERAGE` | 指数覆盖率下限 | `0.5` |
| `INTRADAY_DATA_QUALITY_ALERT_ENABLED` | 数据质量告警 | `true` |
| `INTRADAY_INDEX_DATA_SOURCE` | 指数数据源 | `akshare` |
| `INTRADAY_LLM_FALLBACK_ENABLED` | 备用 LLM 启用 | `false` |
| `INTRADAY_LLM_FALLBACK_PROTOCOL` | 备用 LLM 协议 | `openai` |
| `INTRADAY_LLM_FALLBACK_BASE_URL` | 备用 LLM URL | - |
| `INTRADAY_LLM_FALLBACK_API_KEY` | 备用 LLM 密钥 | - |
| `INTRADAY_LLM_FALLBACK_MODEL` | 备用 LLM 模型 | - |
| `INTRADAY_LLM_FALLBACK_TEMPERATURE` | 备用 LLM temperature | - |
| `INTRADAY_LLM_FALLBACK_MAX_TOKENS` | 备用 LLM max_tokens | - |
| `INTRADAY_LLM_FALLBACK_TIMEOUT_SEC` | 备用 LLM 超时 | `60` |
| `INTRADAY_LLM_FALLBACK_RETRY_ON` | 备用 LLM 重试条件 | `config_error,rate_limited,network_error,empty_response` |
| `SCHEDULE_ENABLED` | 启用调度 | - |
| `SCHEDULE_RUN_IMMEDIATELY` | 调度启动时立即分析 | - |
| `TRADING_DAY_CHECK_ENABLED` | 交易日检查 | - |
| `EMAIL_SENDER` | 发件邮箱 | - |
| `EMAIL_PASSWORD` | 邮箱密码/SMTP 授权码 | - |
| `EMAIL_RECEIVERS` | 收件邮箱 | - |
| `REALTIME_SOURCE_PRIORITY` | 数据源优先级 | `tencent,akshare_sina,...` |
| `LITELLM_MODEL` | LLM 模型 | `gemini-3-flash-preview` |
| `STOCK_LIST` | 默认股票列表 | - |

### GitHub Actions Secrets

`LITELLM_API_KEY`, `GEMINI_API_KEY`, `EMAIL_SENDER`, `EMAIL_PASSWORD`, `EMAIL_RECEIVERS`, `TUSHARE_TOKEN`, `INTRADAY_LLM_FALLBACK_API_KEY`

### GitHub Actions Variables

`INTRADAY_MONITOR_STOCKS`, `INTRADAY_MONITOR_DECISION_TIME`, `REALTIME_SOURCE_PRIORITY`, `LITELLM_MODEL`, `STOCK_LIST`, `INTRADAY_LLM_FALLBACK_ENABLED`, `INTRADAY_LLM_FALLBACK_PROTOCOL`, `INTRADAY_LLM_FALLBACK_BASE_URL`, `INTRADAY_LLM_FALLBACK_MODEL`, `INTRADAY_LLM_FALLBACK_TEMPERATURE`, `INTRADAY_LLM_FALLBACK_MAX_TOKENS`, `INTRADAY_LLM_FALLBACK_TIMEOUT_SEC`, `INTRADAY_LLM_FALLBACK_RETRY_ON`

---

## 四、GitHub Actions Triggers

### 盘中 (intraday-monitor.yml)

| UTC | Beijing | Action |
|-----|---------|--------|
| 1:55 | 9:55 | snapshot |
| 2:25 | 10:25 | snapshot |
| 2:55 | 10:55 | snapshot |
| 3:25 | 11:25 | snapshot |
| 5:30 | 13:30 | snapshot |
| 6:00 | 14:00 | snapshot |
| 6:30 | 14:30 | snapshot + decision |

### 盘后 (main.yml / 00-daily-analysis.yml)

| Workflow | UTC | Beijing |
|----------|-----|---------|
| `main.yml` | 8:00 | 16:00 |
| `00-daily-analysis.yml` | 8:11 | 16:11 |

concurrency: 盘中 `intraday-monitor`，盘后 `stock-analysis`，互不阻塞。

### Workflow 关键配置要点

- **Cache**: `actions/cache@v4`，key 前缀 `dsa-data-*`；snapshot 写/decision 读；盘后 restore→analyze→save 闭环
- **Artifact**: 每次盘后上传 `data/stock_analysis.db`
- **LLM fallback 环境变量映射**: 盘中 workflow env 区域需完整声明 9 项 `INTRADAY_LLM_FALLBACK_*`，API_KEY 从 secrets 读取，其余优先从 vars 读取
- **DB 诊断**: 盘中/盘后均有数据库状态摘要步骤（Python try/except 非阻断），缺表时打印 `not ready`
- **LLM 配置**: `LITELLM_CONFIG_YAML` 环境变量 + YAML 写入；`LITELLM_MODEL`/`LITELLM_API_KEY`/`GEMINI_API_KEY`

---

## 五、Session Log

### Phase 1: 盘中监控核心 (Sessions 1-5, 2026-06-08~06-10)
- Session 1: `intraday_monitor.py` 初始构建 (~1880 行)，LLM 决策 prompt，9 轮 bug fix
- Session 2: Batch Prefetch 重构 — 逐股获取 → snapshot 级批量预取
- Session 3: Batch-First 修复 — HK 部分命中补全、code normalize、Lock TTL 配置化
- Session 4: 快照状态持久化 — `_run_snapshot_with_state_persistence`、防御性建表
- Session 5: 正式决策绑定 `preferred_snapshot_id`、snapshot status 分级 (completed/partial/failed)

### Phase 2: 链路修复与 Workflow 闭环 (Sessions 6-9, 2026-06-10~06-13)
- Session 6: partial snapshot coverage gate、`--analysis-phase` CLI、cache key 统一 `dsa-data-*`
- Session 7: 盘后 restore→analyze→save 闭环、DB 摘要 Python 非阻断诊断
- Session 8: LLM 调用链统一 — 盘中从 `litellm.completion()` → `GeminiAnalyzer.generate_text()`
- Session 9: 错误分类精化、INTRADAY_SYSTEM_PROMPT 配置、workflow env 一致性测试

### Phase 3: 系统加固 (Sessions 10-13, 2026-06-07~06-17)
- Session 10: P0 analysis_phase 修复、进程重启不重置事件、严格日历、未知市场 fail-closed
- Session 11: 港股代码大小写规范化、`src/utils/stock_code.py` 统一工具
- Session 12: 大盘指数快照、个股全天走势、决策 prompt 增强
- Session 13: 指数质量门禁、专用行情路由 (CN→AkShare, HK/US→yfinance)

### Phase 4: 持久化与归档 (Sessions 14-18, 2026-06-17~06-18)
- Session 14: 主库持久化优化 — code_norm 存储、热字段/payload 拆分、schema migration、归档 CLI
- Session 17: 技术面历史结构化 — 4 张历史表、archive extractors、MarketDataRepository 统一查询
- Session 18: Payload split 兼容修复 — extractor/repository LEFT JOIN、cutoff 边界 `<` 统一
- Session 16: 主库补完 — 遗留大列置 NULL、分页查询、canonical 加载、backfill_phase1
- Session 15: 决策邮件分组列表展示 — 买入/卖出止盈/卖出止损/观望 4 组、unusual_volume 附注

### Phase 5: 备用 LLM 渠道 (Sessions 19-20, 2026-06-22)
- Session 19: 主 LLM 失败 → 备用 LLM fallback 链路（`_call_llm_with_fallback`）、9 项独立配置、双失败分离告警、`.env.example` 示例
- Session 20: Refinement review — 全部 9 项 issue 确认已在 Session 19 完整实现，无额外改动

### Phase 6: Decision 内容完整性 (Sessions 21, 2026-06-29)
- decision 邮件截断修复：LLM finish_reason 检测 + `DecisionContentIntegrity` 完整性校验 + `_extract_stock_codes_from_decision_content` 股票覆盖率校验
- Prompt 新增完整性协议 + sentinel 结束标记 `<!-- INTRADAY_DECISION_COMPLETE -->`
- 截断后续写 3 策略（主 LLM 重试 → fallback → raw summary 兜底）
- 邮件附件兜底：`send_to_email_with_attachments()`，长正文自动切换 summary+attachment 模式
- 配置扩展：`INTRADAY_LLM_MAX_TOKENS` 默认 4096→8192；新增 8 项决策完整性/邮件配置
- `GenerateTextResult` dataclass + `generate_text_with_metadata()` 暴露 finish_reason/usage
- New tests: `tests/core/test_intraday_decision_content_integrity.py` (11 测试)

### Phase 6: Decision 内容完整性闭环 (Session 22, 2026-06-29)
- Sentinel 交叉校验: 正则提取 expected/covered 并与程序计数比对，空 sentinel 正则修复
- `CONTENT_INCOMPLETE_ALERT` 邮件类型: strict mode 下完整性失败 → `[内容不完整]` 告警
- `_complete_missing_decision_sections()` 接入主链路: integrity fail → 补全 → 二次校验 → 发送
- Fallback LLM finish_reason/usage 提取: `length`/`max_tokens` → `truncated_suspected=True`
- `_save_decision_artifacts()`: 保存 raw/normalized/integrity JSON/HTML 4 文件到 logs/
- `_build_decision_body_summary()`: section-boundary 切割取代 `content[:N]` 硬截断
- 配置扩展: `INTRADAY_DECISION_STRICT_COMPLETENESS`; workflow 9 项变量透传
- 5 项新测试: sentinel 值不匹配、无 sentinel not ok、分组统计、无 mid-Markdown 截断

---

## 六、Known Issues (2 pre-existing)

1. `test_expired_cache_returns_none` / `test_expired_ttl_returns_none` — cache TTL mock 行为不符
2. Issue 6（常驻 scheduler 14:30 snapshot/decision 冲突）暂缓
