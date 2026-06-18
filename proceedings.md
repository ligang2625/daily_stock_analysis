# Proceedings

## Session 1: 盘中监控系统修复 (2026-06-07)

### Bugs Fixed

| # | Priority | Issue | Key Change |
|---|----------|-------|------------|
| 1 | P0 | 非 Agent 路径 phase 写入 `"auto"` | `_resolve_analysis_phase()` 统一解析；`main.py` 盘后显式传 `analysis_phase="postmarket"` |
| 2 | P0 | 进程重启清空当天盘中事件 | `intraday_monitor_state` 持久化表；`_should_clear_events()` 检查 marker；`--reset-intraday-events` CLI |
| 3 | P1 | 未知市场静默默认 CN | `_get_market_for_stock()` 返回 `None`；`intraday_unknown_market_policy=skip` |
| 4 | P1 | 日历接口 fail-open | `is_market_open_strict()` 返回 `(Optional[bool], str)`；`intraday_calendar_fail_open=false` |
| 5 | P1 | 混合市场报告无日期区分 | `IntradayEvent.market_local_timestamp`；邮件标题 `{MKT}:{date}` |
| 6 | P1 | Legacy fallback 默认开启 | `intraday_legacy_fallback_enabled` 默认 `false` |
| 7 | P2 | Executor 生命周期 | 模块级 `_QUOTE_EXECUTOR = ThreadPoolExecutor(4)` |

### 新增配置

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `intraday_unknown_market_policy` | `skip` | skip/cn_compat/force_open |
| `intraday_calendar_fail_open` | `False` | 日历不可用时放行 |
| `intraday_legacy_fallback_enabled` | `False` | 旧数据 fallback |
| `intraday_reset_on_start` | `False` | 启动清空当天事件 |
| `intraday_force_run` | `False` | 非交易日强制执行 |

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/core/intraday_monitor.py` | phase resolution, persistent marker, unknown market, strict calendar, market timestamps, fallback, executor |
| `src/core/pipeline.py` | `_resolve_analysis_phase()` |
| `src/core/trading_calendar.py` | `is_market_open_strict()` |
| `src/storage.py` | `intraday_monitor_state` 表 |
| `main.py` | `--reset-intraday-events` CLI, explicit analysis_phase |
| `src/config.py` | 5 新配置项 |
| `tests/test_intraday_monitor.py` | 27 新测试 + 预存测试修复 |

## Session 2: 港股代码大小写规范化 (2026-06-16)

### Bugs Fixed

| # | Priority | Issue | Key Change |
|---|----------|-------|------------|
| 1 | Critical | 港股昨日盘后分析查询大小写不一致 | `load_yesterday_analysis()` 所有 SQL `.in_()` 前 `normalize_stock_code_for_history_query()` 统一 HK 大写 |
| 2 | Critical | `_yesterday_analysis` key 不一致 | dict key 统一小写；lookup 用 `normalize_stock_code_key()` + 多 case fallback |

### 新增工具

| 文件 | 内容 |
|------|------|
| `src/utils/stock_code.py` | `normalize_stock_code_key()` (HK→小写), `normalize_stock_code_for_history_query()` (HK→大写) |

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/core/intraday_monitor.py` | 查询/存储/lookup 全链路统一规范化；matched=0 诊断日志 |
| `src/services/alert_worker.py` | 空规则时 `logger.warning` 提示 |
| `tests/test_intraday_monitor.py` | `TestLoadYesterdayAnalysisHKCasing` 3 回归测试 |

### 测试

**203 passed, 3 failed** (3 failed 为预存已知问题，无回归)

## Session 3: 大盘指数快照与决策走势增强 (2026-06-16)

### 新增特性

| # | Priority | Feature | Key Change |
|---|----------|---------|------------|
| 1 | Critical | Decision 阶段增加个股全天走势 | `_load_stock_timelines_for_decision()` 加载全量快照事件构建 per-stock timeline；`_summarize_stock_timeline()` 汇总首末价格、日内高低、最大回撤、涨跌幅趋势 |
| 2 | Critical | 大盘指数盘中快照 | `intraday_market_snapshots` 表 + `_snapshot_market_indices()` 每轮快照抓取 CN/HK/US 主要指数 |
| 3 | Major | 决策 Prompt 增加走势段落 | `build_intraday_prompt(stock_timelines=, market_timelines=)` + 3 新段落：大盘指数走势、个股走势、相对强弱 |
| 4 | Major | 快照时间从 DB 重建 | `_rebuild_snapshot_times_from_db()` 查询 intraday_snapshots，不再依赖进程内存 |
| 5 | Major | raw_quote 填充结构化行情 | `_extract_quote_data()` 提取 open/prev_close/high/low/volume/amount/turnover_rate/source |
| 6 | Major | 多市场按 market_dates 查询 | timeline 加载使用 per-market query_date |
| 7 | Minor | 指数数据降级策略 | status 字段 + fail-open + prompt 标注"数据缺失，勿臆测" |

### 新增配置

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `intraday_market_snapshot_enabled` | `true` | 启用大盘指数快照 |
| `intraday_market_indices_cn` | `000001,399001,399006` | A 股指数代码 |
| `intraday_market_indices_hk` | `HSI,HSTECH` | 港股指数代码 |
| `intraday_market_indices_us` | `^GSPC,^IXIC` | 美股指数代码 |

### 新增数据库表

```sql
CREATE TABLE intraday_market_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id TEXT NOT NULL, run_id TEXT,
    query_date TEXT NOT NULL, market TEXT NOT NULL,
    market_local_timestamp TEXT, index_code TEXT NOT NULL,
    index_name TEXT, current_price REAL, open_price REAL,
    prev_close REAL, high_price REAL, low_price REAL,
    change_pct REAL, volume REAL, amount REAL,
    source TEXT, status TEXT NOT NULL DEFAULT 'valid',
    raw_quote TEXT, created_at TEXT NOT NULL,
    UNIQUE(snapshot_id, market, index_code)
);
```

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/core/intraday_monitor.py` | 指数快照、timeline 构建、raw_quote 填充、DB 快照时间重建 |
| `src/core/intraday_prompt.py` | 新参数 + 3 走势段落 |
| `src/config.py` | 4 新配置项 |

### 测试

**3277 passed, 16 failed** (16 failed 为预存已知问题，无回归)

## Session 4: 指数数据质量门禁与专用行情路由 (2026-06-17)

### 新增特性

| # | Priority | Feature | Key Change |
|---|----------|---------|------------|
| 1 | Critical | 大盘指数数据质量门禁 | `_final_decision_locked()` 覆盖率计算 + quality_alert 标记 + 邮件头部显式大盘状态 |
| 2 | Critical | 专用指数行情路由 | `_fetch_index_quote()` 按市场分发: CN→AkShare, HK/US→yfinance；配置 `intraday_index_data_source` |
| 3 | Major | 邮件大盘覆盖率诊断 | `market_timeline_summary` 日志 per-market；邮件附加大盘覆盖率行 |
| 4 | Major | 全量快照恢复检查 | `historical_snapshot_count` 查询；单点/双点时 prompt 标注走势数据不足 |
| 5 | Major | 指数数据状态区分 | `_load_market_timeline_stats_for_decision()` 返回未采集/采集失败/有效三类；prompt 差异化展示 |
| 6 | Major | Workflow 扩展诊断 | 新增配置打印步骤；DB 摘要含 `intraday_market_snapshots` 表；decision 前打印快照数 |
| 7 | Minor | Prompt 输出表扩展 | 4列→8列：`日内走势\|大盘环境\|相对强弱\|数据质量`；指令要求每行引用指数 |
| 8 | Minor | 配置入口补充 | 3 新配置项 (coverage gate, alert enabled, data source) |

### 新增配置

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `intraday_min_market_index_coverage` | `0.0` | 大盘指数数据质量门禁 (0.0-1.0) |
| `intraday_data_quality_alert_enabled` | `true` | 大盘指数数据质量告警邮件 |
| `intraday_index_data_source` | `dedicated` | 指数数据源策略: dedicated/realtime/auto |

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/core/intraday_monitor.py` | 4 新指数获取方法, coverage gate, 诊断日志, 历史快照检查, email 覆盖率 |
| `src/core/intraday_prompt.py` | 8列输出表, 走势数据不足提醒, 指数状态区分 |
| `src/config.py` | 3 新配置项 |
| `.github/workflows/intraday-monitor.yml` | 3 新步骤/配置 |
| `.env.example` | 3 新 env 变量 |

### 测试

**949 passed, 1 failed, 2 skipped** (1 failed 为预存已知 cache 问题，无回归)

## Session 5: Phase 1 — 主库持久化层优化 (2026-06-17)

### 新增特性 / 重构

| # | Priority | Change | Key Detail |
|---|----------|--------|------------|
| 1 | Major | 股票代码规范化存储 | `normalize_stock_code_for_storage()` / `normalize_market_region()` in `stock_code_utils.py`；CN→600519, HK→HK00700, US→AAPL |
| 2 | Major | AnalysisHistory 热字段与大 payload 拆分 | `analysis_history_payload` 表 (FK→history_id)；`raw_result`/`news_content`/`context_snapshot` 移入 payload；新增 11 个热列 (`code_norm`, `market`, `model_used`, `current_price`, `change_pct`, `volume_ratio`, `turnover_rate`, `market_phase_summary`, `updated_at`) |
| 3 | Major | Schema migration 框架 | `_apply_schema_migrations()` 幂等升级：加列、建表、建索引、回填。JSON 解析失败非致命 (log+skip) |
| 4 | Major | 写路径拆分 | `save_analysis_history()` 同时写热列 + payload 表；`save_daily_data()` 写 `code_norm` |
| 5 | Major | 读路径懒加载 | HistoryService list 只读热列，detail 按 history_id 懒加载 payload |
| 6 | Major | 5 条复合索引 | `ix_analysis_yesterday_lookup_norm`, `ix_analysis_market_review_date`, `ix_analysis_created_id`, `ix_analysis_payload_history_id`, `ix_stock_daily_norm_date` |
| 7 | Major | 历史库基础骨架 | `HistoricalDatabaseManager`（独立单例）+ `archive_runs` 表；配置 `historical_database_path` / `archive_retention_days` |
| 8 | Major | 安全归档 CLI | `python -m src.maintenance.archive --days 5 --dry-run`；`--cutoff-date`/`--skip-vacuum`；归档记录 ledger，异常→failed，VACUUM 仅成功后 |
| 9 | Major | 盘中 code_norm 查询 | `load_yesterday_analysis()` 用 `code_norm.in_()` 优先命中复合索引，`code.in_()` 兜底 |
| 10 | Fixed | 3 个预存测试修复 | `test_history_detail_accepts_dict_raw_result` (hot column precedence), `test_history_detail_handles_missing_overview_when_snapshot_disabled` (None→''), `test_history_list_includes_timeline_summary_fields` (change_pct % strip) |
| 11 | Fixed | 5 个 config registry 项补全 | `INTRADAY_UNKNOWN_MARKET_POLICY`, `INTRADAY_CALENDAR_FAIL_OPEN`, `INTRADAY_LEGACY_FALLBACK_ENABLED`, `INTRADAY_RESET_ON_START`, `INTRADAY_FORCE_RUN` 补 docs；`INTRADAY_MONITOR_ENABLED` 加入 `WEB_SETTINGS_HIDDEN_FROM_UI` |

### 新增配置

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `historical_database_path` | `./data/historical_market.db` | 历史库路径 |
| `archive_retention_days` | `5` | 归档保留天数 |

### 新增文件

| 文件 | 说明 |
|------|------|
| `src/services/stock_code_utils.py` | 股票代码规范化工具 |
| `src/storage_historical.py` | 历史库管理器 + archive_runs 表 |
| `src/maintenance/__init__.py` | 维护工具包 |
| `src/maintenance/archive.py` | 安全归档 CLI |
| `tests/test_code_norm_migration.py` | 34 测试：规范化 |
| `tests/test_storage_payload_split.py` | 7 测试：拆分 + 迁移 |
| `tests/test_archive_dry_run.py` | 4 测试：归档 |
| `tests/test_history_service_payload_compat.py` | 7 测试：兼容性 |

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/storage.py` | `AnalysisHistoryPayload`, 11 hot columns, `code_norm` on StockDaily, 5 indexes, save split, payload helpers, schema migration |
| `src/services/stock_code_utils.py` | NEW: `normalize_stock_code_for_storage()`, `normalize_market_region()` |
| `src/services/history_service.py` | Lazy payload loading, hot column precedence |
| `src/core/intraday_monitor.py` | `load_yesterday_analysis()` code_norm query |
| `src/core/config_registry.py` | 5 docs entries, `INTRADAY_MONITOR_ENABLED` hidden |
| `src/config.py` | `historical_database_path`, `archive_retention_days` |
| `tests/test_analysis_history.py` | 3 预存测试修复 |

### 测试

**3331 passed, 14 failed, 2 skipped** (14 failed 均为预存已知问题，零回归)

## Session 6: Phase 1 Remediation — 主库持久化层补完 (2026-06-18)

### 修复 / 补完

| # | Priority | Issue | Key Change |
|---|----------|-------|------------|
| 1 | Major | 新增 analysis_history 仍向主表写大 payload | `save_analysis_history()` 将 `raw_result`/`news_content`/`context_snapshot` 设为 NULL，仅写入 `analysis_history_payload` |
| 2 | Major | History list 仍加载并解析大遗留列 | 新增 `get_analysis_history_paginated_summary()` 显式列选择排除大列；list serializer 优先使用热列 |
| 3 | Major | Detail/markdown 路径未一致使用 payload 表 | 新增 `get_analysis_history_payload_dict()` canonical 加载器，被 detail/markdown/diagnostics 共用；遗留列 fallback |
| 4 | Major | 归档清理顺序错误（payload 删除先于遗留列置空） | 清理顺序改为：置空遗留列 → 删除 payload 行 → 删除 runtime 表；受保护表显式标记 |
| 5 | Major | `code_norm` 未被历史筛选和昨日分析全量使用 | `get_history_list()` 计算 `code_norm_candidates` 先匹配复合索引；昨日分析已使用 |
| 6 | Major | 存量数据未回填 | `backfill_phase1()` 幂等批量回填 code_norm/market/热列/payload 行，SQLite 启动时自动调用 |
| 7 | Major | `normalize_stock_code_for_storage` 忽略显式 market 参数 | HK heuristic 改在 `market` 未提供时触发；传 `market='cn'` 不走 HK 规则 |
| 8 | Minor | SQLite 维护不完整 | 归档后执行 `PRAGMA wal_checkpoint(TRUNCATE)` + `VACUUM` + `ANALYZE`（事务外） |
| 9 | Regression | `_FakeHistoryDb` 缺少 `get_analysis_history_payload_dict` | 3 diagnostics 测试修复 |
| 10 | Regression | `test_market_review.py` 断言遗留 `news_content` 列 | 改为查询 `analysis_history_payload` 表 |

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/storage.py` | payload dict helper, save 遗留列 NULL, paginated summary query, backfill_phase1, diagnostics 写 payload |
| `src/services/history_service.py` | canonical payload 加载 (detail/markdown/diagnostics), code_norm 筛选, lazy payload fallback |
| `src/maintenance/archive.py` | 清理顺序 (置空→payload→runtime), wal_checkpoint+VACUUM+ANALYZE, 受保护表标记 |
| `src/services/stock_code_utils.py` | 显式 market 参数优先于 HK heuristic |
| `tests/test_analysis_history.py` | pre-existing test fixes |
| `tests/test_storage_payload_split.py` | Phase 1 compliance updates |
| `tests/test_run_diagnostics_p2.py` | _FakeHistoryDb mock 补全 |
| `tests/test_market_review.py` | payload 表断言 |

### 新增文件

| 文件 | 说明 |
|------|------|
| `tests/test_history_code_norm_filters.py` | 5 测试：HK 变体过滤、payload detail、过期 payload |
| `tests/test_archive_sequencing_backfill.py` | 5 测试：归档顺序、dry-run 计数、回填幂等性 |

### 测试

**3364 passed, 14 failed, 2 skipped** (14 failed 均为预存已知问题，零回归)
