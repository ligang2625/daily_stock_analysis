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

## Session 7: Phase 2 — 技术面历史结构化 (2026-06-18)

### 新增特性

| # | Priority | Feature | Key Change |
|---|----------|---------|------------|
| 1 | Major | 历史库 4 张业务表 | `HistoricalStockDaily`, `HistoricalIntradayQuotePoint`, `HistoricalPostmarketTechnicalSummary`, `HistoricalMarketLightDaily` ORM + 唯一约束 + 复合索引 + 幂等 upsert |
| 2 | Major | 技术面归档抽取器 | `archive_extractors.py` 含 4 个 extractor + `extract_phase2_technical_history` 入口；stock_daily pre_close/bias 计算，intraday raw_quote JSON 解析，postmarket hot fields + payload 补充，market_light 抽取 |
| 3 | Major | Archive CLI 集成 | `--technical-only`, `--skip-technical-archive`, `--validate-only`；extraction 失败安全门禁阻止主库 cleanup |
| 4 | Major | MarketDataRepository | 统一查询层：recent→主库/old→历史库路由，跨边界合并去重，HK code 归一化，历史库缺失降级 |
| 5 | Major | Phase 2 测试覆盖 | 35 新测试覆盖 schema、extractor、archive 集成、repository、market light 归档 |

### 新增文件

| 文件 | 说明 |
|------|------|
| `src/maintenance/archive_extractors.py` | 4 extractors + 统一入口 |
| `src/services/market_data_repository.py` | 双库统一查询层 |
| `tests/test_historical_storage_schema.py` | 7 测试：表存在、幂等 init、upsert 幂等、可空字段 |
| `tests/test_technical_archive_extractors.py` | 9 测试：stock_daily、intraday、postmarket、market_light extraction |
| `tests/test_archive_phase2_integration.py` | 7 测试：dry-run、technical-only、validate-only、safety gate |
| `tests/test_market_data_repository.py` | 9 测试：主库/历史库/跨边界路由、HK code、降级 |
| `tests/test_market_light_daily_archive.py` | 5 测试：幂等 upsert、缺失字段容错、多 region |

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/storage_historical.py` | 4 新 ORM model + 4 upsert 方法 |
| `src/maintenance/archive.py` | Phase 2 extraction step + 3 CLI flags + safety gate |
| `src/config.py` | `historical_database_path` env var wiring |

### 测试

**3364 + 35 = 3399 passed, 14 failed, 2 skipped** (14 failed 均为预存已知问题，零回归)

## Session 8: Phase 2 Remediation — Payload Split Compatibility (2026-06-18)

### 修复项

| # | Priority | Fix | Key Change |
|---|----------|-----|------------|
| 1 | Critical | Archive extractors payload-split aware | LEFT JOIN `analysis_history_payload` with COALESCE for `raw_result`/`context_snapshot`/`news_content`; legacy fallback |
| 2 | Critical | MarketDataRepository payload-split aware | LEFT JOIN in main-side `get_market_light_daily`/`get_postmarket_technical_summary` |
| 3 | Major | Cutoff boundary strict `<` | All 4 extractors use `< :cutoff` instead of `<=`; aligns with cleanup semantics |
| 4 | Major | Intraday upsert idempotent with NULL snapshot_id | Normalize NULL to `legacy:{row_id}` |
| 5 | Minor | Composite indexes | 4 query-path indexes matching repository patterns |

### 新增文件

| 文件 | 说明 |
|------|------|
| `tests/test_phase2_payload_split_extractors.py` | 6 测试：payload 表提取、cutoff 边界、幂等性、仓库跨边界去重 |

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/maintenance/archive_extractors.py` | payload LEFT JOIN + cutoff `<` + NULL snapshot_id normalization |
| `src/services/market_data_repository.py` | payload LEFT JOIN for main-side queries |
| `src/storage_historical.py` | 4 composite indexes |

### 测试

**410+ passed, 0 regressions** (仅 3 预存 scheduler failures)

## Session 9: Phase 3 — 盘中决策升级：大盘指数归档与相对强弱计算 (2026-06-18)

### 新增特性

| # | Priority | Feature | Key Change |
|---|----------|---------|------------|
| 1 | Major | `IntradayMarketSnapshot` ORM + schema migration | ORM model + `timestamp`/`error_message` 列 migration；`UniqueConstraint('market', 'index_code', 'market_local_timestamp', 'snapshot_id')` |
| 2 | Major | `HistoricalMarketIndexPoint` + batch upsert | 历史库新 ORM；trend_label/strength_label/breadth_label 衍生标签；空 snapshot_id→'' 标准化 |
| 3 | Major | Market index archive extractor | 从主库 `intraday_market_snapshots` 抽取旧数据写历史库；field mapping (open_price→open)；`extract_phase2_technical_history` 集成 step 5 |
| 4 | Major | `MarketDataRepository.get_market_index_trend` | 跨主库/历史库路由；按 retention boundary 拆分查询；`(market, index_code, market_local_timestamp, snapshot_id)` 去重 |
| 5 | Major | Decision 大盘加载优先走 Repository | `_try_repo_market_timelines()` 优先；失败降级到直接 SQL；无回归风险 |
| 6 | Major | `intraday_relative_strength` 模块 | 系统计算个股涨跌幅/路径标签/基准涨跌幅/相对 spread/强弱标签/背离标记；`summarize_relative_strength()` + `format_relative_strength_section()` |
| 7 | Major | Prompt 动态相对强弱注入 | 取代静态 LLM 自行对比；markdown 表 + 系统计算标签供 LLM 直接引用 |

### 修复项

| # | Priority | Fix | Key Change |
|---|----------|-----|------------|
| 1 | Major | NULL `market_local_timestamp` 在 UNIQUE constraint 不可靠 | SQLite 视 NULL 为 distinct；extractor 和 converter 统一 normalize 为空字符串 `''` |
| 2 | Minor | 测试 fixture `snap_id` 使用前未赋值 | `archive_env` fixture 中 `snap_id = days_ago * 100` 移到 `conn.execute()` 前 |
| 3 | Minor | 测试无效 placeholder 代码 | `test_upsert_idempotent` 废弃 `cnt1` 伪计数变量删除 |

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/storage_historical.py` | `HistoricalMarketIndexPoint` ORM + upsert + 索引 |
| `src/maintenance/archive_extractors.py` | `extract_market_index_points()` + composite step + NULL market_local_timestamp normalize |
| `src/services/market_data_repository.py` | `get_market_index_trend()` + 跨库路由 + market_local_timestamp normalize |
| `src/storage.py` | `IntradayMarketSnapshot` ORM + schema migration (timestamp/error_message) |
| `src/core/intraday_monitor.py` | `_try_repo_market_timelines()` + relative strength wiring |
| `src/core/intraday_prompt.py` | `relative_strength_summary` param + dynamic section |
| `docs/CHANGELOG.md` | 7 条 Phase 3 变更 |

## Session 10: Phase 3 Remediation — Schema mismatch, Prompt stats/trends 修复, 测试补全 (2026-06-22)

### 修复项

| # | Severity | Fix | Key Change |
|---|----------|-----|------------|
| 1 | Critical | `intraday_market_snapshots` DDL 缺少 `timestamp`/`error_message` 列 | DDL 补齐 + 幂等列迁移 (PRAGMA table_info + ALTER ADD COLUMN) + `idx_intraday_market_snapshots_timeline`/`status` 索引 |
| 2 | Major | Prompt market_timeline_stats 分支吞掉 trend 表 | 拆为独立 "### 数据质量" + "### 指数走势" 子段，stats 和 trends 同时输出 |
| 3 | Major | `historical_snapshot_count==0` 提示不可达 (外层 `0 < count < 2`) | 显式三路分支 (==0/==1/>=2)，修复非 f-string 字符串 |
| 4 | Major | 测试覆盖不足 | 4 新测试文件 (27 测试)：DDL schema、persist error 状态、extractor field mapping、Repository 跨库路由、Prompt 双节输出、snapshot_count 路径 |
| 5 | Minor | 表名命名与策略文档不一致 | 3 处添加 `persistence_optimization_strategy.md` 注释 |

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/core/intraday_monitor.py` | DDL 补齐 + migration + 2 索引 + persist timestamp fallback + 命名注释 |
| `src/core/intraday_prompt.py` | market index 节双分支 + snapshot_count 三路修复 |
| `src/maintenance/archive_extractors.py` | 命名注释 |
| `src/services/market_data_repository.py` | 命名注释 |

### 新增文件

| 文件 | 测试数 |
|------|--------|
| `tests/test_phase3_market_index_snapshots.py` | 7 |
| `tests/test_phase3_market_index_archive.py` | 4 |
| `tests/test_phase3_market_data_repository_index.py` | 6 |
| `tests/test_phase3_intraday_prompt_market_context.py` | 8 |

### 测试

**22 new + 52 existing = 74 passed, 0 regression**

### Phase 3 收口确认

- 大盘指数快照可稳定写入主库 (DDL 已补齐)
- Prompt 同时输出覆盖率摘要和实际指数走势表
- 单点行情/大盘缺失时 Prompt 明确约束 LLM 不得过度推断
- 第三阶段核心路径具备离线测试覆盖 (27 测试)
- 可以进入 Phase 4

### 新增文件

| 文件 | 说明 |
|------|------|
| `src/core/intraday_relative_strength.py` | 系统相对强弱 summarizer (176 行) |
| `tests/test_market_index_snapshots.py` | 9 测试：ORM/schema/persist/unique constraint/config |
| `tests/test_market_index_archive.py` | 6 测试：cutoff/field mapping/labels/idempotent/table |
| `tests/test_market_data_repository_market_index.py` | 5 测试：main/历史/跨边界/index code filter |
| `tests/test_intraday_prompt_relative_strength.py` | 10 测试：强弱/背离/缺失/prefix/prompt injection |
| `tests/test_intraday_decision_market_context.py` | 6 测试：market summary/stock timeline/relative strength/缺失/fallback/no LLM |

### 测试

**19 (Phase 2) + 33 (Phase 3) = 52 passed, 0 regressions**

## Session 11: Phase 3 Follow-up — `historical_snapshot_count` query fix + test regression (2026-06-22)

### 修复项

| # | Severity | Fix | Key Change |
|---|----------|-----|------------|
| 1 | Major | `final_decision()` 查询不存在的 `snapshot_type` 列 | SQL 改为 `WHERE query_date IN (...placeholders) AND status IN ('completed', 'partial')`；`decision_market_dates.values()` 全市场日期而非单 CN 默认 |
| 2 | Minor | HK/US-only final decision 日期 fallback 不准确 | 动态 `market_dates_list` + 参数化 IN 子句，仅 dict 空时回退服务器日期 |
| 3 | Minor | 归档日志仍标 "Phase 2" | `"Phase 2/3 extraction complete"`；docstring 同步更新 |

### 新增文件

| 文件 | 测试数 |
|------|--------|
| `tests/test_intraday_market_snapshots_schema.py` | 10 |

### 修改文件

| 文件 | 改动 |
|------|------|
| `src/core/intraday_monitor.py` | `historical_snapshot_count` SQL 重写 + 多市场日期参数化 |
| `src/maintenance/archive_extractors.py` | 日志 "Phase 2/3" + docstring 更新 |

### 测试

**10 new + 42 existing Phase 3 = 52 passed, 0 regressions**
