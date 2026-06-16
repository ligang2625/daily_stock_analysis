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
