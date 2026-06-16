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
