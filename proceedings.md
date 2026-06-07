# Proceedings — 盘中监控系统修复 (2026-06-07)

## 背景

盘中监控系统存在 8 个问题（P0/P1/P2），核心问题涉及分析阶段解析、进程重启安全性、未知市场处理、交易日历可靠性、混合市场报告、legacy fallback 行为、线程池生命周期。

## 改动概要

### P0-1: 非 Agent 分析路径保证 resolved phase 写入

- `src/core/pipeline.py`: 新增 `_resolve_analysis_phase()` 函数，统一解析 `"auto"` → 实际 phase
- `main.py`: 盘后定时任务显式传入 `analysis_phase="postmarket"`
- Agent 和非 Agent 路径均传入 resolved phase，不再静默写入 `"auto"`

### P0-2: 进程重启后不清空当天盘中事件

- `src/storage.py`: 新增 `intraday_monitor_state` 持久化表 (key-value)
- `src/core/intraday_monitor.py`: `_should_clear_events()` 检查持久化 marker；`_monitor_snapshot_locked()` 按需清空
- `main.py`: 新增 `--reset-intraday-events` CLI 参数
- `src/config.py`: 新增 `intraday_reset_on_start` 配置项 (默认 `False`)
- `src/core/config_registry.py`: 对应配置注册

### P1-1: 未知股票市场不静默默认 CN

- `IntradayMonitor._get_market_for_stock()`: 无法识别时返回 `None`
- `load_yesterday_analysis()`: by_market 分组显式处理 unknown codes
- 新增 `intraday_unknown_market_policy` 配置 (默认 `"skip"`, 可选 `"cn_compat"`, `"force_open"`)

### P1-2: 严格交易日历接口

- `src/core/trading_calendar.py`: 新增 `is_market_open_strict()` — 返回 `(Optional[bool], str)` 元组
- `IntradayMonitor._is_stock_trading_today()`: 使用 strict 接口
- 新增 `intraday_calendar_fail_open` 配置 (默认 `False`，fail-closed)
- `_is_trading_day()`: 旧接口按配置决定 fail-open/fail-closed

### P1-3: 市场本地时间戳和混合市场报告标题

- `IntradayEvent`: 增加 `market_local_timestamp` 字段
- `_send_decision_email()`: 单市场标题 `盘中监控报告 - {MKT}:{date}`；多市场标题 `盘中监控报告 - CN:{date} / HK:{date} / US:{date}`
- `_save_event()`: 计算并存储市场本地时间戳

### P1-4: Legacy fallback 默认关闭

- `intraday_legacy_fallback_enabled` 默认从 `True` 改为 `False`
- `load_yesterday_analysis()`: primary query 未命中时输出诊断日志（检查同 code 不同 phase、`analysis_phase="auto"` 记录、NULL `effective_trading_date`）
- fallback 命中时 WARNING 日志包含股票代码和记录信息

### P2: Executor 生命周期管理

- `_QUOTE_EXECUTOR`: 模块级 `ThreadPoolExecutor(max_workers=4)`，替代局部 executor
- 超时在 HTTP fetcher 层下发

## 新增测试

- `TestPhaseResolution` (4 tests): `_resolve_analysis_phase()` 行为
- `TestPersistentMarkerRestart` (5 tests): 持久化 marker 重启安全
- `TestUnknownMarketHandling` (4 tests): 未知市场过滤
- `TestStrictCalendar` (6 tests): 严格日历接口契约
- `TestMarketLocalTimestamp` (4 tests): 市场本地时间戳
- `TestFallbackDefaultOff` (4 tests): fallback 默认关闭行为
- 预存测试修复: TestMonitorSnapshot, TestPerStockTradingDay, TestRestartPreservesEvents, TestLoadYesterdayAnalysisBackwardCompat, TestLoadYesterdayAnalysisFallbackRestriction, TestIsTradingDay

## 测试结果

```
87 passed, 0 failed, 3 warnings in 5.94s
```

## 新增配置项

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `intraday_unknown_market_policy` | `"skip"` | 未知市场策略: skip/cn_compat/force_open |
| `intraday_calendar_fail_open` | `False` | 日历不可用时是否 fail-open |
| `intraday_legacy_fallback_enabled` | `False` | legacy fallback 是否启用 |
| `intraday_reset_on_start` | `False` | 启动时是否清空当天盘中事件 |
| `intraday_force_run` | `False` | 非交易日是否强制执行 |

## 核心设计原则

1. `created_at` 仅表示记录创建时间，不代表分析归属交易日
2. `"auto"` 只能是运行时输入，不作为持久化阶段
3. 盘中主路径依赖 `effective_trading_date + analysis_phase="postmarket"`
4. Legacy fallback 仅服务旧数据迁移，不是正常路径
5. 盘中监控按股票所属市场处理交易日、市场日期和事件时间
6. 进程重启不删除当天已持久化盘中事件
7. Unknown market 和 calendar unknown 默认 fail-closed，除非用户显式强制

## 修改文件

| 文件 | 说明 |
|------|------|
| `src/core/intraday_monitor.py` | 主要修复: phase resolution, persistent marker, unknown market, strict calendar, market-local timestamps, fallback, executor |
| `src/core/pipeline.py` | `_resolve_analysis_phase()` |
| `src/core/trading_calendar.py` | `is_market_open_strict()` |
| `src/storage.py` | `intraday_monitor_state` 表 |
| `main.py` | `--reset-intraday-events` CLI, explicit `analysis_phase` |
| `src/config.py` | 5 个新配置项 |
| `src/core/config_registry.py` | 5 个新配置注册 |
| `tests/test_intraday_monitor.py` | 27 个新测试 + 预存测试修复 |
