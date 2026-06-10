# Session 2026-06-10: Intraday/Postmarket Workflow Phase 3 — 完整跑通链路修复

## 改动概要

本轮修复 6 个问题（3 P0, 2 P1, 1 P2 暂缓），核心方向：partial snapshot coverage gate、workflow data cache 统一、CLI analysis-phase 参数、LLM 配置对齐、DB 摘要日志。

## 修改文件

**`src/core/intraday_monitor.py`** (+135/-6):
- 新增 `_get_usable_snapshot_by_id()` — 查询 `status IN ('completed', 'partial')`
- 新增 `_get_latest_usable_snapshot()` — 查询 `status IN ('completed', 'partial')`，completed 优先
- `_final_decision_locked()` 改用 usable snapshot 方法，coverage gate 为最终仲裁
- `load_yesterday_analysis()` 新增 sniper-point 完整性统计日志
- `_final_decision_locked()` 新增 DB 状态摘要日志 + 决策状态摘要日志

**`main.py`** (+11/-1):
- 新增 `--analysis-phase` CLI 参数，支持 premarket/intraday/postmarket/auto

**`.github/workflows/00-daily-analysis.yml`** (+46/-1):
- 新增 `actions/cache/save@v4` 保存 `data/` 到 `dsa-data-*` 前缀
- 新增 `actions/upload-artifact` 上传 `data/stock_analysis.db`
- `python main.py` 调用全部加 `--analysis-phase postmarket`
- 新增数据库状态摘要步骤（sqlite3 查询 analysis_history）

**`.github/workflows/intraday-monitor.yml`** (+14/-3):
- Cache key 前缀从 `intraday-data-*` 改为 `dsa-data-*`
- 新增 `LITELLM_CONFIG_YAML` 环境变量和 YAML 写入逻辑

**`tests/test_intraday_monitor.py`** (+7/-2):
- 3 个测试 mock 目标从 `_get_latest_completed_snapshot` 改为 `_get_latest_usable_snapshot`

## 验证
- 177 passed, 3 预存失败（cache TTL、fallback event_type）
- 编译检查：OK
- Verifier (opus) 确认：全部验收标准通过
- 无回归：原有 `_get_completed_snapshot_by_id()` 和 `_get_latest_completed_snapshot()` 保留

## 已知预存问题
- `test_expired_cache_returns_none` / `test_expired_ttl_returns_none` — cache TTL mock 行为不符
- `test_fallback_single_quote_success` — event_type 断言不符

## 遗留
- Issue 6（常驻 scheduler 14:30 snapshot/decision 冲突）暂缓
- `actions/cache` 长期迁移到 artifact/外部存储未完成（中期方案）
