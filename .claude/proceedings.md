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

---

# Session 2026-06-10: Intraday/Postmarket Workflow Phase 4 — 手动触发链路补完

## 改动概要

本轮修复 P0×1 + P1×1，核心方向：盘中 DB 摘要非阻断化、preferred snapshot 不可用时禁止静默 fallback。

## 循环前提确认

验证了 Phase 3 以下改动已就位（未回退，未重复修改）：
- `--analysis-phase postmarket` 已存在于 3 个 workflow 分支
- `data/` cache save/restore 已使用 `dsa-data-*` 前缀（两 workflow 统一）
- `data/stock_analysis.db` artifact 已上传
- `--analysis-phase` CLI 参数已存在，plumbing 正确
- `pipeline.py` 的 `_resolve_analysis_phase()` 正确保留非 auto 值
- snapshot 状态已使用 failed/partial/completed 分级
- `run_one_shot_decision()` 已调用 `_final_decision_locked(preferred_snapshot_id=...)`

## 修改文件

**`.github/workflows/intraday-monitor.yml`** (+48/-19):
- `数据库状态摘要` 步骤从 sqlite3 CLI 改为 Python try/except
- 每张表（analysis_history / intraday_snapshots / intraday_events）单独 try/except
- 缺表时打印 `not ready` 不阻断 workflow

**`src/core/intraday_monitor.py`** (+12/-5):
- `_final_decision_locked()` 新增分支：preferred_snapshot_id 指定但不可用时，发送 `SNAPSHOT_INCOMPLETE_ALERT` 邮件并 return
- 不传入 preferred_snapshot_id 时仍 fallback 到 `_get_latest_usable_snapshot()`

## 验证
- 104 passed, 1 预存失败（test_expired_cache_returns_none）
- FinalDecision + CoverageGate 测试：7 passed, 0 failed
- 编译检查：OK

---

# Session 2026-06-10: Refinement-Executor Review — 确认全部 review 项已完成

## 改动概要

本轮由 `/refinement-executor` 审查 review 文档 6 项 issue，确认 Phase 3/4 已覆盖全部 critical + major 项。2 项剩余 edit（timeout-minutes、postmarket DB 摘要）已在磁盘但未提交，本轮 commit。

## 修改文件

**`.github/workflows/intraday-monitor.yml`** (+25/-1):
- `timeout-minutes: 15` → `30`
- DB 摘要新增 postmarket count + 最近 10 条 postmarket 记录

## Review 项状态

| # | Priority | Item | Status |
|---|----------|------|--------|
| 1 | critical | 盘后→盘中 DB 共享（dsa-data-*\* cache + artifact） | ✅ Phase 3 |
| 2 | critical | `--analysis-phase postmarket` + CLI plumbing | ✅ Phase 3 |
| 3 | major | `timeout-minutes: 15` → `30` | ✅ 本轮 commit |
| 4 | major | DB 摘要增加 postmarket 查询 | ✅ 本轮 commit |
| 5 | major | Preferred snapshot 不可用时不 fallback | ✅ Phase 4 |
| 6 | minor | UTC 分支清理 | ⏸️ 暂缓 |

## 验证
- 无新代码改动（变更已在磁盘，仅首次 commit）
- `.bug-fix-pipeline/plan.md` + `.omc/` 保留为本地产物，不入库

---

### Session 19: 盘中决策备用 LLM 渠道 Fallback (2026-06-22)

## 改动概要

盘中决策链路新增可独立配置的备用 LLM 渠道：主 LLM 失败时自动按可配置条件重试备用 LLM，避免直接进入 LLM_FAILURE_ALERT。备用 LLM 使用 `litellm.completion()` 直接调用，不经过主 analyzer。

## 修改文件

**`src/config.py`** (+18 fields, +18 parsing lines):
- Config 新增 9 个 `intraday_llm_fallback_*` 字段（enabled/protocol/base_url/api_key/model/temperature/max_tokens/timeout_sec/retry_on）
- `from_env` 新增对应环境变量解析，temperature/max_tokens 可选值用预计算变量避免 `parse_env_float(None)` 崩溃

**`src/core/intraday_monitor.py`** (+200 lines):
- `LLMResult` 扩展 6 个审计字段：provider/model/base_url_host/attempts/primary_error_message/fallback_error_message
- 新增 `_call_llm_with_fallback()` 主备 LLM 编排
- 新增 `_should_try_fallback_llm()` fallback 条件校验
- 新增 `_call_fallback_llm()` 直接调用 litellm.completion() 实现
- `_final_decision_locked()` 改用 `_call_llm_with_fallback()`；备用成功时邮件追加来源标注；失败时两路错误分别展示
- `__init__` 增加 fallback 启动日志（显示 model/base_url_host/retry_on）

**`.github/workflows/intraday-monitor.yml`** (+7 lines):
- 增加 6 项 `INTRADAY_LLM_FALLBACK_*` 环境变量映射

**`.env.example`** (+10 lines):
- 增加 fallback 配置示例

**`tests/test_intraday_monitor_llm_fallback.py`** (新文件, 174 行):
- 3 个测试类、13 个测试方法：LLMResult 扩展字段、should_try_fallback 条件分支（6 种）、call_llm_with_fallback 主备切换（4 种）

## 验证
- 13 passed, 0 failed（新测试）
- 编译检查：OK
