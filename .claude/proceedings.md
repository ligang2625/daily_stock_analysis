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

---

# Session 2026-07-10: 盘中分析邮件内容完整性 — 端到端链路修复

## 改动概要

本轮修复盘中决策邮件完整性缺失的 4 个根因（1 Critical + 3 Major），覆盖 LLM 元数据透传、股票级完整性校验、分批生成与补偿、邮件无损拆分。

## 修改文件

**`src/analyzer.py`** (+92/-4):
- 新增 `GenerateTextResult` dataclass（content/model/finish_reason/prompt_tokens/completion_tokens/total_tokens）
- 新增 `generate_text_with_metadata()` — 返回结构化结果
- 新增 `_extract_finish_reason()` — 从 provider 响应提取结束原因
- `_call_litellm()` 返回 4-tuple (text, model, usage, finish_reason)
- 旧 `generate_text()` 保留为薄兼容层，解包丢弃 finish_reason

**`src/core/intraday_monitor.py`** (+811/-174):
- `LLMResult` 扩展：finish_reason/prompt_tokens/completion_tokens/total_tokens/response_bytes/integrity_status/expected_codes/covered_codes/missing_codes/duplicate_codes
- `_call_llm()` 改用 `generate_text_with_metadata()`；`finish_reason` 为 `length`/`max_tokens` 时返回 `truncated_response` 状态
- 新增 `IntradayReportResult` dataclass（report_id/content/expected_codes/covered_codes/missing_codes/complete/error_type）
- 新增 `_check_stock_code_in_report()` / `_validate_report_batch()` / `_validate_full_report()` — 股票覆盖完整性校验
- 新增 `_generate_batched_decision()` / `_generate_single_batch_decision()` / `_generate_single_batch_llm()` / `_merge_report_batches()` — 分批生成与缺失补偿流水线
- 新增 batch 结束标记 `<!-- INTRADAY_BATCH_COMPLETE -->` 和报告级结束标记 `<!-- INTRADAY_REPORT_COMPLETE -->`
- `_send_decision_email()` 新增参数：report_coverage_ratio/report_covered_count/report_expected_count/report_id/report_batch_count；邮件头部区分展示"行情快照覆盖率"与"报告正文覆盖率"
- 正式邮件仅当 `report.complete == True` 时发送

**`src/notification_sender/email_sender.py`** (+282/-17):
- 新增配置引用：EMAIL_MAX_INLINE_BYTES/EMAIL_LONG_CONTENT_MODE/EMAIL_ATTACH_FULL_REPORT
- `send_to_email()` 新增 `report_id` 参数；发送前对 MIME 大小做门禁检查
- 新增 `_split_by_stock_blocks()` — 按股票块无损拆分（不切断股票段落）
- 新增 `_build_mime_message()` / `_build_attachment_mime()` / `_send_attachment_email()` / `_send_single_email()`
- 三种交付模式：inline（短报告）、split（按股票块分片，主题 `[i/N]`）、attachment（附件兜底）
- 每个分片携带 report_id、总分片数、完整报告 SHA-256

**`src/config.py`** (+16/-2):
- 新增 `intraday_llm_batch_size`（默认 0，不分批）
- 新增 `email_max_inline_bytes`（默认 0，不限制）
- 新增 `email_long_content_mode`（默认 "auto"）
- 新增 `email_attach_full_report`（默认 True）

**`.env.example`** (+13/-2):
- 新增 INTRADAY_LLM_BATCH_SIZE / EMAIL_MAX_INLINE_BYTES / EMAIL_LONG_CONTENT_MODE / EMAIL_ATTACH_FULL_REPORT 说明

**`tests/test_intraday_monitor.py`** (+199/-6):
- `TestCallLLM` mock 路径更新
- `TestLLMTruncationDetection`（4 测试）：length/max_tokens/stop/None finish_reason
- `TestReportIntegrityValidation`（6 测试）：全覆盖/缺失股票/截断/缺标记/空内容/归一化匹配
- `TestEmailCoverageDistinction`（1 测试）：邮件同时展示两种覆盖率

## 验证
- 214 passed, 11 new, 0 regression
- 3 预存失败（cache TTL / event_type，与本轮无关）
- 编译检查：OK

---

# Session 2026-07-10: Refinement-Executor 盘中邮件内容完整性修复 Phase 2

## 改动概要

本轮修复 10 个问题（2 Critical + 7 Major + 1 Minor）：finish_reason 字段契约、批次/最终标记校验拆分、结构化股票块解析器、跨批次缺失补偿、邮件无损交付加固、安全默认配置。

## 修改文件

**`src/core/intraday_monitor.py`** (+243/-54):
- 新增 `StockBlockParseResult` dataclass + `parse_stock_blocks()` 解析器（`<!-- STOCK_BEGIN/END:CODE -->` 结构化块）
- 新增 `_get_finish_reasons()` 统一转换，禁止访问不存在的 `finish_reasons`
- `_validate_report_batch()` → `_validate_batch_report()`（只认批次标记）
- 新增 `_validate_merged_report()`（只认最终标记）
- 单批路径经 `_merge_report_batches()` 合并后校验
- `_generate_batched_decision()` 缺失代码改用全局 `accumulated_missing` 跨批次汇总

**`src/notification_sender/email_sender.py`** (+44/-10):
- 新增 `EmailDeliveryResult` dataclass（含 `__bool__` 后向兼容）
- 新增 `_measure_mime_bytes()` 封装
- `send_to_email()` 返回 `EmailDeliveryResult`
- `_build_attachment_mime()` 正文仅短索引，不重复完整 HTML

**`src/config.py`** (+6/-6): batch_size 0→6, max_inline_bytes 0→200000

**`.env.example`** (+4/-4): 同步默认值

**`tests/test_intraday_monitor.py`** (+262): 18 新测试（finish_reason 契约、结构化块解析、标记协议、跨批次补偿）

## 验证
- 233 passed, 3 预存失败（cache TTL / event_type，与本轮无关）
- fallback 14 passed
- 编译检查：OK
