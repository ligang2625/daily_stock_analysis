# -*- coding: utf-8 -*-
"""Regression tests for scheduled mode stock selection behavior."""

import logging
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tests.litellm_stub import ensure_litellm_stub

ensure_litellm_stub()

import main
from src.config import Config


class _DummyConfig(SimpleNamespace):
    def validate(self):
        return []


class MainScheduleModeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.env_path = Path(self.temp_dir.name) / ".env"
        self.env_path.write_text("STOCK_LIST=600519\n", encoding="utf-8")
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)
        self.env_patch = patch.dict(os.environ, {"ENV_FILE": str(self.env_path)}, clear=False)
        self.env_patch.start()
        Config.reset_instance()
        root_logger = logging.getLogger()
        self._original_root_handlers = list(root_logger.handlers)
        self._original_root_level = root_logger.level

    def tearDown(self) -> None:
        root_logger = logging.getLogger()
        current_handlers = list(root_logger.handlers)
        for handler in current_handlers:
            if handler not in self._original_root_handlers:
                root_logger.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
        root_logger.setLevel(self._original_root_level)
        os.chdir(self.original_cwd)
        Config.reset_instance()
        self.env_patch.stop()
        self.temp_dir.cleanup()

    def _make_args(self, **overrides):
        defaults = {
            "debug": False,
            "stocks": None,
            "webui": False,
            "webui_only": False,
            "serve": False,
            "serve_only": False,
            "host": "0.0.0.0",
            "port": 8000,
            "backtest": False,
            "backtest_code": None,
            "backtest_days": None,
            "backtest_force": False,
            "market_review": False,
            "schedule": False,
            "no_run_immediately": False,
            "no_notify": False,
            "check_notify": False,
            "no_market_review": False,
            "dry_run": False,
            "workers": 1,
            "force_run": False,
            "single_notify": False,
            "no_context_snapshot": False,
            "intraday_snapshot": False,
            "intraday_decision": False,
            "reset_intraday_events": False,
            "analysis_phase": None,
        }
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def _make_config(self, **overrides):
        defaults = {
            "log_dir": self.temp_dir.name,
            "webui_enabled": False,
            "dingtalk_stream_enabled": False,
            "feishu_stream_enabled": False,
            "schedule_enabled": False,
            "schedule_time": "18:00",
            "schedule_run_immediately": True,
            "run_immediately": True,
            "agent_event_monitor_enabled": False,
            "agent_event_alert_rules_json": "",
            "agent_event_monitor_interval_minutes": 5,
        }
        defaults.update(overrides)
        return _DummyConfig(**defaults)

    def test_public_webui_bind_warns_when_auth_is_disabled(self) -> None:
        with patch("src.auth.is_auth_enabled", return_value=False), \
             patch("main.logger.warning") as warning_log:
            main._warn_if_public_webui_without_auth("0.0.0.0")

        warning_log.assert_called_once()
        self.assertIn("WEBUI_HOST=%s", warning_log.call_args.args[0])
        self.assertEqual(warning_log.call_args.args[1], "0.0.0.0")

    def test_loopback_webui_bind_does_not_warn_when_auth_is_disabled(self) -> None:
        with patch("src.auth.is_auth_enabled", return_value=False), \
             patch("main.logger.warning") as warning_log:
            main._warn_if_public_webui_without_auth("127.0.0.1")

        warning_log.assert_not_called()

    def test_schedule_mode_ignores_cli_stock_snapshot(self) -> None:
        args = self._make_args(schedule=True, stocks="600519,000001")
        config = self._make_config(schedule_enabled=False)
        scheduler_mock = MagicMock()
        scheduler_mock.run.return_value = None
        scheduler_mock.set_daily_task.side_effect = lambda task, run_immediately=True: task()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("main.logger.warning") as warning_log, \
             patch("src.scheduler.Scheduler", return_value=scheduler_mock) as scheduler_cls:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        scheduler_cls.assert_called_once()
        self.assertEqual(scheduler_cls.call_args.kwargs["schedule_time"], "18:00")
        scheduler_mock.set_daily_task.assert_called_once()
        self.assertTrue(
            scheduler_mock.set_daily_task.call_args.kwargs["run_immediately"]
        )
        run_full_analysis.assert_called_once_with(config, args, None, analysis_phase="postmarket")
        warning_log.assert_any_call(
            "定时模式下检测到 --stocks 参数；计划执行将忽略启动时股票快照，并在每次运行前重新读取最新的 STOCK_LIST。"
        )

    def test_schedule_mode_reload_uses_latest_runtime_config(self) -> None:
        args = self._make_args(schedule=True)
        startup_config = self._make_config(schedule_enabled=True, schedule_time="18:00")
        runtime_config = self._make_config(schedule_enabled=True, schedule_time="09:30")
        scheduler_mock = MagicMock()
        scheduler_mock.run.return_value = None
        scheduler_mock.set_daily_task.side_effect = lambda task, run_immediately=True: task()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=startup_config), \
             patch("main._reload_runtime_config", return_value=runtime_config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "09:30"), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("src.scheduler.Scheduler", return_value=scheduler_mock) as scheduler_cls:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        scheduler_cls.assert_called_once()
        self.assertEqual(scheduler_cls.call_args.kwargs["schedule_time"], "18:00")
        run_full_analysis.assert_called_once_with(runtime_config, args, None, analysis_phase="postmarket")

    def test_schedule_mode_registers_event_monitor_background_task(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=False,
            agent_event_monitor_enabled=True,
            agent_event_monitor_interval_minutes=7,
        )
        worker = MagicMock()
        worker.run_once.return_value = {"triggered": 2}
        scheduler_mock = MagicMock()
        scheduler_mock.run.return_value = None

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config) as reload_config, \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("src.services.alert_worker.AlertWorker", return_value=worker) as worker_cls, \
             patch("src.scheduler.Scheduler", return_value=scheduler_mock) as scheduler_cls:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        worker_cls.assert_called_once()
        self.assertIs(worker_cls.call_args.kwargs["config_provider"], reload_config)
        run_full_analysis.assert_not_called()
        scheduler_mock.add_background_task.assert_called_once()
        bt_call = scheduler_mock.add_background_task.call_args
        self.assertEqual(bt_call.kwargs["interval_seconds"], 7 * 60)
        self.assertEqual(bt_call.kwargs["name"], "agent_event_monitor")

        with patch("main.logger.info") as info_log:
            bt_call.kwargs["task"]()

        worker.run_once.assert_called_once_with()
        info_log.assert_any_call("[EventMonitor] 本轮触发 %d 条提醒", 2)

    def test_schedule_mode_registers_event_monitor_worker_without_legacy_rules(self) -> None:
        args = self._make_args(schedule=True)
        config = self._make_config(
            schedule_enabled=False,
            agent_event_monitor_enabled=True,
            agent_event_alert_rules_json="",
        )
        worker = MagicMock()
        worker.run_once.return_value = {"triggered": 0}
        scheduler_mock = MagicMock()
        scheduler_mock.run.return_value = None

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch("src.services.alert_worker.AlertWorker", return_value=worker) as worker_cls, \
             patch("src.scheduler.Scheduler", return_value=scheduler_mock) as scheduler_cls:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        worker_cls.assert_called_once()
        run_full_analysis.assert_not_called()
        scheduler_mock.add_background_task.assert_called_once()
        self.assertEqual(
            scheduler_mock.add_background_task.call_args.kwargs["name"],
            "agent_event_monitor",
        )

    def test_check_notify_returns_before_other_modes(self) -> None:
        args = self._make_args(check_notify=True, serve=True, schedule=True, market_review=True)
        config = self._make_config(webui_enabled=False)
        diagnostic_result = SimpleNamespace(ok=True)

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("main.start_api_server") as start_api_server, \
             patch("main.run_full_analysis") as run_full_analysis, \
             patch(
                 "src.services.notification_diagnostics.run_notification_diagnostics",
                 return_value=diagnostic_result,
             ) as run_diagnostics, \
             patch(
                 "src.services.notification_diagnostics.format_notification_diagnostics",
                 return_value="通知配置诊断",
             ), \
             patch("builtins.print") as print_output:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        run_diagnostics.assert_called_once_with(config)
        print_output.assert_called_once_with("通知配置诊断")
        start_api_server.assert_not_called()
        run_full_analysis.assert_not_called()

    def test_reload_runtime_config_preserves_process_env_overrides(self) -> None:
        self.env_path.write_text(
            "OPENAI_API_KEY=stale-file\nSCHEDULE_TIME=09:30\n",
            encoding="utf-8",
        )
        runtime_config = self._make_config(schedule_enabled=True, schedule_time="09:30")

        with patch.dict(
            os.environ,
            {
                "ENV_FILE": str(self.env_path),
                "OPENAI_API_KEY": "runtime-secret",
                "SCHEDULE_TIME": "18:00",
            },
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {"OPENAI_API_KEY": "runtime-secret"},
        ), patch.object(
            main,
            "_RUNTIME_ENV_FILE_KEYS",
            {"SCHEDULE_TIME"},
        ), patch(
            "main.get_config",
            return_value=runtime_config,
        ) as get_config_mock:
            reloaded_config = main._reload_runtime_config()
            self.assertEqual(os.environ["OPENAI_API_KEY"], "runtime-secret")
            self.assertEqual(os.environ["SCHEDULE_TIME"], "09:30")

        self.assertIs(reloaded_config, runtime_config)
        get_config_mock.assert_called_once_with()

    def test_reload_env_file_values_preserves_managed_env_vars_when_read_fails(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV_FILE": str(self.env_path),
                "OPENAI_API_KEY": "runtime-secret",
                "SCHEDULE_TIME": "09:30",
            },
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {},
        ), patch.object(
            main,
            "_RUNTIME_ENV_FILE_KEYS",
            {"OPENAI_API_KEY", "SCHEDULE_TIME"},
        ), patch(
            "main.dotenv_values",
            side_effect=OSError("boom"),
        ):
            main._reload_env_file_values_preserving_overrides()

            self.assertEqual(os.environ["OPENAI_API_KEY"], "runtime-secret")
            self.assertEqual(os.environ["SCHEDULE_TIME"], "09:30")
            self.assertEqual(
                main._RUNTIME_ENV_FILE_KEYS,
                {"OPENAI_API_KEY", "SCHEDULE_TIME"},
            )

    def test_reload_runtime_config_refreshes_env_before_resetting_singleton(self) -> None:
        runtime_config = self._make_config(schedule_enabled=True, schedule_time="09:30")
        call_order = []

        def fake_reload_env() -> None:
            call_order.append("reload_env")

        def fake_reset_instance() -> None:
            call_order.append("reset_instance")

        def fake_get_config():
            call_order.append("get_config")
            return runtime_config

        with patch(
            "main._reload_env_file_values_preserving_overrides",
            side_effect=fake_reload_env,
        ), patch(
            "main.Config.reset_instance",
            side_effect=fake_reset_instance,
        ), patch(
            "main.get_config",
            side_effect=fake_get_config,
        ):
            reloaded_config = main._reload_runtime_config()

        self.assertIs(reloaded_config, runtime_config)
        self.assertEqual(call_order, ["reload_env", "reset_instance", "get_config"])

    def test_schedule_time_provider_propagates_config_read_failures(self) -> None:
        with patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {},
        ), patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            side_effect=RuntimeError("boom"),
        ):
            provider = main._build_schedule_time_provider("18:00")

            with self.assertRaisesRegex(RuntimeError, "boom"):
                provider()

    def test_schedule_time_provider_respects_process_env_precedence(self) -> None:
        with patch.dict(
            os.environ,
            {"SCHEDULE_TIME": "18:00"},
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {"SCHEDULE_TIME": "18:00"},
        ), patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            side_effect=AssertionError("should not read .env when process env override exists"),
        ):
            provider = main._build_schedule_time_provider("09:30")

            self.assertEqual(provider(), "18:00")

    def test_schedule_time_provider_falls_back_to_system_default_on_clear(self) -> None:
        """When SCHEDULE_TIME is cleared/removed from config, provider returns '18:00'."""
        with patch.dict(
            os.environ,
            {"SCHEDULE_TIME": "09:30"},
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {},
        ), patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            return_value={},
        ):
            provider = main._build_schedule_time_provider("09:30")
            self.assertEqual(provider(), "18:00")

    def test_schedule_time_provider_falls_back_to_system_default_on_empty(self) -> None:
        """When SCHEDULE_TIME is empty string in config, provider returns '18:00'."""
        with patch.dict(
            os.environ,
            {"SCHEDULE_TIME": "09:30"},
            clear=False,
        ), patch.object(
            main,
            "_INITIAL_PROCESS_ENV",
            {},
        ), patch(
            "src.core.config_manager.ConfigManager.read_config_map",
            return_value={"SCHEDULE_TIME": "  "},
        ):
            provider = main._build_schedule_time_provider("09:30")
            self.assertEqual(provider(), "18:00")

    def test_single_run_keeps_cli_stock_override(self) -> None:
        args = self._make_args(stocks="600519,000001")
        config = self._make_config(run_immediately=True)

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_full_analysis:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        run_full_analysis.assert_called_once_with(config, args, ["600519", "000001"], analysis_phase="auto")

    def test_run_full_analysis_skips_market_review_when_shared_lock_is_held(self) -> None:
        from src.core.market_review_lock import (
            release_market_review_lock,
            try_acquire_market_review_lock,
        )

        args = self._make_args()
        config = self._make_config(
            trading_day_check_enabled=False,
            market_review_enabled=True,
            no_market_review=False,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []
        events = []

        def refresh_index(config_arg):
            events.append("refresh")

        def build_pipeline(*args, **kwargs):
            events.append("pipeline")
            return pipeline

        lock_token = try_acquire_market_review_lock(config)
        self.assertIsNotNone(lock_token)
        try:
            with patch.object(main, "_refresh_stock_index_cache_for_analysis", side_effect=refresh_index) as refresh, \
                 patch("src.core.pipeline.StockAnalysisPipeline", side_effect=build_pipeline), \
                 patch("src.core.market_review.run_market_review") as run_market_review:
                main.run_full_analysis(config, args, [])
        finally:
            release_market_review_lock(lock_token)

        refresh.assert_called_once_with(config)
        self.assertEqual(events[:2], ["refresh", "pipeline"])
        pipeline.run.assert_called_once()
        run_market_review.assert_not_called()

    def test_config_enabled_schedule_marks_market_review_source_as_schedule(self) -> None:
        args = self._make_args(schedule=False)
        config = self._make_config(
            schedule_enabled=True,
            trading_day_check_enabled=False,
            market_review_enabled=True,
            no_market_review=False,
            single_stock_notify=False,
            merge_email_notification=False,
            analysis_delay=0,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        pipeline = MagicMock()
        pipeline.run.return_value = []

        with patch.object(main, "_refresh_stock_index_cache_for_analysis"), \
             patch.object(main, "_compute_trading_day_filter", return_value=(["600519"], "cn", False)), \
             patch("src.core.pipeline.StockAnalysisPipeline", return_value=pipeline), \
             patch("main._run_market_review_with_shared_lock", return_value="market report") as run_with_lock, \
             patch("src.core.market_review.run_market_review") as run_market_review:
            main.run_full_analysis(config, args, ["600519"])

        pipeline.run.assert_called_once()
        run_with_lock.assert_called_once()
        call_args = run_with_lock.call_args
        self.assertIs(call_args.args[1], run_market_review)
        self.assertEqual(call_args.kwargs["trigger_source"], "schedule")

    def test_market_review_mode_uses_shared_runtime_assembly(self) -> None:
        args = self._make_args(market_review=True)
        config = self._make_config(
            trading_day_check_enabled=True,
            market_review_region="both",
            market_review_enabled=False,
            database_path=str(Path(self.temp_dir.name) / "stock_analysis.db"),
        )
        runtime_notifier = MagicMock()
        runtime_analyzer = MagicMock()
        runtime_search_service = MagicMock()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("main._run_market_review_with_shared_lock") as run_with_lock, \
             patch(
                 "src.core.market_review_runtime.build_market_review_runtime",
                 return_value=(
                    runtime_notifier,
                    runtime_analyzer,
                    runtime_search_service,
                 ),
             ) as runtime_builder, \
             patch("src.core.market_review.run_market_review") as run_market_review, \
             patch("src.core.trading_calendar.get_open_markets_today", return_value={"cn", "us"}), \
             patch("src.core.trading_calendar.compute_effective_region", return_value="cn,us"):
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        runtime_builder.assert_called_once_with(config)
        run_with_lock.assert_called_once()
        call_args = run_with_lock.call_args
        self.assertEqual(call_args.args[0], config)
        self.assertIs(call_args.args[1], run_market_review)
        self.assertIs(call_args.kwargs["notifier"], runtime_notifier)
        self.assertIs(call_args.kwargs["analyzer"], runtime_analyzer)
        self.assertIs(call_args.kwargs["search_service"], runtime_search_service)
        self.assertTrue(call_args.kwargs["send_notification"])
        self.assertNotIn("merge_notification", call_args.kwargs)
        self.assertEqual(call_args.kwargs["override_region"], "cn,us")
        self.assertEqual(call_args.kwargs["trigger_source"], "cli")

    def test_bootstrap_logging_persists_when_config_load_fails(self) -> None:
        """Config load failure must be logged to stderr and return exit code 1.

        Bootstrap logging is stderr-only so healthy runs never write to a
        hard-coded directory.  The error is still captured by process runners
        (e.g. GitHub Actions) that collect stderr output.
        """
        import io

        args = self._make_args()

        capture_stream = io.StringIO()
        capture_handler = logging.StreamHandler(capture_stream)
        capture_handler.setLevel(logging.DEBUG)
        capture_handler.setFormatter(logging.Formatter("%(message)s"))

        root_logger = logging.getLogger()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", side_effect=RuntimeError("config boom")):
            root_logger.addHandler(capture_handler)
            try:
                exit_code = main.main()
            finally:
                root_logger.removeHandler(capture_handler)
                capture_handler.close()

        self.assertEqual(exit_code, 1)
        output = capture_stream.getvalue()
        self.assertIn("加载配置失败", output)
        self.assertIn("config boom", output)

    def test_bootstrap_logging_failure_does_not_block_startup(self) -> None:
        """Bootstrap log dir unwritable must not prevent startup (P1 regression)."""
        args = self._make_args()
        config = self._make_config()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._setup_bootstrap_logging", side_effect=OSError("read-only fs")), \
             patch("main.setup_logging"), \
             patch("main.run_full_analysis") as run_mock:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        run_mock.assert_called_once()

    def test_runtime_file_logging_permission_error_falls_back_to_console(self) -> None:
        """Configured file logging failures should not prevent Docker startup."""
        import io

        args = self._make_args()
        config = self._make_config(log_dir="/app/logs")
        capture_stream = io.StringIO()
        capture_handler = logging.StreamHandler(capture_stream)
        capture_handler.setLevel(logging.DEBUG)
        capture_handler.setFormatter(logging.Formatter("%(message)s"))

        root_logger = logging.getLogger()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch(
                 "main.setup_logging",
                 side_effect=PermissionError("/app/logs/stock_analysis_20260511.log"),
             ), \
             patch("main.run_full_analysis") as run_mock:
            root_logger.addHandler(capture_handler)
            try:
                exit_code = main.main()
            finally:
                root_logger.removeHandler(capture_handler)
                capture_handler.close()

        self.assertEqual(exit_code, 0)
        run_mock.assert_called_once()
        output = capture_stream.getvalue()
        self.assertIn("文件日志初始化失败，已降级为控制台日志输出", output)
        self.assertIn("/app/logs", output)
        self.assertIn("官方 Docker 镜像启动入口会自动修复默认挂载目录权限", output)

    def test_run_full_analysis_import_failure_propagates(self) -> None:
        """P1: import failures in run_full_analysis must propagate, not be swallowed."""
        args = self._make_args()
        config = self._make_config()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch.dict("sys.modules", {"src.core.pipeline": None}):
            exit_code = main.main()

        self.assertEqual(exit_code, 1)

    def test_lazy_pipeline_triggers_env_bootstrap(self) -> None:
        """P2: lazy StockAnalysisPipeline access must call _bootstrap_environment."""
        # Reset the lazy descriptor cache so __get__ fires again
        main._LazyPipelineDescriptor._resolved = None
        main._env_bootstrapped = False

        with patch("main._bootstrap_environment", wraps=main._bootstrap_environment) as mock_boot, \
             patch("src.core.pipeline.StockAnalysisPipeline", create=True, new_callable=lambda: type("FakePipeline", (), {})):
            try:
                _ = main.StockAnalysisPipeline
            except Exception:
                pass
            mock_boot.assert_called()

        # Cleanup: reset state
        main._LazyPipelineDescriptor._resolved = None
        main._env_bootstrapped = False


class IntradaySnapshotDecisionInjectionTestCase(unittest.TestCase):
    """Verify analyzer injection for --intraday-snapshot / --intraday-decision."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.env_path = Path(self.temp_dir.name) / ".env"
        self.env_path.write_text("STOCK_LIST=600519\n", encoding="utf-8")
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)
        self.env_patch = patch.dict(os.environ, {"ENV_FILE": str(self.env_path)}, clear=False)
        self.env_patch.start()
        Config.reset_instance()

    def tearDown(self) -> None:
        os.chdir(self.original_cwd)
        Config.reset_instance()
        self.env_patch.stop()
        self.temp_dir.cleanup()

    def _make_intraday_args(self, snapshot=True):
        return SimpleNamespace(
            debug=False, stocks=None, webui=False, webui_only=False,
            serve=False, serve_only=False, host="0.0.0.0", port=8000,
            backtest=False, market_review=False, schedule=False,
            no_run_immediately=False, no_notify=False, check_notify=False,
            no_market_review=False, dry_run=False, workers=1,
            force_run=False, single_notify=False, no_context_snapshot=False,
            intraday_snapshot=snapshot,
            intraday_decision=not snapshot,
        )

    def test_snapshot_mode_creates_monitor_without_analyzer(self):
        args = self._make_intraday_args(snapshot=True)
        config = _DummyConfig(log_dir=self.temp_dir.name, webui_enabled=False)
        config.validate = lambda: []

        monitor_mock = MagicMock()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("src.storage.DatabaseManager") as db_cls, \
             patch("data_provider.base.DataFetcherManager") as fetcher_cls, \
             patch("src.notification_sender.email_sender.EmailSender") as email_cls, \
             patch("src.core.intraday_monitor.IntradayMonitor", return_value=monitor_mock) as monitor_cls:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        monitor_cls.assert_called_once()
        call_kwargs = monitor_cls.call_args.kwargs
        self.assertIsNone(call_kwargs["llm_analyzer"],
                          "Snapshot mode should NOT inject an LLM analyzer")
        monitor_mock.run_one_shot_snapshot.assert_called_once()

    def test_decision_mode_creates_monitor_with_analyzer(self):
        args = self._make_intraday_args(snapshot=False)
        config = _DummyConfig(log_dir=self.temp_dir.name, webui_enabled=False)
        config.validate = lambda: []

        monitor_mock = MagicMock()
        mock_analyzer = MagicMock()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main.setup_logging"), \
             patch("src.storage.DatabaseManager") as db_cls, \
             patch("data_provider.base.DataFetcherManager") as fetcher_cls, \
             patch("src.notification_sender.email_sender.EmailSender") as email_cls, \
             patch("src.analyzer.GeminiAnalyzer", return_value=mock_analyzer) as analyzer_cls, \
             patch("src.core.intraday_monitor.IntradayMonitor", return_value=monitor_mock) as monitor_cls:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        analyzer_cls.assert_called_once_with(config=config)
        monitor_cls.assert_called_once()
        call_kwargs = monitor_cls.call_args.kwargs
        self.assertIs(call_kwargs["llm_analyzer"], mock_analyzer,
                      "Decision mode should inject an LLM analyzer")
        monitor_mock.run_one_shot_decision.assert_called_once()

    def test_schedule_mode_creates_intraday_monitor_with_analyzer(self):
        args = SimpleNamespace(
            debug=False, stocks=None, webui=False, webui_only=False,
            serve=False, serve_only=False, host="0.0.0.0", port=8000,
            backtest=False, market_review=False, schedule=True,
            no_run_immediately=True, no_notify=False, check_notify=False,
            no_market_review=False, dry_run=False, workers=1,
            force_run=False, single_notify=False, no_context_snapshot=False,
            intraday_snapshot=False,
            intraday_decision=False,
            reset_intraday_events=False,
        )
        config = _DummyConfig(
            log_dir=self.temp_dir.name,
            webui_enabled=False,
            schedule_enabled=False,
            schedule_time="18:00",
            schedule_run_immediately=False,
            intraday_monitor_enabled=True,
            agent_event_monitor_enabled=False,
        )
        config.validate = lambda: []

        monitor_mock = MagicMock()
        mock_analyzer = MagicMock()
        scheduler_mock = MagicMock()
        scheduler_mock.run.return_value = None
        scheduler_mock.add_daily_job = MagicMock()

        with patch("main.parse_arguments", return_value=args), \
             patch("main.get_config", return_value=config), \
             patch("main._reload_runtime_config", return_value=config), \
             patch("main._build_schedule_time_provider", return_value=lambda: "18:00"), \
             patch("main.setup_logging"), \
             patch("src.storage.DatabaseManager") as db_cls, \
             patch("data_provider.base.DataFetcherManager") as fetcher_cls, \
             patch("src.notification_sender.email_sender.EmailSender") as email_cls, \
             patch("src.analyzer.GeminiAnalyzer", return_value=mock_analyzer) as analyzer_cls, \
             patch("src.core.intraday_monitor.IntradayMonitor", return_value=monitor_mock) as monitor_cls, \
             patch("src.scheduler.Scheduler", return_value=scheduler_mock) as scheduler_cls:
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        analyzer_cls.assert_called_once_with(config=config)
        monitor_cls.assert_called_once()
        call_kwargs = monitor_cls.call_args.kwargs
        self.assertIs(call_kwargs["llm_analyzer"], mock_analyzer,
                      "Schedule mode should inject an LLM analyzer for intraday monitor")
        scheduler_cls.assert_called_once()
        scheduler_mock.run.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
