# -*- coding: utf-8 -*-
"""Tests for Scheduler.add_daily_job() — multi-job daily scheduling."""

from datetime import datetime
import sys
import unittest
from unittest.mock import MagicMock, patch


class _FakeJob:
    """Simulate a schedule library Job."""
    def __init__(self, schedule_module):
        self._schedule_module = schedule_module
        self.next_run = datetime(2026, 1, 1, 18, 0, 0)
        self.at_time = None

    @property
    def day(self):
        return self

    def at(self, value):
        self.at_time = value
        hour, minute = [int(part) for part in value.split(":")]
        self.next_run = datetime(2026, 1, 1, hour, minute, 0)
        return self

    def do(self, fn):
        self.job_func = fn
        self._schedule_module.jobs.append(self)
        return self


class FakeSchedule:
    """Minimal schedule module stub."""
    def __init__(self):
        self.jobs = []

    def every(self):
        return _FakeJob(self)

    def cancel_job(self, job):
        if job in self.jobs:
            self.jobs.remove(job)

    def get_jobs(self):
        return list(self.jobs)

    def run_pending(self):
        pass


class TestAddDailyJob(unittest.TestCase):
    def setUp(self):
        # stub schedule import
        schedule_stub = FakeSchedule()
        self._schedule_patcher = patch.dict(sys.modules, {"schedule": schedule_stub})
        self._schedule_patcher.start()
        import schedule as sched_mod
        sched_mod.every = schedule_stub.every
        sched_mod.cancel_job = schedule_stub.cancel_job
        sched_mod.get_jobs = schedule_stub.get_jobs
        sched_mod.run_pending = schedule_stub.run_pending

        from src.scheduler import Scheduler
        self.scheduler = Scheduler(schedule_time="18:00")

    def tearDown(self):
        self._schedule_patcher.stop()

    def test_add_daily_job_registers(self):
        callback = MagicMock()
        self.scheduler.add_daily_job(callback, "10:00", name="test_job")
        self.assertEqual(len(self.scheduler._daily_jobs), 1)
        self.assertEqual(self.scheduler._daily_jobs[0]["name"], "test_job")
        self.assertEqual(self.scheduler._daily_jobs[0]["time_str"], "10:00")

    def test_add_multiple_jobs(self):
        cb1 = MagicMock()
        cb2 = MagicMock()
        self.scheduler.add_daily_job(cb1, "10:00", name="job1")
        self.scheduler.add_daily_job(cb2, "14:20", name="job2")
        self.assertEqual(len(self.scheduler._daily_jobs), 2)
        times = {e["time_str"] for e in self.scheduler._daily_jobs}
        self.assertIn("10:00", times)
        self.assertIn("14:20", times)

    def test_invalid_time_rejected(self):
        callback = MagicMock()
        with self.assertRaises(ValueError):
            self.scheduler.add_daily_job(callback, "25:00")

    def test_set_daily_task_adds_job(self):
        callback = MagicMock()
        self.scheduler.set_daily_task(callback, run_immediately=False)
        self.assertEqual(len(self.scheduler._daily_jobs), 1)
        self.assertEqual(self.scheduler._daily_jobs[0]["name"], "main_analysis")

    def test_cancel_all_clears_jobs(self):
        cb1 = MagicMock()
        cb2 = MagicMock()
        self.scheduler.add_daily_job(cb1, "10:00")
        self.scheduler.add_daily_job(cb2, "14:20")
        self.assertEqual(len(self.scheduler._daily_jobs), 2)
        self.scheduler._cancel_all_daily_jobs()
        self.assertEqual(len(self.scheduler._daily_jobs), 0)

    def test_job_runner_calls_callback(self):
        callback = MagicMock()
        runner = self.scheduler._make_safe_runner(callback, "test_job")
        runner()
        callback.assert_called_once()

    def test_job_runner_catches_exception(self):
        def failing():
            raise ValueError("test error")
        runner = self.scheduler._make_safe_runner(failing, "failing_job")
        runner()  # should not raise


if __name__ == "__main__":
    unittest.main()
