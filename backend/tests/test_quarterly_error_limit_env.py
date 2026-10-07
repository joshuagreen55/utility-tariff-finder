"""QUARTERLY_ERROR_LIMIT must be env-configurable (default 200).

The VM holds the quarterly recovery job at 0 via .env through October
2026; a hard-coded constant would silently lose that on a clean deploy.

    cd backend && python -m unittest tests.test_quarterly_error_limit_env -v
"""
from __future__ import annotations

import importlib
import os
import unittest
from unittest import mock


class TestQuarterlyErrorLimitEnv(unittest.TestCase):
    def tearDown(self):
        os.environ.pop("QUARTERLY_ERROR_LIMIT", None)
        from app.tasks import refresh

        importlib.reload(refresh)

    def test_default_is_200_when_unset(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("QUARTERLY_ERROR_LIMIT", None)
            from app.tasks import refresh

            mod = importlib.reload(refresh)
            self.assertEqual(mod.QUARTERLY_ERROR_LIMIT, 200)

    def test_env_override_zero_holds_the_job(self):
        with mock.patch.dict(os.environ, {"QUARTERLY_ERROR_LIMIT": "0"}):
            from app.tasks import refresh

            mod = importlib.reload(refresh)
            self.assertEqual(mod.QUARTERLY_ERROR_LIMIT, 0)

    def test_env_override_custom_positive(self):
        with mock.patch.dict(os.environ, {"QUARTERLY_ERROR_LIMIT": "50"}):
            from app.tasks import refresh

            mod = importlib.reload(refresh)
            self.assertEqual(mod.QUARTERLY_ERROR_LIMIT, 50)


if __name__ == "__main__":
    unittest.main()
