"""Throwaway-Postgres harness for DB-backed regression tests.

Set ``TEST_DATABASE_URL`` to a PostGIS server where the role may CREATE
DATABASE, e.g. ``postgresql://postgres:postgres@localhost:5432/postgres``.
Each test class gets a fresh database migrated with ``alembic upgrade head``
(so the migrations and their triggers are exercised too) and dropped
afterwards. Without ``TEST_DATABASE_URL`` the DB-backed tests are skipped.

Never point this at a real deployment: it creates and drops databases.
"""
from __future__ import annotations

import os
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

BACKEND_DIR = Path(__file__).resolve().parents[1]
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL", "").strip()


def run_alembic(db_url: str, *args: str) -> None:
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(BACKEND_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(BACKEND_DIR / "alembic"))
    prev = os.environ.get("SYNC_DATABASE_URL")
    os.environ["SYNC_DATABASE_URL"] = db_url
    try:
        getattr(command, args[0])(cfg, *args[1:])
    finally:
        if prev is None:
            os.environ.pop("SYNC_DATABASE_URL", None)
        else:
            os.environ["SYNC_DATABASE_URL"] = prev


@unittest.skipUnless(TEST_DATABASE_URL, "TEST_DATABASE_URL not set (DB-backed tests)")
class PostgresTestCase(unittest.TestCase):
    db_url: str = ""
    engine = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        from app.db import session as db_session
        from app.db.session import normalize_sync_url

        cls._admin_url = make_url(normalize_sync_url(TEST_DATABASE_URL))
        cls._db_name = f"utf_test_{uuid.uuid4().hex[:12]}"
        admin = create_engine(cls._admin_url, isolation_level="AUTOCOMMIT")
        with admin.connect() as conn:
            conn.execute(text(f'CREATE DATABASE "{cls._db_name}"'))
        admin.dispose()

        cls.db_url = cls._admin_url.set(database=cls._db_name).render_as_string(
            hide_password=False
        )
        bootstrap = create_engine(cls.db_url)
        with bootstrap.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        bootstrap.dispose()
        run_alembic(cls.db_url, "upgrade", "head")

        cls.engine = create_engine(cls.db_url)
        cls._prev_engine = db_session._sync_engine
        db_session._sync_engine = cls.engine

    @classmethod
    def tearDownClass(cls):
        from app.db import session as db_session

        db_session._sync_engine = cls._prev_engine
        cls.engine.dispose()
        admin = create_engine(cls._admin_url, isolation_level="AUTOCOMMIT")
        with admin.connect() as conn:
            conn.execute(text(f'DROP DATABASE IF EXISTS "{cls._db_name}" WITH (FORCE)'))
        admin.dispose()
        super().tearDownClass()

    # -- fixtures ---------------------------------------------------------

    def session(self):
        from sqlalchemy.orm import Session

        return Session(self.engine, expire_on_commit=False)

    def make_utility(self, name: str, *, state: str = "NH", country: str = "US",
                     website_url: str = "https://utility.example.com"):
        from app.models import Country, Utility, UtilityType

        with self.session() as s:
            u = Utility(
                name=name,
                country=Country(country),
                state_province=state,
                utility_type=UtilityType.IOU,
                website_url=website_url,
            )
            s.add(u)
            s.commit()
            return u.id

    def make_tariff(self, utility_id: int, name: str, components: list[dict], *,
                    rate_type: str = "flat", customer_class: str = "residential",
                    approved: bool = False, confidence_factors: dict | None = None,
                    source_url: str | None = "https://utility.example.com/rates",
                    effective_date=None, verified: bool = True,
                    superseded_by: int | None = None, supersede_reason: str | None = None,
                    openei_id: str | None = None) -> int:
        from app.models import ComponentType, CustomerClass, RateComponent, RateType, Tariff

        with self.session() as s:
            t = Tariff(
                utility_id=utility_id,
                name=name,
                customer_class=CustomerClass(customer_class),
                rate_type=RateType(rate_type),
                approved=approved,
                confidence_factors=confidence_factors,
                source_url=source_url,
                effective_date=effective_date,
                last_verified_at=datetime(2026, 1, 1, tzinfo=timezone.utc) if verified else None,
                superseded_by_tariff_id=superseded_by,
                supersede_reason=supersede_reason,
                openei_id=openei_id,
            )
            for c in components:
                c = dict(c)
                c["component_type"] = ComponentType(c["component_type"])
                c.setdefault("unit", "$/kWh")
                t.rate_components.append(RateComponent(**c))
            s.add(t)
            s.commit()
            return t.id

    def get_tariff(self, tariff_id: int):
        from app.models import Tariff

        with self.session() as s:
            return s.get(Tariff, tariff_id)

    def tariffs_for(self, utility_id: int, *, live_only: bool = False) -> list:
        from sqlalchemy import select
        from app.models import Tariff

        with self.session() as s:
            stmt = select(Tariff).where(Tariff.utility_id == utility_id).order_by(Tariff.id)
            if live_only:
                stmt = stmt.where(
                    Tariff.superseded_by_tariff_id.is_(None),
                    Tariff.supersede_reason.is_(None),
                )
            return list(s.execute(stmt).scalars().all())

    def events_for(self, utility_id: int) -> list:
        from sqlalchemy import select
        from app.models import TariffChangeEvent

        with self.session() as s:
            return list(s.execute(
                select(TariffChangeEvent)
                .where(TariffChangeEvent.utility_id == utility_id)
                .order_by(TariffChangeEvent.id)
            ).scalars().all())


def energy_values(tariff) -> list[float]:
    return sorted(
        float(rc.rate_value) for rc in tariff.rate_components
        if rc.component_type.value == "energy"
    )


def fixed_values(tariff) -> list[float]:
    return sorted(
        float(rc.rate_value) for rc in tariff.rate_components
        if rc.component_type.value == "fixed"
    )
