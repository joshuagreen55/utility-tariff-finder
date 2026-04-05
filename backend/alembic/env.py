import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

from app.config import settings
from app.db.base import Base
from app.models import *  # noqa: F401,F403

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

EXCLUDE_TABLES = {"spatial_ref_sys"}


def include_object(object, name, type_, reflected, compare_to):
    if type_ == "table" and name in EXCLUDE_TABLES:
        return False
    return True


def _sync_db_url() -> str:
    return (os.environ.get("SYNC_DATABASE_URL") or settings.sync_database_url).strip()


def run_migrations_offline() -> None:
    url = _sync_db_url() or config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    url = _sync_db_url()
    if not url:
        raise RuntimeError("SYNC_DATABASE_URL or settings.sync_database_url must be set for migrations")
    connectable = create_engine(url, poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, include_object=include_object)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
