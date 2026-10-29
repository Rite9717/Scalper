"""Apply the SQL files shipped in scalper.data.sql."""

from __future__ import annotations

from importlib.resources import files

import asyncpg


def split_sql(script: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    for line in script.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        buffer.append(line)
        if stripped.endswith(";"):
            statement = "\n".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
    trailing = "\n".join(buffer).strip()
    if trailing:
        statements.append(trailing)
    return statements


def migration_scripts() -> list[tuple[str, str]]:
    package = files("scalper.data.sql")
    found: list[tuple[str, str]] = []
    for path in sorted(package.iterdir(), key=lambda item: item.name):
        if not path.name.endswith(".sql"):
            continue
        found.append((path.name, path.read_text(encoding="utf-8")))
    return found


async def apply_migrations(dsn: str) -> list[str]:
    connection = await asyncpg.connect(dsn, timeout=10)
    applied: list[str] = []
    try:
        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version text PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        rows = await connection.fetch("SELECT version FROM schema_migrations")
        seen = {row["version"] for row in rows}
        for name, script in migration_scripts():
            if name in seen:
                continue
            async with connection.transaction():
                for statement in split_sql(script):
                    await connection.execute(statement)
                await connection.execute(
                    "INSERT INTO schema_migrations (version) VALUES ($1)",
                    name,
                )
            applied.append(name)
    finally:
        await connection.close()
    return applied
