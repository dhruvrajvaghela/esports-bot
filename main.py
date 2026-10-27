#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ==================================================================================================
# AEGIS ESPORTS - Universal Esports Discord Bot (single-file edition)
#
# Consolidated from the multi-file project (main.py, bot.py, config.py, database/, services/, ui/, cogs/, utils/)
# into one self-contained, section-based module. Behaviour, commands, schema and settings are unchanged.
#
# Stack     : Python 3.11+, discord.py 2.x (interaction/slash based), SQLite (stdlib sqlite3), python-dotenv
# Run       : python main.py        (needs DISCORD_BOT_TOKEN; see .env.example)
#
# CONTENTS
#   1. File header and project information
#   2. Imports and dependency handling
#   3. Configuration and environment variables
#   4. Database and data-storage layer (schema, migrations, async SQLite wrapper, backups)
#   5. Bot initialization and intents (AegisBot, command tree, central error reporting)
#   6. Helper functions, validators, permission checks and the service layer
#   7. Embeds and reusable UI components (buttons, selects, modals, views, announcements)
#   8. Commands organized by category (cogs): Home, Teams, Players, Tournaments, Scrims, Registrations, Matches,
#      Leaderboards, Staff, Reports, Community, Admin panel, Owner panel
#   9. Slash commands: /aegis, /panel (Home), /profile (Players), /evidence (Matches). Everything else is a
#      button/select/modal-driven panel opened from /aegis; this bot has no prefix commands, so there are no
#      prefix aliases and no command groups to merge (items 9-10 of the template are fully covered by the cogs).
#  11. Buttons, select menus, modals and interactive panels (section 7 + the cogs)
#  12. Event/interaction handlers and persistent (restart-safe) components (EventButton DynamicItem)
#  13. Background tasks and scheduled jobs (Tasks cog) and the COGS registry
#  14. Error handlers and logging (AegisTree.on_error, AegisBot.report_error, logging setup)
#  15. Startup and bot execution
# ==================================================================================================
"""AEGIS ESPORTS - single-file edition. See the header comment for the section map."""
from __future__ import annotations

# ==================================================================================================
# SECTION 2 - IMPORTS AND DEPENDENCY HANDLING
# ==================================================================================================
# Standard library
import asyncio
import csv
import io
import json
import logging
import os
import platform
import random
import re
import secrets
import shutil
import signal
import sqlite3
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, TypeVar
from urllib.parse import urlparse

# Third-party (requirements.txt: discord.py>=2.4,<3  python-dotenv>=1.0  aiohttp>=3.9)
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv


# ==================================================================================================
# SECTION 3 - CONFIGURATION AND ENVIRONMENT VARIABLES
# ==================================================================================================

# --------------------------------------------------------------------------------------------------
# config.py
# Environment-driven configuration. Secrets are never hardcoded.
# --------------------------------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent


class ConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class Config:
    token: str
    owner_id: int
    database_path: str
    prefix: str
    log_level: str

    @property
    def data_dir(self) -> Path:
        return Path(self.database_path).resolve().parent

    @property
    def backup_dir(self) -> Path:
        return self.data_dir / "backups"

    @property
    def log_dir(self) -> Path:
        return self.data_dir / "logs"

    @classmethod
    def from_env(cls, require_token: bool = True) -> "Config":
        load_dotenv(BASE_DIR / ".env")
        token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
        if require_token and not token:
            raise ConfigError(
                "DISCORD_BOT_TOKEN is missing. Copy .env.example to .env and put your bot token in it."
            )
        owner_raw = os.getenv("OWNER_ID", "").strip()
        owner_id = 0
        if owner_raw:
            if not owner_raw.isdigit():
                raise ConfigError("OWNER_ID must be a numeric Discord user ID.")
            owner_id = int(owner_raw)
        db_path = os.getenv("DATABASE_PATH", "data/aegis.sqlite3").strip() or "data/aegis.sqlite3"
        if db_path != ":memory:" and not os.path.isabs(db_path):
            db_path = str(BASE_DIR / db_path)
        level = os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"
        if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            level = "INFO"
        prefix = os.getenv("DEFAULT_PREFIX", "!").strip() or "!"
        return cls(token=token, owner_id=owner_id, database_path=db_path, prefix=prefix, log_level=level)


# ==================================================================================================
# SECTION 4 - DATABASE AND DATA-STORAGE LAYER
# ==================================================================================================

# --------------------------------------------------------------------------------------------------
# database/schema.py
# SQLite schema, version 1. All statements are idempotent (IF NOT EXISTS).
# --------------------------------------------------------------------------------------------------

SCHEMA_V1: list[str] = [
    """CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS bot_state (
        key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS guild_settings (
        guild_id INTEGER NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, key))""",
    """CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS player_profiles (
        guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
        ign TEXT NOT NULL, game_uid TEXT, role TEXT, region TEXT, language TEXT,
        games TEXT NOT NULL DEFAULT '[]', achievements TEXT,
        status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','LFT','INACTIVE')),
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id))""",
    """CREATE TABLE IF NOT EXISTS teams (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
        name TEXT NOT NULL, name_key TEXT NOT NULL, tag TEXT, logo_url TEXT, game TEXT,
        captain_id INTEGER NOT NULL, verified INTEGER NOT NULL DEFAULT 0, verified_by INTEGER,
        recruiting INTEGER NOT NULL DEFAULT 0, disbanded INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_teams_name ON teams(guild_id, name_key) WHERE disbanded = 0",
    "CREATE INDEX IF NOT EXISTS ix_teams_guild ON teams(guild_id, disbanded)",
    """CREATE TABLE IF NOT EXISTS team_members (
        team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
        guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
        role TEXT NOT NULL CHECK (role IN ('captain','cocaptain','player','sub')),
        joined_at TEXT NOT NULL, PRIMARY KEY (team_id, user_id))""",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_member_guild ON team_members(guild_id, user_id)",
    """CREATE TABLE IF NOT EXISTS team_invites (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
        team_id INTEGER NOT NULL REFERENCES teams(id) ON DELETE CASCADE, user_id INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('invite','request','transfer')),
        status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','ACCEPTED','DECLINED','CANCELLED')),
        created_by INTEGER NOT NULL, created_at TEXT NOT NULL, resolved_at TEXT)""",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_invite_pending ON team_invites(team_id, user_id, kind) WHERE status = 'PENDING'",
    "CREATE INDEX IF NOT EXISTS ix_invite_user ON team_invites(user_id, status)",
    """CREATE TABLE IF NOT EXISTS score_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, name TEXT NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('br','versus')),
        placement_points TEXT NOT NULL DEFAULT '[]', kill_points REAL NOT NULL DEFAULT 1,
        win_points REAL NOT NULL DEFAULT 3, draw_points REAL NOT NULL DEFAULT 1,
        loss_points REAL NOT NULL DEFAULT 0, round_points REAL NOT NULL DEFAULT 0,
        tiebreak TEXT NOT NULL DEFAULT '["points","kills","best_placement"]',
        is_default INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
        UNIQUE (guild_id, name))""",
    """CREATE TABLE IF NOT EXISTS tournaments (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
        title TEXT NOT NULL, game TEXT NOT NULL,
        mode TEXT NOT NULL CHECK (mode IN ('solo','duo','squad')), team_size INTEGER NOT NULL,
        map TEXT, region TEXT, start_time TEXT, entry_fee TEXT, prize TEXT,
        max_teams INTEGER NOT NULL DEFAULT 16, reserve_slots INTEGER NOT NULL DEFAULT 0,
        reg_open TEXT, reg_close TEXT, publish_at TEXT,
        status TEXT NOT NULL DEFAULT 'DRAFT'
            CHECK (status IN ('DRAFT','SCHEDULED','PUBLISHED','LIVE','COMPLETED','CANCELLED')),
        rules TEXT, poster_url TEXT, organizer_id INTEGER NOT NULL, organizer_info TEXT,
        score_rule_id INTEGER REFERENCES score_rules(id) ON DELETE SET NULL, season TEXT,
        channel_id INTEGER, message_id INTEGER, reminded INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS ix_tour_guild ON tournaments(guild_id, status)",
    """CREATE TABLE IF NOT EXISTS tournament_registrations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tournament_id INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
        team_id INTEGER NOT NULL REFERENCES teams(id),
        status TEXT NOT NULL CHECK (status IN ('PENDING','APPROVED','REJECTED','WAITLIST','WITHDRAWN')),
        checked_in INTEGER NOT NULL DEFAULT 0, registered_by INTEGER, note TEXT,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        UNIQUE (tournament_id, team_id))""",
    """CREATE TABLE IF NOT EXISTS tournament_reg_players (
        reg_id INTEGER NOT NULL REFERENCES tournament_registrations(id) ON DELETE CASCADE,
        user_id INTEGER NOT NULL, is_sub INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (reg_id, user_id))""",
    "CREATE INDEX IF NOT EXISTS ix_trp_user ON tournament_reg_players(user_id)",
    """CREATE TABLE IF NOT EXISTS scrims (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
        title TEXT NOT NULL, game TEXT NOT NULL,
        mode TEXT NOT NULL CHECK (mode IN ('solo','duo','squad')), team_size INTEGER NOT NULL,
        tier TEXT, map TEXT, fee TEXT, start_time TEXT,
        lobby_count INTEGER NOT NULL DEFAULT 1, lobby_capacity INTEGER NOT NULL DEFAULT 20,
        status TEXT NOT NULL DEFAULT 'DRAFT'
            CHECK (status IN ('DRAFT','PUBLISHED','PAUSED','CANCELLED','COMPLETED')),
        organizer_id INTEGER NOT NULL,
        score_rule_id INTEGER REFERENCES score_rules(id) ON DELETE SET NULL, season TEXT,
        channel_id INTEGER, message_id INTEGER, reminded INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS ix_scrim_guild ON scrims(guild_id, status)",
    """CREATE TABLE IF NOT EXISTS lobbies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scrim_id INTEGER NOT NULL REFERENCES scrims(id) ON DELETE CASCADE,
        number INTEGER NOT NULL, capacity INTEGER NOT NULL, start_time TEXT,
        UNIQUE (scrim_id, number))""",
    """CREATE TABLE IF NOT EXISTS scrim_registrations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scrim_id INTEGER NOT NULL REFERENCES scrims(id) ON DELETE CASCADE,
        team_id INTEGER NOT NULL REFERENCES teams(id),
        status TEXT NOT NULL CHECK (status IN ('PENDING','APPROVED','REJECTED','WAITLIST','WITHDRAWN')),
        checked_in INTEGER NOT NULL DEFAULT 0, no_show INTEGER NOT NULL DEFAULT 0,
        lobby_id INTEGER REFERENCES lobbies(id) ON DELETE SET NULL,
        registered_by INTEGER, note TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        UNIQUE (scrim_id, team_id))""",
    """CREATE TABLE IF NOT EXISTS scrim_reg_players (
        reg_id INTEGER NOT NULL REFERENCES scrim_registrations(id) ON DELETE CASCADE,
        user_id INTEGER NOT NULL, is_sub INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (reg_id, user_id))""",
    "CREATE INDEX IF NOT EXISTS ix_srp_user ON scrim_reg_players(user_id)",
    """CREATE TABLE IF NOT EXISTS matches (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('tournament','scrim')), event_id INTEGER NOT NULL,
        lobby_id INTEGER REFERENCES lobbies(id) ON DELETE CASCADE,
        number INTEGER NOT NULL, map TEXT, scheduled_at TEXT,
        room_id TEXT, room_password TEXT, room_released INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'SCHEDULED' CHECK (status IN ('SCHEDULED','COMPLETED','CANCELLED')),
        created_at TEXT NOT NULL)""",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_match ON matches(kind, event_id, COALESCE(lobby_id, 0), number)",
    """CREATE TABLE IF NOT EXISTS match_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        match_id INTEGER NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
        team_id INTEGER NOT NULL REFERENCES teams(id),
        placement INTEGER, kills INTEGER NOT NULL DEFAULT 0, bonus REAL NOT NULL DEFAULT 0,
        score_for INTEGER, score_against INTEGER,
        status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','APPROVED','REJECTED','DISPUTED')),
        submitted_by INTEGER NOT NULL, evidence TEXT NOT NULL DEFAULT '[]', notes TEXT,
        reviewed_by INTEGER, review_reason TEXT, reviewed_at TEXT,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        UNIQUE (match_id, team_id))""",
    "CREATE INDEX IF NOT EXISTS ix_results_status ON match_results(status)",
    """CREATE TABLE IF NOT EXISTS standings (
        kind TEXT NOT NULL CHECK (kind IN ('tournament','scrim')), event_id INTEGER NOT NULL,
        team_id INTEGER NOT NULL REFERENCES teams(id),
        rank INTEGER NOT NULL, played INTEGER NOT NULL, points REAL NOT NULL, kills INTEGER NOT NULL,
        wins INTEGER NOT NULL, best_placement INTEGER, round_diff INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (kind, event_id, team_id))""",
    """CREATE TABLE IF NOT EXISTS staff_members (
        guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
        role TEXT NOT NULL CHECK (role IN ('staff','referee','organizer','scrim_manager')),
        granted_by INTEGER NOT NULL, granted_at TEXT NOT NULL,
        PRIMARY KEY (guild_id, user_id, role))""",
    """CREATE TABLE IF NOT EXISTS staff_duty (
        guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL, on_duty INTEGER NOT NULL DEFAULT 0,
        since TEXT NOT NULL, PRIMARY KEY (guild_id, user_id))""",
    """CREATE TABLE IF NOT EXISTS staff_applications (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
        role_wanted TEXT NOT NULL, reason TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','APPROVED','REJECTED')),
        reviewed_by INTEGER, review_note TEXT, created_at TEXT NOT NULL, reviewed_at TEXT)""",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_app_pending ON staff_applications(guild_id, user_id) WHERE status = 'PENDING'",
    """CREATE TABLE IF NOT EXISTS referee_assignments (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('tournament','scrim')), event_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL, assigned_by INTEGER NOT NULL, created_at TEXT NOT NULL,
        UNIQUE (kind, event_id, user_id))""",
    """CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('player','team','dispute','appeal','registration','incident','other')),
        reporter_id INTEGER NOT NULL, target TEXT, description TEXT NOT NULL,
        evidence TEXT NOT NULL DEFAULT '[]',
        result_id INTEGER REFERENCES match_results(id) ON DELETE SET NULL,
        status TEXT NOT NULL DEFAULT 'OPEN'
            CHECK (status IN ('OPEN','IN_REVIEW','NEED_EVIDENCE','RESOLVED','REOPENED')),
        assigned_to INTEGER, resolution TEXT,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL, resolved_at TEXT)""",
    "CREATE INDEX IF NOT EXISTS ix_reports_guild ON reports(guild_id, status)",
    "CREATE INDEX IF NOT EXISTS ix_reports_reporter ON reports(reporter_id)",
    """CREATE TABLE IF NOT EXISTS report_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_id INTEGER NOT NULL REFERENCES reports(id) ON DELETE CASCADE,
        author_id INTEGER NOT NULL, note TEXT NOT NULL, internal INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, actor_id INTEGER NOT NULL,
        action TEXT NOT NULL, target TEXT, detail TEXT, created_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS ix_audit_guild ON audit_logs(guild_id, id)",
    "CREATE INDEX IF NOT EXISTS ix_audit_target ON audit_logs(guild_id, target)",
    """CREATE TABLE IF NOT EXISTS announcements (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, kind TEXT NOT NULL,
        title TEXT NOT NULL, body TEXT NOT NULL, channel_id INTEGER, message_id INTEGER,
        author_id INTEGER NOT NULL, created_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS recruitment_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('team','player')), user_id INTEGER NOT NULL,
        team_id INTEGER REFERENCES teams(id) ON DELETE SET NULL,
        game TEXT, role TEXT, region TEXT, body TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS ix_recruit_guild ON recruitment_posts(guild_id, kind, active)",
    """CREATE TABLE IF NOT EXISTS polls (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, question TEXT NOT NULL,
        options TEXT NOT NULL, channel_id INTEGER, message_id INTEGER,
        closed INTEGER NOT NULL DEFAULT 0, creator_id INTEGER NOT NULL, created_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS poll_votes (
        poll_id INTEGER NOT NULL REFERENCES polls(id) ON DELETE CASCADE,
        user_id INTEGER NOT NULL, option_idx INTEGER NOT NULL, PRIMARY KEY (poll_id, user_id))""",
    """CREATE TABLE IF NOT EXISTS giveaways (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, prize TEXT NOT NULL,
        ends_at TEXT NOT NULL, winners INTEGER NOT NULL DEFAULT 1,
        require_team INTEGER NOT NULL DEFAULT 0, require_profile INTEGER NOT NULL DEFAULT 0,
        channel_id INTEGER, message_id INTEGER, host_id INTEGER NOT NULL,
        ended INTEGER NOT NULL DEFAULT 0, winner_ids TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS ix_giveaway_due ON giveaways(ended, ends_at)",
    """CREATE TABLE IF NOT EXISTS giveaway_entries (
        giveaway_id INTEGER NOT NULL REFERENCES giveaways(id) ON DELETE CASCADE,
        user_id INTEGER NOT NULL, PRIMARY KEY (giveaway_id, user_id))""",
    """CREATE TABLE IF NOT EXISTS badges (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
        user_id INTEGER, team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
        badge TEXT NOT NULL, note TEXT, awarded_by INTEGER NOT NULL, created_at TEXT NOT NULL,
        CHECK (user_id IS NOT NULL OR team_id IS NOT NULL))""",
    """CREATE TABLE IF NOT EXISTS hall_of_fame (
        id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL, season TEXT,
        title TEXT NOT NULL, winner TEXT NOT NULL, note TEXT, created_by INTEGER NOT NULL,
        created_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS persistent_panel_settings (
        guild_id INTEGER NOT NULL, panel TEXT NOT NULL, channel_id INTEGER NOT NULL,
        message_id INTEGER NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY (guild_id, panel))""",
]

# Tables every valid backup must contain.
REQUIRED_TABLES = ("schema_version", "guild_settings", "teams", "tournaments", "scrims", "match_results", "audit_logs")

# --------------------------------------------------------------------------------------------------
# database/migrations.py
# Versioned, transactional schema migrations.
# --------------------------------------------------------------------------------------------------

log_migrations = logging.getLogger("aegis.db.migrations")

# (version, statements). Append new entries; never edit an applied migration.
MIGRATIONS: list[tuple[int, list[str]]] = [
    (1, SCHEMA_V1),
]


def latest_version(migrations: list[tuple[int, list[str]]] | None = None) -> int:
    return max(v for v, _ in (migrations or MIGRATIONS))


def current_version(conn: sqlite3.Connection) -> int:
    try:
        row = conn.execute("SELECT MAX(version) AS v FROM schema_version").fetchone()
    except sqlite3.OperationalError:
        return 0
    value = row["v"] if isinstance(row, dict) else row[0]
    return int(value or 0)


def migrate(conn: sqlite3.Connection, migrations: list[tuple[int, list[str]]] | None = None) -> int:
    """Apply pending migrations, each inside its own transaction. Idempotent."""
    migrations = sorted(migrations or MIGRATIONS, key=lambda m: m[0])
    current = current_version(conn)
    for version, statements in migrations:
        if version <= current:
            continue
        log_migrations.info("applying migration v%s", version)
        conn.execute("BEGIN IMMEDIATE")
        try:
            for stmt in statements:
                conn.execute(stmt)
            conn.execute("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
            conn.execute("INSERT INTO schema_version(version, applied_at) VALUES (?, ?)", (version, now_iso()))
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            log_migrations.exception("migration v%s failed and was rolled back", version)
            raise
        current = version
    return current

# --------------------------------------------------------------------------------------------------
# database/db.py
# Thread-safe SQLite wrapper. Blocking sqlite3 calls run in a worker thread so the
# asyncio event loop is never blocked. Service code is written as plain functions that
# take a connection and are executed through Database.run / Database.read.
# --------------------------------------------------------------------------------------------------

log_db = logging.getLogger("aegis.db")
T = TypeVar("T")


def _dict_factory(cursor: sqlite3.Cursor, row: tuple) -> dict[str, Any]:
    return {col[0]: row[i] for i, col in enumerate(cursor.description)}


class BackupError(RuntimeError):
    pass


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ lifecycle
    def _open(self) -> sqlite3.Connection:
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None, timeout=30)
        conn.row_factory = _dict_factory
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")
        if self.path != ":memory:":
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    def connect_sync(self) -> int:
        with self._lock:
            if self._conn is None:
                self._conn = self._open()
            return migrate(self._conn)

    async def connect(self) -> int:
        return await asyncio.to_thread(self.connect_sync)

    def close_sync(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                finally:
                    self._conn = None

    async def close(self) -> None:
        await asyncio.to_thread(self.close_sync)

    # ------------------------------------------------------------------ execution
    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        return self._conn

    def run_sync(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        """Run fn inside one IMMEDIATE transaction; roll back on any exception."""
        with self._lock:
            conn = self.conn
            conn.execute("BEGIN IMMEDIATE")
            try:
                result = fn(conn)
                conn.execute("COMMIT")
                return result
            except BaseException:
                try:
                    conn.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise

    def read_sync(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        with self._lock:
            return fn(self.conn)

    async def run(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return await asyncio.to_thread(self.run_sync, fn)

    async def read(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        return await asyncio.to_thread(self.read_sync, fn)

    # ------------------------------------------------------------------ info
    def status_sync(self) -> dict[str, Any]:
        def _q(conn: sqlite3.Connection) -> dict[str, Any]:
            integrity = conn.execute("PRAGMA integrity_check").fetchone()["integrity_check"]
            version = conn.execute("SELECT MAX(version) AS v FROM schema_version").fetchone()["v"]
            counts = {}
            for t in ("teams", "tournaments", "scrims", "match_results", "reports", "audit_logs"):
                counts[t] = conn.execute(f"SELECT COUNT(*) AS c FROM {t}").fetchone()["c"]  # noqa: S608 (fixed names)
            return {"integrity": integrity, "schema_version": version, "latest_version": latest_version(), "counts": counts}

        info = self.read_sync(_q)
        size = os.path.getsize(self.path) if self.path != ":memory:" and os.path.exists(self.path) else 0
        info["size_bytes"] = size
        return info

    async def status(self) -> dict[str, Any]:
        return await asyncio.to_thread(self.status_sync)

    # ------------------------------------------------------------------ backup / restore
    def backup_sync(self, dest: str | Path) -> Path:
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        if tmp.exists():
            tmp.unlink()
        with self._lock:
            target = sqlite3.connect(str(tmp))
            try:
                self.conn.backup(target)
            finally:
                target.close()
        validate_backup(tmp)  # never keep a backup that does not validate
        os.replace(tmp, dest)
        return dest

    async def backup(self, dest: str | Path) -> Path:
        return await asyncio.to_thread(self.backup_sync, dest)

    def restore_sync(self, source: str | Path, safety_dir: str | Path | None = None) -> Path | None:
        """Validate `source`, snapshot the live DB (safety copy), then replace the live DB with it."""
        source = Path(source)
        validate_backup(source)
        if self.path == ":memory:":
            raise BackupError("Cannot restore into an in-memory database.")
        safety: Path | None = None
        with self._lock:
            if self._conn is not None and safety_dir is not None:
                safety = Path(safety_dir) / f"pre-restore-{now_iso().replace(':', '')}.sqlite3"
                self.backup_sync(safety)
            self.close_sync()
            tmp = Path(self.path).with_suffix(".restore.tmp")
            shutil.copyfile(source, tmp)
            for suffix in ("-wal", "-shm"):
                p = Path(self.path + suffix)
                if p.exists():
                    p.unlink()
            os.replace(tmp, self.path)
            self.connect_sync()  # reopen + run any pending migrations
        return safety

    async def restore(self, source: str | Path, safety_dir: str | Path | None = None) -> Path | None:
        return await asyncio.to_thread(self.restore_sync, source, safety_dir)


def validate_backup(path: str | Path) -> None:
    """Raise BackupError unless `path` is a healthy SQLite file this bot version can use."""
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        raise BackupError("Backup file is missing or empty.")
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        raise BackupError(f"Cannot open backup: {exc}") from exc
    try:
        try:
            ok = conn.execute("PRAGMA integrity_check").fetchone()[0]
        except sqlite3.DatabaseError as exc:
            raise BackupError(f"Not a valid SQLite database: {exc}") from exc
        if ok != "ok":
            raise BackupError(f"Integrity check failed: {ok}")
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        missing = [t for t in REQUIRED_TABLES if t not in tables]
        if missing:
            raise BackupError(f"Backup is missing required tables: {', '.join(missing)}")
        version = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] or 0
        if version > latest_version():
            raise BackupError(f"Backup schema v{version} is newer than this bot supports (v{latest_version()}).")
    finally:
        conn.close()


# ==================================================================================================
# SECTION 5 - BOT INITIALIZATION AND INTENTS
# ==================================================================================================

# --------------------------------------------------------------------------------------------------
# bot.py
# AegisBot: shared services for every cog (database, permissions, branding, gating, error reporting).
# --------------------------------------------------------------------------------------------------

log_bot = logging.getLogger("aegis.bot")

COMMANDS_VERSION = "1"  # bump when slash-command definitions change; triggers a re-sync on next start
# NOTE: the cog registry (COGS) lives at the end of section 13, once every cog class is defined.

# entry name -> module that must be enabled (None = always available)
ENTRY_MODULE = {
    "tournaments": "tournaments", "scrims": "scrims", "team": "teams", "player": "players",
    "leaderboards": "leaderboards", "report": "reports", "staff": "staff", "community": "community",
}

Entry = Callable[[discord.Interaction], Awaitable[None]]


class AegisTree(app_commands.CommandTree):
    async def on_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        original = getattr(error, "original", error)
        if isinstance(error, app_commands.CheckFailure) and not isinstance(error, app_commands.CommandInvokeError):
            await interaction.client.report_error(interaction, ServiceError(str(error) or "You cannot use that."), "slash")  # type: ignore[attr-defined]
            return
        await interaction.client.report_error(interaction, original, f"slash:{interaction.command.qualified_name if interaction.command else '?'}")  # type: ignore[attr-defined]


class AegisBot(commands.Bot):
    def __init__(self, config: Config, db: Database) -> None:
        intents = discord.Intents.default()  # no privileged intents are needed: everything runs on interactions
        super().__init__(command_prefix=commands.when_mentioned, intents=intents, tree_cls=AegisTree,
                         help_command=None, allowed_mentions=discord.AllowedMentions.none())
        self.config = config
        self.db = db
        self.cooldowns = Cooldowns()
        self.errors = ErrorBuffer()
        self.entries: dict[str, Entry] = {}
        self.event_handlers: dict[str, Callable[[discord.Interaction, str, int], Awaitable[None]]] = {}
        self.started_at = time.time()
        self.maintenance = False
        self.loaded_cogs: list[str] = []
        self.failed_cogs: dict[str, str] = {}

    # ------------------------------------------------------------------ entry registry
    def register_entry(self, name: str, fn: Entry) -> None:
        self.entries[name] = fn

    def register_event_handler(self, kind_code: str, fn: Callable[[discord.Interaction, str, int], Awaitable[None]]) -> None:
        self.event_handlers[kind_code] = fn

    async def open(self, interaction: discord.Interaction, name: str) -> None:
        """Open a registered screen. Checks module switches here so every route is covered."""
        fn = self.entries.get(name)
        if fn is None:
            await respond(interaction, embed=notice("That section is not available in this build.", "error"))
            return
        module = ENTRY_MODULE.get(name.split(".")[0])
        if module and interaction.guild_id and module in await self.db.read(lambda c: disabled_modules(c, interaction.guild_id)):
            await show(interaction, notice(f"The **{module}** module is switched off on this server.", "warn"))
            return
        await fn(interaction)

    # ------------------------------------------------------------------ lifecycle
    async def setup_hook(self) -> None:
        version = await self.db.connect()
        log_bot.info("database ready (schema v%s) at %s", version, self.config.database_path)
        self.add_dynamic_items(EventButton)  # restart-safe buttons on announced events
        for ext, cog_cls in COGS:
            try:
                await self.add_cog(cog_cls(self))
                self.loaded_cogs.append(ext)
            except Exception as exc:  # noqa: BLE001 - one broken cog must not take the bot down
                self.failed_cogs[ext] = f"{type(exc).__name__}: {exc}"
                log_bot.exception("failed to load %s", ext)
        missing = self.validate_entries()
        if missing:
            log_bot.error("dashboard buttons reference unregistered screens: %s", ", ".join(sorted(missing)))
        synced_for = await self.db.read(lambda c: get_bot_state(c, "commands_version"))
        if synced_for != COMMANDS_VERSION:
            await self.sync_commands()

    def validate_entries(self) -> set[str]:
        return {e for e in referenced_entries() if e not in self.entries}

    async def sync_commands(self) -> int:
        synced = await self.tree.sync()
        await self.db.run(lambda c: set_bot_state(c, "commands_version", COMMANDS_VERSION))
        log_bot.info("synced %s application commands", len(synced))
        return len(synced)

    async def on_ready(self) -> None:
        log_bot.info("connected as %s (%s) in %s server(s)", self.user, self.user.id if self.user else "?", len(self.guilds))

    async def close(self) -> None:
        log_bot.info("shutting down")
        try:
            await super().close()
        finally:
            await self.db.close()

    # ------------------------------------------------------------------ helpers
    async def brand(self, guild_id: int | None) -> Branding:
        if not guild_id:
            return Branding()
        return Branding.from_settings(await self.db.read(lambda c: all_settings(c, guild_id)))

    async def settings(self, guild_id: int) -> dict[str, str]:
        return await self.db.read(lambda c: all_settings(c, guild_id))

    async def perms(self, interaction: discord.Interaction) -> Perms:
        user = interaction.user
        guild_id = interaction.guild_id or 0
        is_admin = isinstance(user, discord.Member) and user.guild_permissions.administrator
        role_ids = [r.id for r in user.roles] if isinstance(user, discord.Member) else []
        return await self.db.read(lambda c: get_perms_sync(
            c, guild_id=guild_id, user_id=user.id, owner_id=self.config.owner_id, discord_admin=is_admin, role_ids=role_ids))

    async def managed_event(self, interaction: discord.Interaction, kind: str, event_id: int) -> dict:
        """Load an event and verify the caller may manage it (server-side, on every privileged callback)."""
        ev = await self.db.read(lambda c: get_event(c, kind, event_id))
        perms = await self.perms(interaction)
        if ev["guild_id"] != interaction.guild_id or not can_manage_event(perms, interaction.user.id, ev["organizer_id"], kind):
            raise ServiceError("You don't have permission to manage this event.")
        return ev

    def gate(self, interaction: discord.Interaction) -> str | None:
        """Cheap synchronous checks run before every component callback. Returns a message to refuse, else None."""
        if interaction.guild_id is None:
            return "AEGIS only works inside a server."
        if self.maintenance and interaction.user.id != self.config.owner_id:
            return "The bot is in maintenance mode. Please try again shortly."
        if self.cooldowns.check(interaction.user.id, "ui", 0.6):
            return "Slow down a little - try again in a second."
        return None

    async def require(self, interaction: discord.Interaction, check: Callable[[Perms], bool], message: str) -> Perms | None:
        """Server-side authorization for privileged callbacks. Responds and returns None when refused."""
        p = await self.perms(interaction)
        if not check(p):
            await show(interaction, notice(message, "error"))
            return None
        return p

    async def post(self, guild: discord.Guild | None, channel_id: int | None, *, embed: discord.Embed | None = None,
                   content: str | None = None, view: discord.ui.View | None = None) -> discord.Message | None:
        """Send to a configured channel. Returns None (and logs) if the channel is missing or the bot lacks access."""
        if guild is None or not channel_id:
            return None
        ch = guild.get_channel(channel_id)
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            log_bot.warning("configured channel %s is missing in guild %s", channel_id, guild.id)
            return None
        me = guild.me
        if me is not None and not ch.permissions_for(me).send_messages:
            log_bot.warning("no permission to send in #%s (guild %s)", ch.name, guild.id)
            return None
        kw: dict = {}
        if embed is not None:
            kw["embed"] = embed
        if content is not None:
            kw["content"] = content
        if view is not None:
            kw["view"] = view
        try:
            return await ch.send(**kw)
        except discord.HTTPException:
            log_bot.warning("failed to send to channel %s", channel_id, exc_info=True)
            return None

    async def dm(self, user_id: int, *, embed: discord.Embed | None = None, content: str | None = None) -> bool:
        """Best-effort direct message. Never raises; returns whether it was delivered."""
        try:
            user = self.get_user(user_id) or await self.fetch_user(user_id)
            await user.send(embed=embed, content=content) if embed is not None else await user.send(content=content or "")
            return True
        except (discord.Forbidden, discord.HTTPException, discord.NotFound):
            return False

    async def staff_log(self, guild: discord.Guild | None, text: str) -> None:
        if guild is None:
            return
        cid = await self.db.read(lambda c: int(all_settings(c, guild.id).get("ch_staff_log") or 0))
        if cid:
            await self.post(guild, cid, embed=discord.Embed(description=text[:3900], color=0x00B4FF))

    # ------------------------------------------------------------------ errors
    async def report_error(self, interaction: discord.Interaction, error: BaseException, where: str) -> None:
        if isinstance(error, (ServiceError, ValidationError)):
            await self._reply_error(interaction, notice(str(error), "error"))
            return
        if isinstance(error, discord.Forbidden):
            await self._reply_error(interaction, notice("I'm missing a Discord permission needed for that. Ask an admin to check my role/channel permissions.", "error"))
            log_bot.warning("forbidden in %s: %s", where, error)
            return
        ref = secrets.token_hex(3).upper()
        self.errors.add(ref, where, f"{type(error).__name__}: {error}")
        log_bot.error("unhandled error [%s] in %s", ref, where, exc_info=(type(error), error, error.__traceback__))
        await self._reply_error(interaction, notice(f"Something went wrong (ref `{ref}`). Staff can find it in the owner panel's error log.", "error"))

    async def _reply_error(self, interaction: discord.Interaction, embed: discord.Embed) -> None:
        try:
            if interaction.response.is_done():
                await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)
        except discord.HTTPException:
            log_bot.debug("could not deliver error message", exc_info=True)

    def module_names(self) -> tuple[str, ...]:
        return MODULES


# ==================================================================================================
# SECTION 6 - HELPER FUNCTIONS, VALIDATORS, PERMISSION CHECKS AND SERVICE LAYER
# ==================================================================================================

# --------------------------------------------------------------------------------------------------
# utils/validators.py
# Input validation helpers. Every modal value goes through these.
# --------------------------------------------------------------------------------------------------

class ValidationError(ValueError):
    """Raised with a user-presentable message."""


_CTRL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def clean(text: str | None, *, field: str = "Value", max_len: int = 100, min_len: int = 0,
          multiline: bool = False) -> str:
    t = _CTRL.sub("", (text or "")).strip()
    if not multiline:
        t = re.sub(r"\s+", " ", t)
    if len(t) < min_len:
        raise ValidationError(f"{field} must be at least {min_len} characters.")
    if len(t) > max_len:
        raise ValidationError(f"{field} must be at most {max_len} characters.")
    return t


def optional(text: str | None, **kw) -> str | None:
    t = clean(text, **kw) if (text or "").strip() else ""
    return t or None


def validate_url(text: str | None, *, field: str = "URL", required: bool = False) -> str | None:
    t = (text or "").strip()
    if not t:
        if required:
            raise ValidationError(f"{field} is required.")
        return None
    if len(t) > 500:
        raise ValidationError(f"{field} is too long.")
    p = urlparse(t)
    if p.scheme not in ("http", "https") or not p.netloc or " " in t:
        raise ValidationError(f"{field} must be a valid http(s) link.")
    return t


def parse_int(text: str | None, *, field: str, lo: int, hi: int, default: int | None = None) -> int:
    t = (text or "").strip()
    if not t:
        if default is not None:
            return default
        raise ValidationError(f"{field} is required.")
    try:
        v = int(t)
    except ValueError:
        raise ValidationError(f"{field} must be a whole number.") from None
    if not lo <= v <= hi:
        raise ValidationError(f"{field} must be between {lo} and {hi}.")
    return v


def parse_float(text: str | None, *, field: str, lo: float, hi: float, default: float | None = None) -> float:
    t = (text or "").strip()
    if not t:
        if default is not None:
            return default
        raise ValidationError(f"{field} is required.")
    try:
        v = float(t)
    except ValueError:
        raise ValidationError(f"{field} must be a number.") from None
    if not lo <= v <= hi:
        raise ValidationError(f"{field} must be between {lo:g} and {hi:g}.")
    return v


def parse_choice(text: str | None, *, field: str, choices: dict[str, str], default: str | None = None) -> str:
    t = (text or "").strip().lower()
    if not t and default is not None:
        return default
    if t in choices:
        return choices[t]
    raise ValidationError(f"{field} must be one of: {', '.join(sorted(set(choices.values())))}.")


def parse_number_list(text: str | None, *, field: str, max_items: int = 100, lo: float = 0,
                      hi: float = 10000, min_items: int = 1) -> list[float]:
    parts = [p for p in re.split(r"[,\s]+", (text or "").strip()) if p]
    if len(parts) < min_items:
        raise ValidationError(f"{field} needs at least {min_items} number(s), separated by commas.")
    if len(parts) > max_items:
        raise ValidationError(f"{field} can have at most {max_items} numbers.")
    out: list[float] = []
    for p in parts:
        try:
            v = float(p)
        except ValueError:
            raise ValidationError(f"{field}: '{p[:12]}' is not a number.") from None
        if not lo <= v <= hi:
            raise ValidationError(f"{field}: values must be between {lo:g} and {hi:g}.")
        out.append(v)
    return out


def parse_hex_color(text: str | None) -> int:
    t = (text or "").strip().lstrip("#")
    if not re.fullmatch(r"[0-9a-fA-F]{6}", t):
        raise ValidationError("Color must be a 6-digit hex code like 00B4FF.")
    return int(t, 16)


_NAME_RE = re.compile(r"^[\w .'&!\-]+$", re.UNICODE)


def validate_team_name(text: str | None) -> str:
    t = clean(text, field="Team name", min_len=3, max_len=32)
    if not _NAME_RE.match(t):
        raise ValidationError("Team name has unsupported characters.")
    if "@" in t:
        raise ValidationError("Team name cannot contain '@'.")
    return t


def validate_tag(text: str | None) -> str | None:
    t = (text or "").strip()
    if not t:
        return None
    if not re.fullmatch(r"[A-Za-z0-9]{2,5}", t):
        raise ValidationError("Tag must be 2-5 letters/numbers.")
    return t.upper()


def parse_bool(text: str | None, *, field: str = "Value", default: bool | None = None) -> bool:
    t = (text or "").strip().lower()
    if not t and default is not None:
        return default
    if t in {"y", "yes", "true", "1", "on"}:
        return True
    if t in {"n", "no", "false", "0", "off"}:
        return False
    raise ValidationError(f"{field} must be yes or no.")


def parse_id(text: str | None, *, field: str = "ID") -> int:
    return parse_int(text, field=field, lo=1, hi=2**63 - 1)


def trunc(s: str | None, n: int) -> str:
    s = s or ""
    return s if len(s) <= n else s[: n - 1] + "…"

# --------------------------------------------------------------------------------------------------
# utils/timeutil.py
# UTC time helpers. All timestamps are stored as UTC ISO strings: YYYY-MM-DDTHH:MM:SSZ.
# --------------------------------------------------------------------------------------------------

ISO_FMT = "%Y-%m-%dT%H:%M:%SZ"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return utcnow().strftime(ISO_FMT)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime(ISO_FMT)


def from_iso(s: str) -> datetime:
    return datetime.strptime(s, ISO_FMT).replace(tzinfo=timezone.utc)


def parse_offset(text: str) -> int:
    """'+05:30' -> 330 minutes."""
    m = re.fullmatch(r"\s*([+-])(\d{1,2}):?(\d{2})\s*", text or "")
    if not m:
        raise ValidationError("UTC offset must look like +05:30 or -04:00.")
    h, mi = int(m.group(2)), int(m.group(3))
    if h > 14 or mi > 59:
        raise ValidationError("UTC offset is out of range.")
    total = h * 60 + mi
    return total if m.group(1) == "+" else -total


def format_offset(minutes: int) -> str:
    sign = "+" if minutes >= 0 else "-"
    minutes = abs(minutes)
    return f"{sign}{minutes // 60:02d}:{minutes % 60:02d}"


_FORMATS = ("%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M", "%d/%m/%Y %H:%M", "%Y-%m-%d %I:%M %p", "%d-%m-%Y %I:%M %p")


def parse_local(text: str | None, offset_minutes: int, *, field: str = "Date/time", required: bool = True) -> str | None:
    t = re.sub(r"\s+", " ", (text or "").strip())
    if not t:
        if required:
            raise ValidationError(f"{field} is required (e.g. 25-12-2026 21:30).")
        return None
    for fmt in _FORMATS:
        try:
            naive = datetime.strptime(t.upper() if "%p" in fmt else t, fmt)
        except ValueError:
            continue
        utc = naive - timedelta(minutes=offset_minutes)
        return utc.strftime(ISO_FMT)
    raise ValidationError(f"{field} must look like 25-12-2026 21:30 or 2026-12-25 21:30 (24-hour, server time).")


def to_local_text(iso: str | None, offset_minutes: int) -> str:
    """Pre-fill text for edit modals."""
    if not iso:
        return ""
    return (from_iso(iso) + timedelta(minutes=offset_minutes)).strftime("%d-%m-%Y %H:%M")


def unix(iso: str) -> int:
    return int(from_iso(iso).timestamp())


def ts(iso: str | None, style: str = "F") -> str:
    if not iso:
        return "TBA"
    return f"<t:{unix(iso)}:{style}>"


def ts_both(iso: str | None) -> str:
    if not iso:
        return "TBA"
    return f"<t:{unix(iso)}:F> (<t:{unix(iso)}:R>)"


def parse_duration(text: str | None, *, field: str = "Duration") -> timedelta:
    t = (text or "").strip().lower().replace(" ", "")
    if not t:
        raise ValidationError(f"{field} is required (e.g. 30m, 2h, 1d).")
    parts = re.findall(r"(\d+)([dhm])", t)
    if not parts or "".join(f"{n}{u}" for n, u in parts) != t:
        raise ValidationError(f"{field} must look like 30m, 2h, 1d or 1h30m.")
    delta = timedelta()
    for n, u in parts:
        n = int(n)
        delta += timedelta(days=n) if u == "d" else timedelta(hours=n) if u == "h" else timedelta(minutes=n)
    if delta < timedelta(minutes=1) or delta > timedelta(days=30):
        raise ValidationError(f"{field} must be between 1 minute and 30 days.")
    return delta

# --------------------------------------------------------------------------------------------------
# utils/checks.py
# Small runtime helpers: cooldowns and an in-memory error ring buffer.
# --------------------------------------------------------------------------------------------------

class Cooldowns:
    """Per (user, key) cooldown. check() returns remaining seconds (0 means allowed) and arms it."""

    def __init__(self) -> None:
        self._hits: dict[tuple[int, str], float] = {}

    def check(self, user_id: int, key: str, seconds: float) -> float:
        now = time.monotonic()
        k = (user_id, key)
        last = self._hits.get(k, 0.0)
        remaining = seconds - (now - last)
        if remaining > 0:
            return remaining
        self._hits[k] = now
        if len(self._hits) > 5000:
            cutoff = now - 3600
            self._hits = {kk: v for kk, v in self._hits.items() if v > cutoff}
        return 0.0


@dataclass
class ErrorRecord:
    ref: str
    where: str
    summary: str
    at: float = field(default_factory=time.time)


class ErrorBuffer:
    def __init__(self, size: int = 50) -> None:
        self._items: deque[ErrorRecord] = deque(maxlen=size)
        self.total = 0

    def add(self, ref: str, where: str, summary: str) -> None:
        self.total += 1
        self._items.append(ErrorRecord(ref, where, summary[:200]))

    def recent(self, n: int = 10) -> list[ErrorRecord]:
        return list(self._items)[-n:][::-1]

    def clear(self) -> None:
        self._items.clear()

# --------------------------------------------------------------------------------------------------
# services/common.py
# Shared service helpers: errors, settings, audit log.
# --------------------------------------------------------------------------------------------------

class ServiceError(Exception):
    """A business-rule failure whose message is safe to show to the user."""


DEFAULT_GAMES = ["BGMI", "PUBG Mobile", "Free Fire", "Valorant", "Call of Duty Mobile", "Fortnite", "Minecraft"]

MODULES = ("tournaments", "scrims", "teams", "players", "leaderboards", "reports", "community", "staff")

CHANNEL_KEYS = {
    "ch_esports": "General esports / news / polls / giveaways",
    "ch_tournaments": "Tournament announcements",
    "ch_scrims": "Scrim announcements",
    "ch_results": "Results & standings",
    "ch_staff_log": "Staff log (private)",
    "ch_recruit": "Recruitment board",
}
ROLE_KEYS = {
    "role_admin": "Administrator role",
    "role_organizer": "Tournament organizer role",
    "role_scrim_manager": "Scrim manager role",
    "role_referee": "Referee role",
    "role_staff": "Staff role",
}
BOOL_KEYS = {
    "auto_approve_free": "Auto-approve registrations for free events",
    "require_verified_team": "Only verified teams may register",
    "auto_announce": "Automatic reminders & announcements",
    "waitlist": "Waitlist when events are full",
}

SETTING_DEFAULTS: dict[str, str] = {
    "brand_name": "AEGIS ESPORTS",
    "brand_color": "00B4FF",
    "brand_footer": "AEGIS ESPORTS • Universal Competitive Management",
    "brand_image": "",
    "utc_offset": "+05:30",
    "season": "Season 1",
    "games": json.dumps(DEFAULT_GAMES),
    "max_roster": "6",
    "max_subs": "2",
    "checkin_window": "60",
    "reminder_minutes": "15",
    "auto_approve_free": "0",
    "require_verified_team": "0",
    "auto_announce": "1",
    "waitlist": "1",
    "modules_disabled": "[]",
}
ALLOWED_SETTINGS = set(SETTING_DEFAULTS) | set(CHANNEL_KEYS) | set(ROLE_KEYS)


def get_setting(conn: sqlite3.Connection, guild_id: int, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM guild_settings WHERE guild_id=? AND key=?", (guild_id, key)).fetchone()
    if row is not None:
        return row["value"]
    return SETTING_DEFAULTS.get(key, default) if default is None else default


def set_setting(conn: sqlite3.Connection, guild_id: int, key: str, value: str) -> None:
    if key not in ALLOWED_SETTINGS:
        raise ServiceError(f"Unknown setting '{key}'.")
    conn.execute(
        "INSERT INTO guild_settings(guild_id,key,value,updated_at) VALUES (?,?,?,?) "
        "ON CONFLICT(guild_id,key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (guild_id, key, str(value), now_iso()),
    )


def clear_setting(conn: sqlite3.Connection, guild_id: int, key: str) -> None:
    conn.execute("DELETE FROM guild_settings WHERE guild_id=? AND key=?", (guild_id, key))


def get_int(conn: sqlite3.Connection, guild_id: int, key: str, default: int = 0) -> int:
    v = get_setting(conn, guild_id, key)
    try:
        return int(v) if v not in (None, "") else default
    except ValueError:
        return default


def get_bool(conn: sqlite3.Connection, guild_id: int, key: str) -> bool:
    return get_setting(conn, guild_id, key) in ("1", "true", "True")


def get_json(conn: sqlite3.Connection, guild_id: int, key: str, default: Any) -> Any:
    try:
        return json.loads(get_setting(conn, guild_id, key) or "")
    except (ValueError, TypeError):
        return default


def get_games(conn: sqlite3.Connection, guild_id: int) -> list[str]:
    games = get_json(conn, guild_id, "games", DEFAULT_GAMES)
    return [g for g in games if isinstance(g, str)] or list(DEFAULT_GAMES)


def disabled_modules(conn: sqlite3.Connection, guild_id: int) -> set[str]:
    return {m for m in get_json(conn, guild_id, "modules_disabled", []) if isinstance(m, str)}


def all_settings(conn: sqlite3.Connection, guild_id: int) -> dict[str, str]:
    out = dict(SETTING_DEFAULTS)
    for row in conn.execute("SELECT key, value FROM guild_settings WHERE guild_id=?", (guild_id,)):
        out[row["key"]] = row["value"]
    return out


def audit(conn: sqlite3.Connection, guild_id: int, actor_id: int, action: str, target: str | None = None,
          detail: str | None = None) -> None:
    conn.execute(
        "INSERT INTO audit_logs(guild_id,actor_id,action,target,detail,created_at) VALUES (?,?,?,?,?,?)",
        (guild_id, actor_id, action, target, (detail or "")[:900], now_iso()),
    )


def touch_user(conn: sqlite3.Connection, user_id: int) -> None:
    now = now_iso()
    conn.execute(
        "INSERT INTO users(user_id,first_seen,last_seen) VALUES (?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET last_seen=excluded.last_seen",
        (user_id, now, now),
    )


def get_bot_state(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM bot_state WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_bot_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO bot_state(key,value,updated_at) VALUES (?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (key, value, now_iso()),
    )

# --------------------------------------------------------------------------------------------------
# services/permission_service.py
# Authorization. Pure functions decide; get_perms() gathers the inputs.
# Every privileged callback must call these server-side - hiding buttons is never enough.
# --------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class Perms:
    is_owner: bool = False
    is_admin: bool = False
    is_organizer: bool = False
    is_scrim_manager: bool = False
    is_referee: bool = False
    is_staff: bool = False

    @property
    def any_staff(self) -> bool:
        return any((self.is_admin, self.is_organizer, self.is_scrim_manager, self.is_referee, self.is_staff))


def compute_perms(*, user_id: int, owner_id: int, discord_admin: bool, role_ids: Iterable[int],
                  config_roles: dict[str, int], db_roles: Iterable[str]) -> Perms:
    """config_roles maps 'admin'|'organizer'|'scrim_manager'|'referee'|'staff' -> Discord role id (0 = unset)."""
    roles = set(role_ids)
    db = set(db_roles)
    is_owner = bool(owner_id) and user_id == owner_id

    def has(name: str) -> bool:
        rid = config_roles.get(name, 0)
        return bool(rid) and rid in roles

    is_admin = is_owner or discord_admin or has("admin")
    return Perms(
        is_owner=is_owner,
        is_admin=is_admin,
        is_organizer=is_admin or "organizer" in db or has("organizer"),
        is_scrim_manager=is_admin or "scrim_manager" in db or has("scrim_manager") or "organizer" in db or has("organizer"),
        is_referee=is_admin or "referee" in db or has("referee"),
        is_staff=is_admin or "staff" in db or has("staff") or "referee" in db or has("referee"),
    )


def get_perms_sync(conn: sqlite3.Connection, *, guild_id: int, user_id: int, owner_id: int,
                   discord_admin: bool, role_ids: Iterable[int]) -> Perms:
    config_roles = {n: get_int(conn, guild_id, f"role_{n}", 0)
                    for n in ("admin", "organizer", "scrim_manager", "referee", "staff")}
    db_roles = [r["role"] for r in conn.execute(
        "SELECT role FROM staff_members WHERE guild_id=? AND user_id=?", (guild_id, user_id))]
    return compute_perms(user_id=user_id, owner_id=owner_id, discord_admin=discord_admin,
                         role_ids=role_ids, config_roles=config_roles, db_roles=db_roles)


def can_create_event(perms: Perms, kind: str) -> bool:
    if perms.is_admin:
        return True
    return perms.is_organizer if kind == "tournament" else perms.is_scrim_manager


def can_manage_event(perms: Perms, user_id: int, organizer_id: int, kind: str) -> bool:
    """Admins manage everything; organizers/scrim managers only events they own."""
    if perms.is_admin:
        return True
    return can_create_event(perms, kind) and user_id == organizer_id


def can_review_result(perms: Perms, user_id: int, organizer_id: int | None, assigned_referees: set[int]) -> bool:
    """If referees are assigned to the event only they (plus admins and the owner of the event) may review;
    otherwise any referee may."""
    if perms.is_admin:
        return True
    if organizer_id is not None and user_id == organizer_id and (perms.is_organizer or perms.is_scrim_manager):
        return True
    if assigned_referees:
        return user_id in assigned_referees and perms.is_referee
    return perms.is_referee


def can_grant_roles(perms: Perms) -> bool:
    return perms.is_admin


def can_verify_team(perms: Perms) -> bool:
    return perms.is_staff or perms.is_admin


def can_manage_reports(perms: Perms) -> bool:
    return perms.is_staff or perms.is_admin


def can_post_community(perms: Perms) -> bool:
    return perms.is_staff or perms.is_organizer or perms.is_admin


def dashboard_sections(perms: Perms, is_captain: bool, has_team: bool) -> list[str]:
    """Ordered dashboard names this user may open (public first)."""
    out = ["public", "player"]
    if is_captain:
        out.append("captain")
    if perms.is_organizer:
        out.append("organizer")
    if perms.is_scrim_manager:
        out.append("scrim")
    if perms.is_referee:
        out.append("referee")
    if perms.is_staff:
        out.append("staff")
    if perms.is_admin:
        out.append("admin")
    if perms.is_owner:
        out.append("owner")
    return out

# --------------------------------------------------------------------------------------------------
# services/team_service.py
# Team, roster, invite, join-request and transfer logic. Authorization is enforced here too
# (defense in depth): every mutating function takes the acting user and checks their team role.
# --------------------------------------------------------------------------------------------------

ROLE_ORDER = {"captain": 0, "cocaptain": 1, "player": 2, "sub": 3}
ROLE_LABEL = {"captain": "Captain", "cocaptain": "Co-Captain", "player": "Player", "sub": "Substitute"}
TEAM_EDITABLE = {"tag", "logo_url", "game", "recruiting"}


def roster_limits(conn: sqlite3.Connection, guild_id: int) -> tuple[int, int]:
    return get_int(conn, guild_id, "max_roster", 6), get_int(conn, guild_id, "max_subs", 2)


def team_of_user(conn: sqlite3.Connection, guild_id: int, user_id: int) -> dict[str, Any] | None:
    return conn.execute(
        "SELECT t.*, m.role AS my_role FROM team_members m JOIN teams t ON t.id=m.team_id "
        "WHERE m.guild_id=? AND m.user_id=? AND t.disbanded=0", (guild_id, user_id)).fetchone()


def get_team(conn: sqlite3.Connection, team_id: int) -> dict[str, Any] | None:
    return conn.execute("SELECT * FROM teams WHERE id=?", (team_id,)).fetchone()


def find_team_by_name(conn: sqlite3.Connection, guild_id: int, name: str) -> dict[str, Any] | None:
    return conn.execute("SELECT * FROM teams WHERE guild_id=? AND name_key=? AND disbanded=0",
                        (guild_id, name.strip().casefold())).fetchone()


def team_members(conn: sqlite3.Connection, team_id: int) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM team_members WHERE team_id=? ORDER BY joined_at", (team_id,)).fetchall()
    return sorted(rows, key=lambda r: ROLE_ORDER[r["role"]])


def team_counts(conn: sqlite3.Connection, team_id: int) -> tuple[int, int]:
    """(main roster incl. captain/co-captain, substitutes)"""
    rows = conn.execute("SELECT role, COUNT(*) AS c FROM team_members WHERE team_id=? GROUP BY role", (team_id,)).fetchall()
    d = {r["role"]: r["c"] for r in rows}
    return d.get("captain", 0) + d.get("cocaptain", 0) + d.get("player", 0), d.get("sub", 0)


def _require_manager(conn: sqlite3.Connection, team_id: int, actor_id: int, *, captain_only: bool = False,
                     admin: bool = False) -> dict[str, Any]:
    team = get_team(conn, team_id)
    if not team or team["disbanded"]:
        raise ServiceError("That team no longer exists.")
    if admin:
        return team
    m = conn.execute("SELECT role FROM team_members WHERE team_id=? AND user_id=?", (team_id, actor_id)).fetchone()
    allowed = {"captain"} if captain_only else {"captain", "cocaptain"}
    if not m or m["role"] not in allowed:
        raise ServiceError("Only the team captain can do that." if captain_only else "Only the captain or co-captain can do that.")
    return team


def create_team(conn: sqlite3.Connection, guild_id: int, user_id: int, name: str, tag: str | None,
                logo_url: str | None, game: str | None) -> int:
    if team_of_user(conn, guild_id, user_id):
        raise ServiceError("You are already in a team. Leave it first.")
    if find_team_by_name(conn, guild_id, name):
        raise ServiceError("A team with that name already exists.")
    if tag and conn.execute("SELECT 1 FROM teams WHERE guild_id=? AND disbanded=0 AND UPPER(tag)=?", (guild_id, tag.upper())).fetchone():
        raise ServiceError("That team tag is already taken.")
    now = now_iso()
    try:
        cur = conn.execute(
            "INSERT INTO teams(guild_id,name,name_key,tag,logo_url,game,captain_id,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (guild_id, name, name.casefold(), tag, logo_url, game, user_id, now, now))
    except sqlite3.IntegrityError:
        raise ServiceError("A team with that name already exists.") from None
    tid = int(cur.lastrowid)
    conn.execute("INSERT INTO team_members(team_id,guild_id,user_id,role,joined_at) VALUES (?,?,?,'captain',?)",
                 (tid, guild_id, user_id, now))
    touch_user(conn, user_id)
    audit(conn, guild_id, user_id, "team.create", f"team:{tid}", name)
    return tid


def update_team(conn: sqlite3.Connection, team_id: int, actor_id: int, *, admin: bool = False, **fields: Any) -> None:
    team = _require_manager(conn, team_id, actor_id, admin=admin)
    bad = set(fields) - TEAM_EDITABLE
    if bad:
        raise ServiceError("Those fields cannot be edited.")
    if not fields:
        return
    if "tag" in fields and fields["tag"]:
        if conn.execute("SELECT 1 FROM teams WHERE guild_id=? AND disbanded=0 AND UPPER(tag)=? AND id<>?",
                        (team["guild_id"], fields["tag"].upper(), team_id)).fetchone():
            raise ServiceError("That team tag is already taken.")
    sets = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE teams SET {sets}, updated_at=? WHERE id=?", (*fields.values(), now_iso(), team_id))  # noqa: S608
    audit(conn, team["guild_id"], actor_id, "team.update", f"team:{team_id}", ",".join(fields))


def set_verified(conn: sqlite3.Connection, team_id: int, verifier_id: int, flag: bool) -> None:
    team = get_team(conn, team_id)
    if not team:
        raise ServiceError("Team not found.")
    conn.execute("UPDATE teams SET verified=?, verified_by=?, updated_at=? WHERE id=?",
                 (1 if flag else 0, verifier_id if flag else None, now_iso(), team_id))
    audit(conn, team["guild_id"], verifier_id, "team.verify" if flag else "team.unverify", f"team:{team_id}")


# ---------------------------------------------------------------- invites / requests / transfers
def _add_member(conn: sqlite3.Connection, team: dict[str, Any], user_id: int, preferred: str = "player") -> str:
    mains, subs = team_counts(conn, team["id"])
    max_roster, max_subs = roster_limits(conn, team["guild_id"])
    if preferred == "player" and mains < max_roster:
        role = "player"
    elif subs < max_subs:
        role = "sub"
    else:
        raise ServiceError("That team's roster is full.")
    conn.execute("INSERT INTO team_members(team_id,guild_id,user_id,role,joined_at) VALUES (?,?,?,?,?)",
                 (team["id"], team["guild_id"], user_id, role, now_iso()))
    touch_user(conn, user_id)
    return role


def create_invite(conn: sqlite3.Connection, *, team_id: int, actor_id: int, target_id: int, kind: str,
                  admin: bool = False) -> int:
    """kind='invite': captain/co-captain invites target. kind='request'/'transfer': target(=requester) asks to join."""
    team = get_team(conn, team_id)
    if not team or team["disbanded"]:
        raise ServiceError("That team no longer exists.")
    if kind == "invite":
        _require_manager(conn, team_id, actor_id, admin=admin)
        if target_id == actor_id:
            raise ServiceError("You cannot invite yourself.")
    elif actor_id != target_id:
        raise ServiceError("You can only request for yourself.")
    existing = team_of_user(conn, team["guild_id"], target_id)
    if kind in ("invite", "request") and existing:
        raise ServiceError("That player is already in a team." if kind == "invite" else "Leave your current team first, or request a transfer.")
    if kind == "transfer":
        if not existing:
            raise ServiceError("You are not in a team - send a normal join request instead.")
        if existing["id"] == team_id:
            raise ServiceError("You are already in that team.")
        if existing["my_role"] == "captain":
            raise ServiceError("Captains must transfer captaincy or disband before moving teams.")
    mains, subs = team_counts(conn, team_id)
    max_roster, max_subs = roster_limits(conn, team["guild_id"])
    if mains >= max_roster and subs >= max_subs:
        raise ServiceError("That team's roster is full.")
    try:
        cur = conn.execute(
            "INSERT INTO team_invites(guild_id,team_id,user_id,kind,created_by,created_at) VALUES (?,?,?,?,?,?)",
            (team["guild_id"], team_id, target_id, kind, actor_id, now_iso()))
    except sqlite3.IntegrityError:
        raise ServiceError("There is already a pending request/invite for this.") from None
    touch_user(conn, target_id)
    audit(conn, team["guild_id"], actor_id, f"team.{kind}", f"team:{team_id}", f"user:{target_id}")
    return int(cur.lastrowid)


def pending_for_user(conn: sqlite3.Connection, guild_id: int, user_id: int) -> list[dict[str, Any]]:
    return conn.execute(
        "SELECT i.*, t.name AS team_name FROM team_invites i JOIN teams t ON t.id=i.team_id "
        "WHERE i.guild_id=? AND i.user_id=? AND i.kind='invite' AND i.status='PENDING' AND t.disbanded=0 ORDER BY i.id",
        (guild_id, user_id)).fetchall()


def pending_for_team(conn: sqlite3.Connection, team_id: int) -> list[dict[str, Any]]:
    return conn.execute(
        "SELECT * FROM team_invites WHERE team_id=? AND kind IN ('request','transfer') AND status='PENDING' ORDER BY id",
        (team_id,)).fetchall()


def resolve_invite(conn: sqlite3.Connection, invite_id: int, actor_id: int, accept: bool, *, admin: bool = False) -> dict[str, Any]:
    inv = conn.execute("SELECT * FROM team_invites WHERE id=?", (invite_id,)).fetchone()
    if not inv or inv["status"] != "PENDING":
        raise ServiceError("That request is no longer pending.")
    team = get_team(conn, inv["team_id"])
    if not team or team["disbanded"]:
        raise ServiceError("That team no longer exists.")
    if inv["kind"] == "invite":
        if actor_id != inv["user_id"]:
            raise ServiceError("This invite is not for you.")
    else:
        _require_manager(conn, inv["team_id"], actor_id, admin=admin)
    status = "DECLINED" if not accept else "ACCEPTED"
    role = None
    if accept:
        current = team_of_user(conn, team["guild_id"], inv["user_id"])
        if inv["kind"] == "transfer":
            if not current or current["my_role"] == "captain":
                raise ServiceError("That player can no longer transfer.")
            conn.execute("DELETE FROM team_members WHERE team_id=? AND user_id=?", (current["id"], inv["user_id"]))
        elif current:
            raise ServiceError("That player already joined another team.")
        role = _add_member(conn, team, inv["user_id"])
        conn.execute("UPDATE team_invites SET status='CANCELLED', resolved_at=? WHERE user_id=? AND guild_id=? AND status='PENDING' AND id<>?",
                     (now_iso(), inv["user_id"], team["guild_id"], invite_id))
    conn.execute("UPDATE team_invites SET status=?, resolved_at=? WHERE id=?", (status, now_iso(), invite_id))
    audit(conn, team["guild_id"], actor_id, f"team.{inv['kind']}.{status.lower()}", f"team:{team['id']}", f"user:{inv['user_id']}")
    return {"invite": inv, "team": team, "role": role, "accepted": accept}


def remove_member(conn: sqlite3.Connection, team_id: int, target_id: int, actor_id: int, *, admin: bool = False) -> None:
    team = _require_manager(conn, team_id, actor_id, admin=admin)
    m = conn.execute("SELECT role FROM team_members WHERE team_id=? AND user_id=?", (team_id, target_id)).fetchone()
    if not m:
        raise ServiceError("That player is not on the team.")
    if m["role"] == "captain":
        raise ServiceError("The captain cannot be removed. Transfer captaincy first.")
    actor_role = conn.execute("SELECT role FROM team_members WHERE team_id=? AND user_id=?", (team_id, actor_id)).fetchone()
    if not admin and actor_role and actor_role["role"] == "cocaptain" and m["role"] == "cocaptain":
        raise ServiceError("Only the captain can remove a co-captain.")
    conn.execute("DELETE FROM team_members WHERE team_id=? AND user_id=?", (team_id, target_id))
    audit(conn, team["guild_id"], actor_id, "team.remove_member", f"team:{team_id}", f"user:{target_id}")


def set_member_role(conn: sqlite3.Connection, team_id: int, target_id: int, role: str, actor_id: int, *, admin: bool = False) -> None:
    if role not in ("cocaptain", "player", "sub"):
        raise ServiceError("Invalid role.")
    team = _require_manager(conn, team_id, actor_id, captain_only=(role == "cocaptain"), admin=admin)
    m = conn.execute("SELECT role FROM team_members WHERE team_id=? AND user_id=?", (team_id, target_id)).fetchone()
    if not m:
        raise ServiceError("That player is not on the team.")
    if m["role"] == "captain":
        raise ServiceError("Change the captain with 'Transfer Captaincy'.")
    if m["role"] == role:
        return
    mains, subs = team_counts(conn, team_id)
    max_roster, max_subs = roster_limits(conn, team["guild_id"])
    if role == "sub" and subs >= max_subs:
        raise ServiceError(f"Substitute list is full ({max_subs}).")
    if role in ("cocaptain", "player") and m["role"] == "sub" and mains >= max_roster:
        raise ServiceError(f"Main roster is full ({max_roster}).")
    if role == "cocaptain" and conn.execute("SELECT 1 FROM team_members WHERE team_id=? AND role='cocaptain'", (team_id,)).fetchone():
        raise ServiceError("The team already has a co-captain.")
    conn.execute("UPDATE team_members SET role=? WHERE team_id=? AND user_id=?", (role, team_id, target_id))
    audit(conn, team["guild_id"], actor_id, "team.set_role", f"team:{team_id}", f"user:{target_id}={role}")


def transfer_captaincy(conn: sqlite3.Connection, team_id: int, new_captain_id: int, actor_id: int, *, admin: bool = False) -> None:
    team = _require_manager(conn, team_id, actor_id, captain_only=True, admin=admin)
    m = conn.execute("SELECT role FROM team_members WHERE team_id=? AND user_id=?", (team_id, new_captain_id)).fetchone()
    if not m or m["role"] == "captain":
        raise ServiceError("Pick another member of the team.")
    old = team["captain_id"]
    conn.execute("UPDATE team_members SET role='player' WHERE team_id=? AND user_id=?", (team_id, old))
    conn.execute("UPDATE team_members SET role='captain' WHERE team_id=? AND user_id=?", (team_id, new_captain_id))
    conn.execute("UPDATE teams SET captain_id=?, updated_at=? WHERE id=?", (new_captain_id, now_iso(), team_id))
    audit(conn, team["guild_id"], actor_id, "team.transfer_captain", f"team:{team_id}", f"user:{new_captain_id}")


def leave_team(conn: sqlite3.Connection, guild_id: int, user_id: int) -> dict[str, Any]:
    team = team_of_user(conn, guild_id, user_id)
    if not team:
        raise ServiceError("You are not in a team.")
    if team["my_role"] == "captain":
        raise ServiceError("Captains cannot leave. Transfer captaincy or disband the team.")
    conn.execute("DELETE FROM team_members WHERE team_id=? AND user_id=?", (team["id"], user_id))
    audit(conn, guild_id, user_id, "team.leave", f"team:{team['id']}")
    return team


def disband_team(conn: sqlite3.Connection, team_id: int, actor_id: int, *, admin: bool = False) -> None:
    team = _require_manager(conn, team_id, actor_id, captain_only=True, admin=admin)
    # Withdraw from events that have not finished; keep history rows (soft delete).
    conn.execute("UPDATE tournament_registrations SET status='WITHDRAWN', updated_at=? WHERE team_id=? AND status IN ('PENDING','APPROVED','WAITLIST')",
                 (now_iso(), team_id))
    conn.execute("UPDATE scrim_registrations SET status='WITHDRAWN', updated_at=? WHERE team_id=? AND status IN ('PENDING','APPROVED','WAITLIST')",
                 (now_iso(), team_id))
    conn.execute("DELETE FROM team_members WHERE team_id=?", (team_id,))
    conn.execute("UPDATE team_invites SET status='CANCELLED', resolved_at=? WHERE team_id=? AND status='PENDING'", (now_iso(), team_id))
    conn.execute("UPDATE teams SET disbanded=1, recruiting=0, updated_at=? WHERE id=?", (now_iso(), team_id))
    audit(conn, team["guild_id"], actor_id, "team.disband", f"team:{team_id}", team["name"])


def recruiting_teams(conn: sqlite3.Connection, guild_id: int) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM teams WHERE guild_id=? AND disbanded=0 AND recruiting=1 ORDER BY name", (guild_id,)).fetchall()
    max_roster, max_subs = roster_limits(conn, guild_id)
    out = []
    for t in rows:
        mains, subs = team_counts(conn, t["id"])
        if mains < max_roster or subs < max_subs:
            t["mains"], t["subs"] = mains, subs
            out.append(t)
    return out


def list_teams(conn: sqlite3.Connection, guild_id: int) -> list[dict[str, Any]]:
    return conn.execute("SELECT * FROM teams WHERE guild_id=? AND disbanded=0 ORDER BY name", (guild_id,)).fetchall()


def team_stats(conn: sqlite3.Connection, team_id: int) -> dict[str, Any]:
    s = conn.execute(
        "SELECT COALESCE(SUM(points),0) AS points, COALESCE(SUM(kills),0) AS kills, COALESCE(SUM(wins),0) AS wins, "
        "COALESCE(SUM(played),0) AS played, COUNT(*) AS events FROM standings WHERE team_id=?", (team_id,)).fetchone()
    s["no_shows"] = conn.execute("SELECT COUNT(*) AS c FROM scrim_registrations WHERE team_id=? AND no_show=1", (team_id,)).fetchone()["c"]
    s["badges"] = [b["badge"] for b in conn.execute("SELECT badge FROM badges WHERE team_id=? ORDER BY id", (team_id,))]
    return s


def team_history(conn: sqlite3.Connection, team_id: int, limit: int = 15) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT 'Tournament' AS kind, e.title, e.start_time, r.status, s.rank FROM tournament_registrations r "
        "JOIN tournaments e ON e.id=r.tournament_id LEFT JOIN standings s ON s.kind='tournament' AND s.event_id=e.id AND s.team_id=r.team_id "
        "WHERE r.team_id=? UNION ALL "
        "SELECT 'Scrim', e.title, e.start_time, r.status, s.rank FROM scrim_registrations r "
        "JOIN scrims e ON e.id=r.scrim_id LEFT JOIN standings s ON s.kind='scrim' AND s.event_id=e.id AND s.team_id=r.team_id "
        "WHERE r.team_id=? ORDER BY start_time DESC LIMIT ?", (team_id, team_id, limit)).fetchall()
    return rows

# --------------------------------------------------------------------------------------------------
# services/player_service.py
# Player profiles and discovery.
# --------------------------------------------------------------------------------------------------

STATUSES = {"ACTIVE": "Active", "LFT": "Looking for team", "INACTIVE": "Inactive"}


def player_get(conn: sqlite3.Connection, guild_id: int, user_id: int) -> dict[str, Any] | None:
    return conn.execute("SELECT * FROM player_profiles WHERE guild_id=? AND user_id=?", (guild_id, user_id)).fetchone()


def upsert(conn: sqlite3.Connection, guild_id: int, user_id: int, *, ign: str, game_uid: str | None, role: str | None,
           region: str | None, language: str | None, achievements: str | None) -> bool:
    """Returns True if created."""
    now = now_iso()
    touch_user(conn, user_id)
    existing = player_get(conn, guild_id, user_id)
    if existing:
        conn.execute("UPDATE player_profiles SET ign=?,game_uid=?,role=?,region=?,language=?,achievements=?,updated_at=? WHERE guild_id=? AND user_id=?",
                     (ign, game_uid, role, region, language, achievements, now, guild_id, user_id))
        return False
    conn.execute("INSERT INTO player_profiles(guild_id,user_id,ign,game_uid,role,region,language,achievements,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                 (guild_id, user_id, ign, game_uid, role, region, language, achievements, now, now))
    audit(conn, guild_id, user_id, "player.create", f"user:{user_id}")
    return True


def set_games(conn: sqlite3.Connection, guild_id: int, user_id: int, games: list[str]) -> None:
    if not player_get(conn, guild_id, user_id):
        raise ServiceError("Create your profile first.")
    conn.execute("UPDATE player_profiles SET games=?, updated_at=? WHERE guild_id=? AND user_id=?", (json.dumps(games[:10]), now_iso(), guild_id, user_id))


def player_set_status(conn: sqlite3.Connection, guild_id: int, user_id: int, status: str) -> None:
    if status not in STATUSES:
        raise ServiceError("Invalid status.")
    if not player_get(conn, guild_id, user_id):
        raise ServiceError("Create your profile first.")
    conn.execute("UPDATE player_profiles SET status=?, updated_at=? WHERE guild_id=? AND user_id=?", (status, now_iso(), guild_id, user_id))


def search(conn: sqlite3.Connection, guild_id: int, *, game: str | None = None, lft_only: bool = False, exclude_user: int | None = None,
           limit: int = 100) -> list[dict[str, Any]]:
    sql = ("SELECT p.*, (SELECT 1 FROM team_members m WHERE m.guild_id=p.guild_id AND m.user_id=p.user_id) AS has_team "
           "FROM player_profiles p WHERE p.guild_id=? AND p.status<>'INACTIVE'")
    params: list[Any] = [guild_id]
    if lft_only:
        sql += " AND p.status='LFT'"
    if exclude_user:
        sql += " AND p.user_id<>?"
        params.append(exclude_user)
    rows = conn.execute(sql + " ORDER BY p.updated_at DESC LIMIT ?", (*params, limit * 3)).fetchall()
    if game:
        rows = [r for r in rows if game in json.loads(r["games"] or "[]")]
    return rows[:limit]


def badges_for_user(conn: sqlite3.Connection, guild_id: int, user_id: int) -> list[str]:
    return [r["badge"] for r in conn.execute("SELECT badge FROM badges WHERE guild_id=? AND user_id=? ORDER BY id", (guild_id, user_id))]


def event_history(conn: sqlite3.Connection, guild_id: int, user_id: int, limit: int = 30) -> list[dict[str, Any]]:
    """Tournaments and scrims where the user was on a registered roster snapshot."""
    return conn.execute(
        "SELECT 'Tournament' AS kind, e.title, e.start_time, r.status, t.name AS team_name, s.rank FROM tournament_reg_players p "
        "JOIN tournament_registrations r ON r.id=p.reg_id JOIN tournaments e ON e.id=r.tournament_id JOIN teams t ON t.id=r.team_id "
        "LEFT JOIN standings s ON s.kind='tournament' AND s.event_id=e.id AND s.team_id=r.team_id "
        "WHERE p.user_id=? AND e.guild_id=? AND r.status IN ('PENDING','APPROVED','WAITLIST') "
        "UNION ALL "
        "SELECT 'Scrim', e.title, e.start_time, r.status, t.name, s.rank FROM scrim_reg_players p "
        "JOIN scrim_registrations r ON r.id=p.reg_id JOIN scrims e ON e.id=r.scrim_id JOIN teams t ON t.id=r.team_id "
        "LEFT JOIN standings s ON s.kind='scrim' AND s.event_id=e.id AND s.team_id=r.team_id "
        "WHERE p.user_id=? AND e.guild_id=? AND r.status IN ('PENDING','APPROVED','WAITLIST') "
        "ORDER BY 3 DESC LIMIT ?", (user_id, guild_id, user_id, guild_id, limit)).fetchall()

# --------------------------------------------------------------------------------------------------
# services/scoring_service.py
# Scoring engine: rules, result lifecycle (PENDING/APPROVED/REJECTED/DISPUTED) and standings.
#
# Standings are always recomputed from APPROVED rows only, so re-submitting, correcting or
# re-approving a result can never double count points.
# --------------------------------------------------------------------------------------------------

TIEBREAK_KEYS = ("points", "kills", "best_placement", "wins", "round_diff", "played")
DEFAULT_BR_POINTS = [10, 6, 5, 4, 3, 2, 1, 1]
RESULT_STATES = ("PENDING", "APPROVED", "REJECTED", "DISPUTED")


@dataclass
class Rule:
    id: int = 0
    name: str = "Default"
    kind: str = "br"
    placement_points: list[float] = field(default_factory=lambda: list(map(float, DEFAULT_BR_POINTS)))
    kill_points: float = 1.0
    win_points: float = 3.0
    draw_points: float = 1.0
    loss_points: float = 0.0
    round_points: float = 0.0
    tiebreak: list[str] = field(default_factory=lambda: ["points", "kills", "best_placement"])

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Rule":
        return cls(
            id=row["id"], name=row["name"], kind=row["kind"],
            placement_points=[float(x) for x in json.loads(row["placement_points"] or "[]")],
            kill_points=float(row["kill_points"]), win_points=float(row["win_points"]),
            draw_points=float(row["draw_points"]), loss_points=float(row["loss_points"]),
            round_points=float(row["round_points"]),
            tiebreak=[k for k in json.loads(row["tiebreak"] or "[]") if k in TIEBREAK_KEYS] or ["points"],
        )

    def describe(self) -> str:
        if self.kind == "br":
            pp = ", ".join(f"{p:g}" for p in self.placement_points) or "none"
            return f"Placement pts (1st→): {pp}\nKill pts: {self.kill_points:g}\nTie-break: {' > '.join(self.tiebreak)}"
        return (f"Win {self.win_points:g} • Draw {self.draw_points:g} • Loss {self.loss_points:g}\n"
                f"Per-round pts: {self.round_points:g}\nTie-break: {' > '.join(self.tiebreak)}")


# ---------------------------------------------------------------------- pure engine
def result_wins(rule: Rule, r: dict[str, Any]) -> bool:
    if rule.kind == "br":
        return r.get("placement") == 1
    return (r.get("score_for") or 0) > (r.get("score_against") or 0)


def points_for(rule: Rule, r: dict[str, Any]) -> float:
    bonus = float(r.get("bonus") or 0)
    if rule.kind == "br":
        place = r.get("placement")
        pp = 0.0
        if place and 1 <= place <= len(rule.placement_points):
            pp = rule.placement_points[place - 1]
        return pp + (r.get("kills") or 0) * rule.kill_points + bonus
    sf, sa = r.get("score_for") or 0, r.get("score_against") or 0
    base = rule.win_points if sf > sa else rule.draw_points if sf == sa else rule.loss_points
    return base + sf * rule.round_points + bonus


def compute_standings(rule: Rule, results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """results: approved rows with team_id/placement/kills/bonus/score_for/score_against."""
    agg: dict[int, dict[str, Any]] = {}
    for r in results:
        t = agg.setdefault(r["team_id"], {"team_id": r["team_id"], "played": 0, "points": 0.0, "kills": 0,
                                          "wins": 0, "best_placement": None, "round_diff": 0})
        t["played"] += 1
        t["points"] += points_for(rule, r)
        t["kills"] += r.get("kills") or 0
        if result_wins(rule, r):
            t["wins"] += 1
        if rule.kind == "br" and r.get("placement"):
            bp = t["best_placement"]
            t["best_placement"] = r["placement"] if bp is None else min(bp, r["placement"])
        if rule.kind == "versus":
            t["round_diff"] += (r.get("score_for") or 0) - (r.get("score_against") or 0)

    def key(t: dict[str, Any]) -> tuple:
        parts: list[float] = []
        for k in rule.tiebreak:
            if k == "best_placement":
                parts.append(t["best_placement"] if t["best_placement"] is not None else 10**6)  # lower is better
            elif k == "played":
                parts.append(t["played"])  # fewer games played wins a tie
            else:
                parts.append(-t[k])  # higher is better
        return tuple(parts)

    ordered = sorted(agg.values(), key=lambda t: (key(t), t["team_id"]))
    rank = 0
    prev: tuple | None = None
    for i, t in enumerate(ordered, start=1):
        k = key(t)
        if k != prev:
            rank = i  # competition ranking: exact ties share a rank
            prev = k
        t["rank"] = rank
        t["points"] = round(t["points"], 4)
    return ordered


def validate_result_input(rule: Rule, *, placement: int | None, kills: int | None,
                          score_for: int | None, score_against: int | None) -> None:
    if rule.kind == "br":
        if placement is None or not 1 <= placement <= 100:
            raise ServiceError("Placement must be between 1 and 100.")
        if kills is None or not 0 <= kills <= 200:
            raise ServiceError("Kills must be between 0 and 200.")
    else:
        for name, v in (("Your score", score_for), ("Opponent score", score_against)):
            if v is None or not 0 <= v <= 999:
                raise ServiceError(f"{name} must be between 0 and 999.")


# ---------------------------------------------------------------------- rules (DB)
def ensure_default_rules(conn: sqlite3.Connection, guild_id: int) -> None:
    n = conn.execute("SELECT COUNT(*) AS c FROM score_rules WHERE guild_id=?", (guild_id,)).fetchone()["c"]
    if n:
        return
    now = now_iso()
    conn.execute(
        "INSERT INTO score_rules(guild_id,name,kind,placement_points,kill_points,tiebreak,is_default,created_at) "
        "VALUES (?,?,?,?,?,?,1,?)",
        (guild_id, "BGMI Standard", "br", json.dumps(DEFAULT_BR_POINTS), 1.0,
         json.dumps(["points", "kills", "best_placement"]), now))
    conn.execute(
        "INSERT INTO score_rules(guild_id,name,kind,win_points,draw_points,loss_points,round_points,tiebreak,created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (guild_id, "Versus Standard", "versus", 3, 1, 0, 0, json.dumps(["points", "round_diff", "wins"]), now))


def list_rules(conn: sqlite3.Connection, guild_id: int) -> list[dict[str, Any]]:
    ensure_default_rules(conn, guild_id)
    return conn.execute("SELECT * FROM score_rules WHERE guild_id=? ORDER BY is_default DESC, name", (guild_id,)).fetchall()


def get_rule(conn: sqlite3.Connection, guild_id: int, rule_id: int | None) -> Rule:
    """Rule for an event: explicit id, else guild default, else first rule."""
    ensure_default_rules(conn, guild_id)
    row = None
    if rule_id:
        row = conn.execute("SELECT * FROM score_rules WHERE id=? AND guild_id=?", (rule_id, guild_id)).fetchone()
    if row is None:
        row = conn.execute("SELECT * FROM score_rules WHERE guild_id=? ORDER BY is_default DESC, id LIMIT 1",
                           (guild_id,)).fetchone()
    return Rule.from_row(row)


def save_rule(conn: sqlite3.Connection, guild_id: int, actor_id: int, rule: Rule, rule_id: int | None = None) -> int:
    if rule.kind not in ("br", "versus"):
        raise ServiceError("Rule type must be 'br' or 'versus'.")
    if rule.kind == "br" and not rule.placement_points:
        raise ServiceError("A battle-royale rule needs placement points.")
    if any(k not in TIEBREAK_KEYS for k in rule.tiebreak):
        raise ServiceError(f"Tie-break keys must be from: {', '.join(TIEBREAK_KEYS)}.")
    vals = (rule.name, rule.kind, json.dumps(rule.placement_points), rule.kill_points, rule.win_points,
            rule.draw_points, rule.loss_points, rule.round_points, json.dumps(rule.tiebreak or ["points"]))
    try:
        if rule_id:
            cur = conn.execute(
                "UPDATE score_rules SET name=?,kind=?,placement_points=?,kill_points=?,win_points=?,draw_points=?,"
                "loss_points=?,round_points=?,tiebreak=? WHERE id=? AND guild_id=?", (*vals, rule_id, guild_id))
            if cur.rowcount == 0:
                raise ServiceError("Rule not found.")
            new_id = rule_id
            # points depend on the rule, so refresh every affected event
            recalc_events_using_rule(conn, guild_id, rule_id)
        else:
            cur = conn.execute(
                "INSERT INTO score_rules(guild_id,name,kind,placement_points,kill_points,win_points,draw_points,"
                "loss_points,round_points,tiebreak,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (guild_id, *vals, now_iso()))
            new_id = cur.lastrowid
    except sqlite3.IntegrityError:
        raise ServiceError("A scoring rule with that name already exists.") from None
    audit(conn, guild_id, actor_id, "score_rule.save", f"rule:{new_id}", rule.name)
    return int(new_id)


def set_default_rule(conn: sqlite3.Connection, guild_id: int, actor_id: int, rule_id: int) -> None:
    if not conn.execute("SELECT 1 FROM score_rules WHERE id=? AND guild_id=?", (rule_id, guild_id)).fetchone():
        raise ServiceError("Rule not found.")
    conn.execute("UPDATE score_rules SET is_default=0 WHERE guild_id=?", (guild_id,))
    conn.execute("UPDATE score_rules SET is_default=1 WHERE id=?", (rule_id,))
    audit(conn, guild_id, actor_id, "score_rule.default", f"rule:{rule_id}")


def recalc_events_using_rule(conn: sqlite3.Connection, guild_id: int, rule_id: int) -> None:
    for table, kind in (("tournaments", "tournament"), ("scrims", "scrim")):
        for r in conn.execute(f"SELECT id FROM {table} WHERE guild_id=? AND score_rule_id=?", (guild_id, rule_id)):  # noqa: S608
            recalc_standings(conn, guild_id, kind, r["id"])


# ---------------------------------------------------------------------- results (DB)
def _event_rule_id(conn: sqlite3.Connection, kind: str, event_id: int) -> int | None:
    table = "tournaments" if kind == "tournament" else "scrims"
    row = conn.execute(f"SELECT score_rule_id FROM {table} WHERE id=?", (event_id,)).fetchone()  # noqa: S608
    return row["score_rule_id"] if row else None


def rule_for_match(conn: sqlite3.Connection, match: dict[str, Any]) -> Rule:
    return get_rule(conn, match["guild_id"], _event_rule_id(conn, match["kind"], match["event_id"]))


def submit_result(conn: sqlite3.Connection, *, match_id: int, team_id: int, user_id: int,
                  placement: int | None = None, kills: int | None = 0, score_for: int | None = None,
                  score_against: int | None = None, evidence: list[str] | None = None,
                  notes: str | None = None) -> tuple[int, bool]:
    """Create (or replace a PENDING/REJECTED) result. Returns (result_id, created). Never duplicates points:
    one row per (match, team); APPROVED/DISPUTED rows are frozen until a referee reopens them."""
    match = conn.execute("SELECT * FROM matches WHERE id=?", (match_id,)).fetchone()
    if not match:
        raise ServiceError("Match not found.")
    if match["status"] == "CANCELLED":
        raise ServiceError("This match was cancelled.")
    rule = rule_for_match(conn, match)
    validate_result_input(rule, placement=placement, kills=kills, score_for=score_for, score_against=score_against)
    now = now_iso()
    existing = conn.execute("SELECT * FROM match_results WHERE match_id=? AND team_id=?", (match_id, team_id)).fetchone()
    ev = json.dumps((evidence or [])[:10])
    if existing:
        if existing["status"] in ("APPROVED", "DISPUTED"):
            raise ServiceError(f"A result for this match is already {existing['status'].lower()}. "
                               "Open a dispute/appeal if it is wrong.")
        conn.execute(
            "UPDATE match_results SET placement=?,kills=?,score_for=?,score_against=?,status='PENDING',submitted_by=?,"
            "evidence=?,notes=?,reviewed_by=NULL,review_reason=NULL,reviewed_at=NULL,updated_at=? WHERE id=?",
            (placement, kills or 0, score_for, score_against, user_id, ev, notes, now, existing["id"]))
        return int(existing["id"]), False
    if match["kind"] == "scrim" and match["lobby_id"]:
        reg = conn.execute("SELECT lobby_id FROM scrim_registrations WHERE scrim_id=? AND team_id=? AND status='APPROVED'",
                           (match["event_id"], team_id)).fetchone()
        if not reg or reg["lobby_id"] != match["lobby_id"]:
            raise ServiceError("Your team is not assigned to this lobby.")
    cur = conn.execute(
        "INSERT INTO match_results(match_id,team_id,placement,kills,score_for,score_against,status,submitted_by,"
        "evidence,notes,created_at,updated_at) VALUES (?,?,?,?,?,?,'PENDING',?,?,?,?,?)",
        (match_id, team_id, placement, kills or 0, score_for, score_against, user_id, ev, notes, now, now))
    return int(cur.lastrowid), True


def scoring_add_evidence(conn: sqlite3.Connection, result_id: int, urls: list[str]) -> int:
    row = conn.execute("SELECT evidence, status FROM match_results WHERE id=?", (result_id,)).fetchone()
    if not row:
        raise ServiceError("Result not found.")
    current = json.loads(row["evidence"] or "[]")
    merged = list(dict.fromkeys(current + urls))[:10]
    conn.execute("UPDATE match_results SET evidence=?, updated_at=? WHERE id=?", (json.dumps(merged), now_iso(), result_id))
    return len(merged)


def review_result(conn: sqlite3.Connection, *, result_id: int, reviewer_id: int, approve: bool,
                  reason: str | None, bonus: float | None = None) -> dict[str, Any]:
    row = conn.execute(
        "SELECT r.*, m.kind, m.event_id, m.guild_id FROM match_results r JOIN matches m ON m.id=r.match_id WHERE r.id=?",
        (result_id,)).fetchone()
    if not row:
        raise ServiceError("Result not found.")
    if row["status"] not in ("PENDING", "DISPUTED"):
        raise ServiceError(f"This result is already {row['status'].lower()}.")
    if not approve and not (reason or "").strip():
        raise ServiceError("A reason is required when rejecting a result.")
    new = "APPROVED" if approve else "REJECTED"
    conn.execute(
        "UPDATE match_results SET status=?, reviewed_by=?, review_reason=?, reviewed_at=?, updated_at=?, bonus=COALESCE(?,bonus) WHERE id=?",
        (new, reviewer_id, reason, now_iso(), now_iso(), bonus, result_id))
    audit(conn, row["guild_id"], reviewer_id, f"result.{new.lower()}", f"result:{result_id}", reason)
    recalc_standings(conn, row["guild_id"], row["kind"], row["event_id"])
    row["status"] = new
    return row


def correct_result(conn: sqlite3.Connection, *, result_id: int, reviewer_id: int, reason: str,
                   placement: int | None, kills: int | None, bonus: float | None,
                   score_for: int | None, score_against: int | None) -> None:
    row = conn.execute(
        "SELECT r.*, m.kind, m.event_id, m.guild_id FROM match_results r JOIN matches m ON m.id=r.match_id WHERE r.id=?",
        (result_id,)).fetchone()
    if not row:
        raise ServiceError("Result not found.")
    if not (reason or "").strip():
        raise ServiceError("A reason is required for corrections.")
    match = conn.execute("SELECT * FROM matches WHERE id=?", (row["match_id"],)).fetchone()
    rule = rule_for_match(conn, match)
    validate_result_input(rule, placement=placement, kills=kills, score_for=score_for, score_against=score_against)
    conn.execute(
        "UPDATE match_results SET placement=?,kills=?,bonus=?,score_for=?,score_against=?,reviewed_by=?,review_reason=?,"
        "reviewed_at=?,updated_at=? WHERE id=?",
        (placement, kills or 0, bonus or 0, score_for, score_against, reviewer_id, f"Corrected: {reason}",
         now_iso(), now_iso(), result_id))
    audit(conn, row["guild_id"], reviewer_id, "result.correct", f"result:{result_id}", reason)
    recalc_standings(conn, row["guild_id"], row["kind"], row["event_id"])


def set_result_status(conn: sqlite3.Connection, *, result_id: int, status: str, actor_id: int, reason: str | None) -> None:
    """Used by dispute handling to freeze (DISPUTED) or settle a result."""
    if status not in RESULT_STATES:
        raise ServiceError("Invalid result status.")
    row = conn.execute(
        "SELECT r.id, m.kind, m.event_id, m.guild_id FROM match_results r JOIN matches m ON m.id=r.match_id WHERE r.id=?",
        (result_id,)).fetchone()
    if not row:
        raise ServiceError("Result not found.")
    conn.execute("UPDATE match_results SET status=?, reviewed_by=?, review_reason=?, reviewed_at=?, updated_at=? WHERE id=?",
                 (status, actor_id, reason, now_iso(), now_iso(), result_id))
    audit(conn, row["guild_id"], actor_id, f"result.set_{status.lower()}", f"result:{result_id}", reason)
    recalc_standings(conn, row["guild_id"], row["kind"], row["event_id"])


def recalc_standings(conn: sqlite3.Connection, guild_id: int, kind: str, event_id: int) -> list[dict[str, Any]]:
    """Rebuild standings for one event from APPROVED results only (idempotent)."""
    rule = get_rule(conn, guild_id, _event_rule_id(conn, kind, event_id))
    rows = conn.execute(
        "SELECT r.* FROM match_results r JOIN matches m ON m.id=r.match_id "
        "WHERE m.kind=? AND m.event_id=? AND r.status='APPROVED'", (kind, event_id)).fetchall()
    table = compute_standings(rule, rows)
    conn.execute("DELETE FROM standings WHERE kind=? AND event_id=?", (kind, event_id))
    for t in table:
        conn.execute(
            "INSERT INTO standings(kind,event_id,team_id,rank,played,points,kills,wins,best_placement,round_diff) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (kind, event_id, t["team_id"], t["rank"], t["played"], t["points"], t["kills"], t["wins"],
             t["best_placement"], t["round_diff"]))
    return table


def get_standings(conn: sqlite3.Connection, kind: str, event_id: int) -> list[dict[str, Any]]:
    return conn.execute(
        "SELECT s.*, t.name, t.tag FROM standings s JOIN teams t ON t.id=s.team_id "
        "WHERE s.kind=? AND s.event_id=? ORDER BY s.rank, t.name", (kind, event_id)).fetchall()


def season_rankings(conn: sqlite3.Connection, guild_id: int, season: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    """Aggregate standings across every event (optionally limited to a season)."""
    params: list[Any] = [guild_id]
    cond_t = cond_s = ""
    if season:
        cond_t = cond_s = " AND e.season=?"
    sql = (
        "SELECT t.id AS team_id, t.name, t.tag, SUM(x.points) AS points, SUM(x.kills) AS kills, SUM(x.wins) AS wins, "
        "SUM(x.played) AS played, COUNT(*) AS events FROM ("
        f" SELECT s.* FROM standings s JOIN tournaments e ON s.kind='tournament' AND e.id=s.event_id WHERE e.guild_id=?{cond_t}"
        " UNION ALL"
        f" SELECT s.* FROM standings s JOIN scrims e ON s.kind='scrim' AND e.id=s.event_id WHERE e.guild_id=?{cond_s}"
        ") x JOIN teams t ON t.id=x.team_id GROUP BY t.id ORDER BY points DESC, kills DESC, wins DESC, t.name LIMIT ?")
    if season:
        params += [season, guild_id, season]
    else:
        params += [guild_id]
    params.append(limit)
    return conn.execute(sql, params).fetchall()

# --------------------------------------------------------------------------------------------------
# services/match_service.py
# Matches and room credentials (shared by tournaments and scrims).
# --------------------------------------------------------------------------------------------------

def add_match(conn: sqlite3.Connection, *, guild_id: int, kind: str, event_id: int, actor_id: int,
              map_name: str | None = None, scheduled_at: str | None = None, lobby_id: int | None = None,
              number: int | None = None) -> int:
    if kind not in ("tournament", "scrim"):
        raise ServiceError("Unknown event type.")
    if number is None:
        row = conn.execute("SELECT COALESCE(MAX(number),0) AS n FROM matches WHERE kind=? AND event_id=? AND COALESCE(lobby_id,0)=COALESCE(?,0)",
                           (kind, event_id, lobby_id)).fetchone()
        number = row["n"] + 1
    if not 1 <= number <= 99:
        raise ServiceError("Match number must be between 1 and 99.")
    try:
        cur = conn.execute(
            "INSERT INTO matches(guild_id,kind,event_id,lobby_id,number,map,scheduled_at,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (guild_id, kind, event_id, lobby_id, number, map_name, scheduled_at, now_iso()))
    except sqlite3.IntegrityError:
        raise ServiceError(f"Match #{number} already exists.") from None
    audit(conn, guild_id, actor_id, f"{kind}.add_match", f"{kind}:{event_id}", f"match:{cur.lastrowid}")
    return int(cur.lastrowid)


def get_match(conn: sqlite3.Connection, match_id: int) -> dict[str, Any]:
    m = conn.execute(
        "SELECT m.*, l.number AS lobby_number FROM matches m LEFT JOIN lobbies l ON l.id=m.lobby_id WHERE m.id=?", (match_id,)).fetchone()
    if not m:
        raise ServiceError("Match not found.")
    return m


def list_matches(conn: sqlite3.Connection, kind: str, event_id: int) -> list[dict[str, Any]]:
    return conn.execute(
        "SELECT m.*, l.number AS lobby_number FROM matches m LEFT JOIN lobbies l ON l.id=m.lobby_id "
        "WHERE m.kind=? AND m.event_id=? ORDER BY m.number, COALESCE(l.number,0)", (kind, event_id)).fetchall()


def match_label(m: dict[str, Any]) -> str:
    base = f"Match {m['number']}"
    if m.get("lobby_number"):
        base = f"Lobby {m['lobby_number']} • {base}"
    return base + (f" ({m['map']})" if m.get("map") else "")


def match_set_room(conn: sqlite3.Connection, match_id: int, room_id: str, password: str, actor_id: int) -> None:
    m = get_match(conn, match_id)
    conn.execute("UPDATE matches SET room_id=?, room_password=?, room_released=0 WHERE id=?", (room_id, password, match_id))
    audit(conn, m["guild_id"], actor_id, f"{m['kind']}.set_room", f"match:{match_id}")  # credentials are never logged


def release_room(conn: sqlite3.Connection, match_id: int, flag: bool, actor_id: int) -> None:
    m = get_match(conn, match_id)
    if flag and not m["room_id"]:
        raise ServiceError("Set the room ID and password first.")
    conn.execute("UPDATE matches SET room_released=? WHERE id=?", (1 if flag else 0, match_id))
    audit(conn, m["guild_id"], actor_id, f"{m['kind']}.{'release' if flag else 'hide'}_room", f"match:{match_id}")


def eligible_team_ids(conn: sqlite3.Connection, match: dict[str, Any]) -> list[int]:
    """Teams that may see room credentials: approved + checked in (and in the right lobby for scrims)."""
    if match["kind"] == "tournament":
        rows = conn.execute("SELECT team_id FROM tournament_registrations WHERE tournament_id=? AND status='APPROVED' AND checked_in=1",
                            (match["event_id"],)).fetchall()
    else:
        sql = "SELECT team_id FROM scrim_registrations WHERE scrim_id=? AND status='APPROVED' AND checked_in=1"
        params: list[Any] = [match["event_id"]]
        if match["lobby_id"]:
            sql += " AND lobby_id=?"
            params.append(match["lobby_id"])
        rows = conn.execute(sql, params).fetchall()
    return [r["team_id"] for r in rows]


def eligible_user_ids(conn: sqlite3.Connection, match: dict[str, Any]) -> list[int]:
    """Roster-snapshot players (starters + subs) of eligible teams."""
    out: list[int] = []
    if match["kind"] == "tournament":
        sql = ("SELECT p.user_id FROM tournament_reg_players p JOIN tournament_registrations r ON r.id=p.reg_id "
               "WHERE r.tournament_id=? AND r.status='APPROVED' AND r.checked_in=1")
        out = [r["user_id"] for r in conn.execute(sql, (match["event_id"],))]
    else:
        sql = ("SELECT p.user_id FROM scrim_reg_players p JOIN scrim_registrations r ON r.id=p.reg_id "
               "WHERE r.scrim_id=? AND r.status='APPROVED' AND r.checked_in=1")
        params: list[Any] = [match["event_id"]]
        if match["lobby_id"]:
            sql += " AND r.lobby_id=?"
            params.append(match["lobby_id"])
        out = [r["user_id"] for r in conn.execute(sql, params)]
    return list(dict.fromkeys(out))


def user_can_see_room(conn: sqlite3.Connection, match: dict[str, Any], user_id: int) -> bool:
    return bool(match["room_released"]) and user_id in eligible_user_ids(conn, match)


def submittable_matches(conn: sqlite3.Connection, team_id: int) -> list[dict[str, Any]]:
    """Matches the team may still submit a result for (approved registration, no frozen result)."""
    rows = conn.execute(
        "SELECT m.*, l.number AS lobby_number, 'tournament' AS ekind, e.title AS title FROM matches m "
        "JOIN tournaments e ON e.id=m.event_id AND m.kind='tournament' "
        "JOIN tournament_registrations r ON r.tournament_id=e.id AND r.team_id=? AND r.status='APPROVED' "
        "LEFT JOIN lobbies l ON l.id=m.lobby_id WHERE m.status<>'CANCELLED' AND e.status IN ('PUBLISHED','LIVE','COMPLETED') "
        "UNION ALL "
        "SELECT m.*, l.number AS lobby_number, 'scrim' AS ekind, e.title AS title FROM matches m "
        "JOIN scrims e ON e.id=m.event_id AND m.kind='scrim' "
        "JOIN scrim_registrations r ON r.scrim_id=e.id AND r.team_id=? AND r.status='APPROVED' AND (m.lobby_id IS NULL OR r.lobby_id=m.lobby_id) "
        "LEFT JOIN lobbies l ON l.id=m.lobby_id WHERE m.status<>'CANCELLED' AND e.status IN ('PUBLISHED','LIVE','COMPLETED') "
        "ORDER BY 1 DESC", (team_id, team_id)).fetchall()
    out = []
    for m in rows:
        res = conn.execute("SELECT status FROM match_results WHERE match_id=? AND team_id=?", (m["id"], team_id)).fetchone()
        if res and res["status"] in ("APPROVED", "DISPUTED"):
            continue
        m["existing"] = res["status"] if res else None
        out.append(m)
    return out


def pending_results(conn: sqlite3.Connection, guild_id: int, limit: int = 100) -> list[dict[str, Any]]:
    return conn.execute(
        "SELECT r.*, t.name AS team_name, m.kind, m.event_id, m.number, m.lobby_id, "
        "CASE m.kind WHEN 'tournament' THEN (SELECT title FROM tournaments WHERE id=m.event_id) "
        "ELSE (SELECT title FROM scrims WHERE id=m.event_id) END AS event_title "
        "FROM match_results r JOIN matches m ON m.id=r.match_id JOIN teams t ON t.id=r.team_id "
        "WHERE m.guild_id=? AND r.status IN ('PENDING','DISPUTED') ORDER BY r.created_at LIMIT ?", (guild_id, limit)).fetchall()


def get_result(conn: sqlite3.Connection, result_id: int) -> dict[str, Any]:
    r = conn.execute(
        "SELECT r.*, t.name AS team_name, m.kind, m.event_id, m.number, m.lobby_id, m.guild_id, "
        "CASE m.kind WHEN 'tournament' THEN (SELECT title FROM tournaments WHERE id=m.event_id) "
        "ELSE (SELECT title FROM scrims WHERE id=m.event_id) END AS event_title, "
        "CASE m.kind WHEN 'tournament' THEN (SELECT organizer_id FROM tournaments WHERE id=m.event_id) "
        "ELSE (SELECT organizer_id FROM scrims WHERE id=m.event_id) END AS organizer_id "
        "FROM match_results r JOIN matches m ON m.id=r.match_id JOIN teams t ON t.id=r.team_id WHERE r.id=?", (result_id,)).fetchone()
    if not r:
        raise ServiceError("Result not found.")
    return r


def assigned_referees(conn: sqlite3.Connection, kind: str, event_id: int) -> set[int]:
    return {r["user_id"] for r in conn.execute("SELECT user_id FROM referee_assignments WHERE kind=? AND event_id=?", (kind, event_id))}


def recent_results(conn: sqlite3.Connection, guild_id: int, limit: int = 30) -> list[dict[str, Any]]:
    return conn.execute(
        "SELECT r.*, t.name AS team_name, m.kind, m.event_id, m.number, "
        "CASE m.kind WHEN 'tournament' THEN (SELECT title FROM tournaments WHERE id=m.event_id) "
        "ELSE (SELECT title FROM scrims WHERE id=m.event_id) END AS event_title "
        "FROM match_results r JOIN matches m ON m.id=r.match_id JOIN teams t ON t.id=r.team_id "
        "WHERE m.guild_id=? AND r.status='APPROVED' ORDER BY r.reviewed_at DESC LIMIT ?", (guild_id, limit)).fetchall()


def user_match_history(conn: sqlite3.Connection, guild_id: int, user_id: int, limit: int = 30) -> list[dict[str, Any]]:
    """Approved results of events where the user was on the registered roster snapshot."""
    return conn.execute(
        "SELECT r.*, t.name AS team_name, m.kind, m.event_id, m.number, e.title AS event_title FROM match_results r "
        "JOIN matches m ON m.id=r.match_id JOIN teams t ON t.id=r.team_id "
        "JOIN tournaments e ON m.kind='tournament' AND e.id=m.event_id "
        "JOIN tournament_registrations tr ON tr.tournament_id=e.id AND tr.team_id=r.team_id "
        "JOIN tournament_reg_players p ON p.reg_id=tr.id AND p.user_id=? "
        "WHERE m.guild_id=? AND r.status='APPROVED' "
        "UNION ALL "
        "SELECT r.*, t.name, m.kind, m.event_id, m.number, e.title FROM match_results r "
        "JOIN matches m ON m.id=r.match_id JOIN teams t ON t.id=r.team_id "
        "JOIN scrims e ON m.kind='scrim' AND e.id=m.event_id "
        "JOIN scrim_registrations sr ON sr.scrim_id=e.id AND sr.team_id=r.team_id "
        "JOIN scrim_reg_players p ON p.reg_id=sr.id AND p.user_id=? "
        "WHERE m.guild_id=? AND r.status='APPROVED' ORDER BY 1 DESC LIMIT ?", (user_id, guild_id, user_id, guild_id, limit)).fetchall()

# --------------------------------------------------------------------------------------------------
# services/tournament_service.py
# Tournament lifecycle: draft -> (scheduled) -> published -> live -> completed / cancelled.
# --------------------------------------------------------------------------------------------------

MODE_SIZE = {"solo": 1, "duo": 2, "squad": 4}
TOURNAMENT_EDITABLE = {"title", "game", "mode", "team_size", "map", "region", "start_time", "entry_fee", "prize", "max_teams",
            "reserve_slots", "reg_open", "reg_close", "publish_at", "rules", "poster_url", "organizer_info",
            "score_rule_id", "channel_id", "message_id", "reminded"}
LOCKED = ("COMPLETED", "CANCELLED")


def tournament_get(conn: sqlite3.Connection, tid: int) -> dict[str, Any]:
    t = conn.execute("SELECT * FROM tournaments WHERE id=?", (tid,)).fetchone()
    if not t:
        raise ServiceError("That tournament no longer exists.")
    return t


def tournament_create_draft(conn: sqlite3.Connection, guild_id: int, organizer_id: int, *, title: str, game: str, mode: str,
                 map_name: str | None, start_time: str) -> int:
    if mode not in MODE_SIZE:
        raise ServiceError("Mode must be solo, duo or squad.")
    season = get_setting(conn, guild_id, "season")
    now = now_iso()
    cur = conn.execute(
        "INSERT INTO tournaments(guild_id,title,game,mode,team_size,map,start_time,organizer_id,season,created_at,updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (guild_id, title, game, mode, MODE_SIZE[mode], map_name, start_time, organizer_id, season, now, now))
    tid = int(cur.lastrowid)
    audit(conn, guild_id, organizer_id, "tournament.create", f"tournament:{tid}", title)
    return tid


def tournament_update(conn: sqlite3.Connection, tid: int, actor_id: int, **fields: Any) -> dict[str, Any]:
    t = tournament_get(conn, tid)
    if t["status"] in LOCKED:
        raise ServiceError(f"A {t['status'].lower()} tournament cannot be edited.")
    bad = set(fields) - TOURNAMENT_EDITABLE
    if bad:
        raise ServiceError("Those fields cannot be edited.")
    if "mode" in fields:
        if fields["mode"] not in MODE_SIZE:
            raise ServiceError("Mode must be solo, duo or squad.")
        fields.setdefault("team_size", MODE_SIZE[fields["mode"]])
    merged = {**t, **fields}
    _check_schedule(merged)
    if "max_teams" in fields:
        active = conn.execute("SELECT COUNT(*) AS c FROM tournament_registrations WHERE tournament_id=? AND status IN ('PENDING','APPROVED')", (tid,)).fetchone()["c"]
        if fields["max_teams"] < active:
            raise ServiceError(f"{active} teams are already registered; max teams cannot go below that.")
    if "start_time" in fields and fields["start_time"] != t["start_time"]:
        fields.setdefault("reminded", 0)
    if not fields:
        return t
    sets = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE tournaments SET {sets}, updated_at=? WHERE id=?", (*fields.values(), now_iso(), tid))  # noqa: S608
    audit(conn, t["guild_id"], actor_id, "tournament.update", f"tournament:{tid}", ",".join(fields))
    return tournament_get(conn, tid)


def _check_schedule(t: dict[str, Any]) -> None:
    if t.get("reg_open") and t.get("reg_close") and t["reg_open"] >= t["reg_close"]:
        raise ServiceError("Registration must close after it opens.")
    if t.get("reg_close") and t.get("start_time") and t["reg_close"] > t["start_time"]:
        raise ServiceError("Registration cannot close after the tournament starts.")
    if t.get("max_teams") is not None and not 2 <= t["max_teams"] <= 512:
        raise ServiceError("Max teams must be between 2 and 512.")
    if t.get("reserve_slots") is not None and not 0 <= t["reserve_slots"] <= 100:
        raise ServiceError("Reserve slots must be between 0 and 100.")
    if t.get("team_size") is not None and not 1 <= t["team_size"] <= 10:
        raise ServiceError("Team size must be between 1 and 10.")


def publish(conn: sqlite3.Connection, tid: int, actor_id: int, now: str | None = None) -> str:
    t = tournament_get(conn, tid)
    now = now or now_iso()
    if t["status"] not in ("DRAFT", "SCHEDULED"):
        raise ServiceError(f"This tournament is already {t['status'].lower()}.")
    if not t["start_time"]:
        raise ServiceError("Set a start time before publishing.")
    status = "SCHEDULED" if (t["publish_at"] and t["publish_at"] > now) else "PUBLISHED"
    conn.execute("UPDATE tournaments SET status=?, updated_at=? WHERE id=?", (status, now_iso(), tid))
    audit(conn, t["guild_id"], actor_id, f"tournament.{status.lower()}", f"tournament:{tid}")
    return status


def unpublish(conn: sqlite3.Connection, tid: int, actor_id: int) -> None:
    t = tournament_get(conn, tid)
    if t["status"] not in ("PUBLISHED", "SCHEDULED"):
        raise ServiceError("Only published or scheduled tournaments can be unpublished.")
    if conn.execute("SELECT 1 FROM tournament_registrations WHERE tournament_id=? AND status IN ('PENDING','APPROVED','WAITLIST')", (tid,)).fetchone():
        raise ServiceError("Teams are already registered. Cancel the tournament instead.")
    conn.execute("UPDATE tournaments SET status='DRAFT', updated_at=? WHERE id=?", (now_iso(), tid))
    audit(conn, t["guild_id"], actor_id, "tournament.unpublish", f"tournament:{tid}")


def tournament_set_status(conn: sqlite3.Connection, tid: int, actor_id: int, status: str) -> None:
    t = tournament_get(conn, tid)
    allowed = {"LIVE": ("PUBLISHED",), "COMPLETED": ("PUBLISHED", "LIVE"), "CANCELLED": ("DRAFT", "SCHEDULED", "PUBLISHED", "LIVE")}
    if status not in allowed:
        raise ServiceError("Invalid status.")
    if t["status"] not in allowed[status]:
        raise ServiceError(f"A {t['status'].lower()} tournament cannot become {status.lower()}.")
    conn.execute("UPDATE tournaments SET status=?, updated_at=? WHERE id=?", (status, now_iso(), tid))
    audit(conn, t["guild_id"], actor_id, f"tournament.{status.lower()}", f"tournament:{tid}")


def reschedule(conn: sqlite3.Connection, tid: int, actor_id: int, new_start: str) -> dict[str, Any]:
    t = tournament_get(conn, tid)
    if t["status"] in LOCKED:
        raise ServiceError("This tournament can no longer be rescheduled.")
    new = tournament_update(conn, tid, actor_id, start_time=new_start, reminded=0)
    audit(conn, t["guild_id"], actor_id, "tournament.reschedule", f"tournament:{tid}", f"{t['start_time']} -> {new_start}")
    return new


def close_registration(conn: sqlite3.Connection, tid: int, actor_id: int) -> None:
    t = tournament_get(conn, tid)
    conn.execute("UPDATE tournaments SET reg_close=?, updated_at=? WHERE id=?", (now_iso(), now_iso(), tid))
    audit(conn, t["guild_id"], actor_id, "tournament.close_registration", f"tournament:{tid}")


def tournament_delete(conn: sqlite3.Connection, tid: int, actor_id: int) -> None:
    t = tournament_get(conn, tid)
    if t["status"] == "LIVE":
        raise ServiceError("A live tournament cannot be deleted. Cancel or complete it first.")
    conn.execute("DELETE FROM matches WHERE kind='tournament' AND event_id=?", (tid,))  # cascades results
    conn.execute("DELETE FROM standings WHERE kind='tournament' AND event_id=?", (tid,))
    conn.execute("DELETE FROM referee_assignments WHERE kind='tournament' AND event_id=?", (tid,))
    conn.execute("DELETE FROM tournaments WHERE id=?", (tid,))  # cascades registrations
    audit(conn, t["guild_id"], actor_id, "tournament.delete", f"tournament:{tid}", t["title"])


def tournament_list_for_guild(conn: sqlite3.Connection, guild_id: int, statuses: tuple[str, ...] | None = None,
                   organizer_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
    sql = "SELECT * FROM tournaments WHERE guild_id=?"
    params: list[Any] = [guild_id]
    if statuses:
        sql += f" AND status IN ({','.join('?' * len(statuses))})"
        params += list(statuses)
    if organizer_id:
        sql += " AND organizer_id=?"
        params.append(organizer_id)
    sql += " ORDER BY COALESCE(start_time,'9999') DESC, id DESC LIMIT ?"
    params.append(limit)
    return conn.execute(sql, params).fetchall()


def due_to_publish(conn: sqlite3.Connection, now: str) -> list[dict[str, Any]]:
    return conn.execute("SELECT * FROM tournaments WHERE status='SCHEDULED' AND publish_at IS NOT NULL AND publish_at<=?", (now,)).fetchall()


def mark_published(conn: sqlite3.Connection, tid: int) -> None:
    conn.execute("UPDATE tournaments SET status='PUBLISHED', updated_at=? WHERE id=? AND status='SCHEDULED'", (now_iso(), tid))


def tournament_due_reminders(conn: sqlite3.Connection, now: str, horizon: str) -> list[dict[str, Any]]:
    return conn.execute(
        "SELECT * FROM tournaments WHERE status IN ('PUBLISHED','LIVE') AND reminded=0 AND start_time IS NOT NULL "
        "AND start_time>? AND start_time<=?", (now, horizon)).fetchall()

# --------------------------------------------------------------------------------------------------
# services/registration_service.py
# Registration logic shared by tournaments and scrims (approval, waitlist, check-in, rosters).
# --------------------------------------------------------------------------------------------------

REGISTRATION_KINDS = {
    "tournament": dict(event="tournaments", reg="tournament_registrations", players="tournament_reg_players", fk="tournament_id"),
    "scrim": dict(event="scrims", reg="scrim_registrations", players="scrim_reg_players", fk="scrim_id"),
}
ACTIVE = ("PENDING", "APPROVED")


def _k(kind: str) -> dict[str, str]:
    if kind not in REGISTRATION_KINDS:
        raise ServiceError("Unknown event type.")
    return REGISTRATION_KINDS[kind]


def get_event(conn: sqlite3.Connection, kind: str, event_id: int) -> dict[str, Any]:
    ev = conn.execute(f"SELECT * FROM {_k(kind)['event']} WHERE id=?", (event_id,)).fetchone()  # noqa: S608
    if not ev:
        raise ServiceError("That event no longer exists.")
    return ev


def capacity(kind: str, ev: dict[str, Any], waitlist_on: bool) -> tuple[int, int]:
    """(main capacity, waitlist capacity)."""
    if kind == "tournament":
        return ev["max_teams"], (ev["reserve_slots"] if waitlist_on else 0)
    cap = ev["lobby_count"] * ev["lobby_capacity"]
    return cap, (ev["lobby_capacity"] if waitlist_on else 0)


def registration_counts(conn: sqlite3.Connection, kind: str, event_id: int) -> dict[str, int]:
    k = _k(kind)
    rows = conn.execute(f"SELECT status, COUNT(*) AS c FROM {k['reg']} WHERE {k['fk']}=? GROUP BY status", (event_id,)).fetchall()  # noqa: S608
    return {r["status"]: r["c"] for r in rows}


def registration_state(kind: str, ev: dict[str, Any], c: dict[str, int], waitlist_on: bool, now_iso_value: str | None = None) -> str:
    """OPEN | WAITLIST | FULL | NOT_OPEN | CLOSED | UNPUBLISHED | CANCELLED | COMPLETED"""
    now = now_iso_value or now_iso()
    st = ev["status"]
    if st == "CANCELLED":
        return "CANCELLED"
    if st == "COMPLETED":
        return "COMPLETED"
    if st in ("DRAFT", "SCHEDULED"):
        return "UNPUBLISHED"
    if st == "PAUSED":
        return "CLOSED"
    if kind == "tournament":
        if ev.get("reg_open") and now < ev["reg_open"]:
            return "NOT_OPEN"
        close = ev.get("reg_close") or ev.get("start_time")
        if st == "LIVE" or (close and now >= close):
            return "CLOSED"
    else:
        if ev.get("start_time") and now >= ev["start_time"]:
            return "CLOSED"
    cap, wl = capacity(kind, ev, waitlist_on)
    active = c.get("PENDING", 0) + c.get("APPROVED", 0)
    if active < cap:
        return "OPEN"
    if wl and c.get("WAITLIST", 0) < wl:
        return "WAITLIST"
    return "FULL"


def _snapshot_roster(conn: sqlite3.Connection, kind: str, reg_id: int, team_id: int, team_size: int) -> None:
    k = _k(kind)
    conn.execute(f"DELETE FROM {k['players']} WHERE reg_id=?", (reg_id,))  # noqa: S608
    mem = team_members(conn, team_id)
    mains = [m for m in mem if m["role"] != "sub"]
    subs = [m for m in mem if m["role"] == "sub"]
    for i, m in enumerate(mains):
        conn.execute(f"INSERT INTO {k['players']}(reg_id,user_id,is_sub) VALUES (?,?,?)", (reg_id, m["user_id"], 0 if i < team_size else 1))  # noqa: S608
    for m in subs:
        conn.execute(f"INSERT INTO {k['players']}(reg_id,user_id,is_sub) VALUES (?,?,1)", (reg_id, m["user_id"]))  # noqa: S608


def register(conn: sqlite3.Connection, kind: str, event_id: int, team_id: int, actor_id: int, *, admin: bool = False) -> dict[str, Any]:
    k = _k(kind)
    ev = get_event(conn, kind, event_id)
    guild_id = ev["guild_id"]
    team = get_team(conn, team_id)
    if not team or team["disbanded"] or team["guild_id"] != guild_id:
        raise ServiceError("Team not found.")
    if not admin:
        m = conn.execute("SELECT role FROM team_members WHERE team_id=? AND user_id=?", (team_id, actor_id)).fetchone()
        if not m or m["role"] not in ("captain", "cocaptain"):
            raise ServiceError("Only the captain or co-captain can register the team.")
    if get_bool(conn, guild_id, "require_verified_team") and not team["verified"]:
        raise ServiceError("This server only allows verified teams to register. Ask staff to verify your team.")
    mains, _subs = team_counts(conn, team_id)
    if mains < ev["team_size"]:
        raise ServiceError(f"Your team needs at least {ev['team_size']} main players for this event (you have {mains}).")
    waitlist_on = get_bool(conn, guild_id, "waitlist")
    c = registration_counts(conn, kind, event_id)
    state = registration_state(kind, ev, c, waitlist_on)
    if state not in ("OPEN", "WAITLIST"):
        raise ServiceError({"NOT_OPEN": "Registration has not opened yet.", "CLOSED": "Registration is closed.",
                            "FULL": "This event is full.", "UNPUBLISHED": "This event is not published.",
                            "CANCELLED": "This event was cancelled.", "COMPLETED": "This event has finished."}[state])
    existing = conn.execute(f"SELECT * FROM {k['reg']} WHERE {k['fk']}=? AND team_id=?", (event_id, team_id)).fetchone()  # noqa: S608
    if existing and existing["status"] != "WITHDRAWN":
        raise ServiceError(f"Your team is already registered ({existing['status'].title()}).")
    free = _is_free(ev.get("entry_fee") if kind == "tournament" else ev.get("fee"))
    status = "WAITLIST" if state == "WAITLIST" else ("APPROVED" if (free and get_bool(conn, guild_id, "auto_approve_free")) else "PENDING")
    now = now_iso()
    if existing:
        conn.execute(f"UPDATE {k['reg']} SET status=?, checked_in=0, registered_by=?, updated_at=? WHERE id=?",  # noqa: S608
                     (status, actor_id, now, existing["id"]))
        reg_id = existing["id"]
    else:
        cur = conn.execute(f"INSERT INTO {k['reg']}({k['fk']},team_id,status,registered_by,created_at,updated_at) VALUES (?,?,?,?,?,?)",  # noqa: S608
                           (event_id, team_id, status, actor_id, now, now))
        reg_id = int(cur.lastrowid)
    _snapshot_roster(conn, kind, reg_id, team_id, ev["team_size"])
    audit(conn, guild_id, actor_id, f"{kind}.register", f"{kind}:{event_id}", f"team:{team_id} -> {status}")
    return {"reg_id": reg_id, "status": status, "event": ev, "team": team}


def _is_free(fee: str | None) -> bool:
    f = (fee or "").strip().lower()
    return f in ("", "free", "0", "none", "no", "n/a", "₹0", "$0")


def get_reg(conn: sqlite3.Connection, kind: str, reg_id: int) -> dict[str, Any]:
    k = _k(kind)
    r = conn.execute(f"SELECT * FROM {k['reg']} WHERE id=?", (reg_id,)).fetchone()  # noqa: S608
    if not r:
        raise ServiceError("Registration not found.")
    return r


def team_reg(conn: sqlite3.Connection, kind: str, event_id: int, team_id: int) -> dict[str, Any] | None:
    k = _k(kind)
    return conn.execute(f"SELECT * FROM {k['reg']} WHERE {k['fk']}=? AND team_id=?", (event_id, team_id)).fetchone()  # noqa: S608


def registration_set_status(conn: sqlite3.Connection, kind: str, reg_id: int, status: str, actor_id: int) -> dict[str, Any]:
    """Organizer decision: APPROVED / REJECTED / WAITLIST. Caller must have checked can_manage_event."""
    k = _k(kind)
    if status not in ("APPROVED", "REJECTED", "WAITLIST", "PENDING"):
        raise ServiceError("Invalid status.")
    reg = get_reg(conn, kind, reg_id)
    ev = get_event(conn, kind, reg[k["fk"]])
    if reg["status"] == "WITHDRAWN":
        raise ServiceError("That team withdrew.")
    if status == "APPROVED":
        cap, _ = capacity(kind, ev, True)
        approved = registration_counts(conn, kind, ev["id"]).get("APPROVED", 0)
        if reg["status"] != "APPROVED" and approved >= cap:
            raise ServiceError("All slots are already approved. Move a team to the waitlist or reject one first.")
    conn.execute(f"UPDATE {k['reg']} SET status=?, updated_at=? WHERE id=?", (status, now_iso(), reg_id))  # noqa: S608
    audit(conn, ev["guild_id"], actor_id, f"{kind}.reg_{status.lower()}", f"{kind}:{ev['id']}", f"team:{reg['team_id']}")
    if status in ("REJECTED", "WAITLIST") and reg["status"] in ACTIVE:
        promote_waitlist(conn, kind, ev["id"])
    return reg


def withdraw(conn: sqlite3.Connection, kind: str, event_id: int, team_id: int, actor_id: int, *, admin: bool = False) -> None:
    k = _k(kind)
    ev = get_event(conn, kind, event_id)
    reg = team_reg(conn, kind, event_id, team_id)
    if not reg or reg["status"] in ("WITHDRAWN", "REJECTED"):
        raise ServiceError("Your team is not registered.")
    if not admin:
        m = conn.execute("SELECT role FROM team_members WHERE team_id=? AND user_id=?", (team_id, actor_id)).fetchone()
        if not m or m["role"] not in ("captain", "cocaptain"):
            raise ServiceError("Only the captain or co-captain can withdraw the team.")
    was_active = reg["status"] in ACTIVE
    conn.execute(f"UPDATE {k['reg']} SET status='WITHDRAWN', updated_at=? WHERE id=?", (now_iso(), reg["id"]))  # noqa: S608
    audit(conn, ev["guild_id"], actor_id, f"{kind}.withdraw", f"{kind}:{event_id}", f"team:{team_id}")
    if was_active:
        promote_waitlist(conn, kind, event_id)


def promote_waitlist(conn: sqlite3.Connection, kind: str, event_id: int) -> list[dict[str, Any]]:
    """Move the oldest waitlisted teams into open slots (as PENDING so organizers still approve)."""
    k = _k(kind)
    ev = get_event(conn, kind, event_id)
    cap, _ = capacity(kind, ev, True)
    promoted = []
    while True:
        c = registration_counts(conn, kind, event_id)
        if c.get("PENDING", 0) + c.get("APPROVED", 0) >= cap:
            break
        nxt = conn.execute(f"SELECT * FROM {k['reg']} WHERE {k['fk']}=? AND status='WAITLIST' ORDER BY id LIMIT 1", (event_id,)).fetchone()  # noqa: S608
        if not nxt:
            break
        conn.execute(f"UPDATE {k['reg']} SET status='PENDING', updated_at=? WHERE id=?", (now_iso(), nxt["id"]))  # noqa: S608
        promoted.append(nxt)
    return promoted


def check_in(conn: sqlite3.Connection, kind: str, event_id: int, team_id: int, actor_id: int, *, admin: bool = False,
             now: str | None = None) -> None:
    k = _k(kind)
    ev = get_event(conn, kind, event_id)
    reg = team_reg(conn, kind, event_id, team_id)
    if not reg or reg["status"] != "APPROVED":
        raise ServiceError("Only approved teams can check in.")
    if not admin:
        m = conn.execute("SELECT role FROM team_members WHERE team_id=? AND user_id=?", (team_id, actor_id)).fetchone()
        if not m or m["role"] not in ("captain", "cocaptain"):
            raise ServiceError("Only the captain or co-captain can check in.")
        start = ev.get("start_time")
        if start:
            window = get_int(conn, ev["guild_id"], "checkin_window", 60)
            now_dt = from_iso(now) if now else utcnow()
            mins_to_start = (from_iso(start) - now_dt).total_seconds() / 60
            if mins_to_start > window:
                raise ServiceError(f"Check-in opens {window} minutes before the start time.")
            if mins_to_start < -30:
                raise ServiceError("Check-in has closed.")
    if reg["checked_in"]:
        raise ServiceError("Your team is already checked in.")
    conn.execute(f"UPDATE {k['reg']} SET checked_in=1, updated_at=? WHERE id=?", (now_iso(), reg["id"]))  # noqa: S608
    audit(conn, ev["guild_id"], actor_id, f"{kind}.check_in", f"{kind}:{event_id}", f"team:{team_id}")


def set_checked_in(conn: sqlite3.Connection, kind: str, reg_id: int, flag: bool, actor_id: int) -> None:
    k = _k(kind)
    reg = get_reg(conn, kind, reg_id)
    ev = get_event(conn, kind, reg[k["fk"]])
    conn.execute(f"UPDATE {k['reg']} SET checked_in=?, updated_at=? WHERE id=?", (1 if flag else 0, now_iso(), reg_id))  # noqa: S608
    audit(conn, ev["guild_id"], actor_id, f"{kind}.force_check_in", f"{kind}:{ev['id']}", f"team:{reg['team_id']}={flag}")


def list_regs(conn: sqlite3.Connection, kind: str, event_id: int, statuses: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
    k = _k(kind)
    sql = (f"SELECT r.*, t.name, t.tag, t.captain_id, t.verified FROM {k['reg']} r JOIN teams t ON t.id=r.team_id "  # noqa: S608
           f"WHERE r.{k['fk']}=?")
    params: list[Any] = [event_id]
    if statuses:
        sql += f" AND r.status IN ({','.join('?' * len(statuses))})"
        params += list(statuses)
    return conn.execute(sql + " ORDER BY r.id", params).fetchall()


def player_ids(conn: sqlite3.Connection, kind: str, reg_id: int) -> list[int]:
    k = _k(kind)
    return [r["user_id"] for r in conn.execute(f"SELECT user_id FROM {k['players']} WHERE reg_id=?", (reg_id,))]  # noqa: S608


def team_registrations(conn: sqlite3.Connection, kind: str, team_id: int) -> list[dict[str, Any]]:
    k = _k(kind)
    return conn.execute(
        f"SELECT r.*, e.title AS title, e.status AS event_status, e.start_time AS start_time, e.id AS event_id "  # noqa: S608
        f"FROM {k['reg']} r JOIN {k['event']} e ON e.id=r.{k['fk']} WHERE r.team_id=? ORDER BY e.start_time DESC", (team_id,)).fetchall()

# --------------------------------------------------------------------------------------------------
# services/scrim_service.py
# Scrim sessions: lobbies, assignment, no-shows and lifecycle.
# --------------------------------------------------------------------------------------------------

SCRIM_EDITABLE = {"title", "game", "mode", "team_size", "tier", "map", "fee", "start_time", "lobby_count", "lobby_capacity",
            "score_rule_id", "channel_id", "message_id", "reminded"}
TIERS = {"sq": "SQ", "t3": "T3", "t2q": "T2Q", "t2": "T2", "custom": "Custom"}


def scrim_get(conn: sqlite3.Connection, sid: int) -> dict[str, Any]:
    s = conn.execute("SELECT * FROM scrims WHERE id=?", (sid,)).fetchone()
    if not s:
        raise ServiceError("That scrim no longer exists.")
    return s


def scrim_create_draft(conn: sqlite3.Connection, guild_id: int, organizer_id: int, *, title: str, game: str, mode: str,
                 tier: str, map_name: str | None, start_time: str, fee: str | None, lobby_count: int,
                 lobby_capacity: int, team_size: int | None) -> int:
    if mode not in MODE_SIZE:
        raise ServiceError("Mode must be solo, duo or squad.")
    size = team_size or MODE_SIZE[mode]
    _check(lobby_count=lobby_count, lobby_capacity=lobby_capacity, team_size=size)
    now = now_iso()
    cur = conn.execute(
        "INSERT INTO scrims(guild_id,title,game,mode,team_size,tier,map,fee,start_time,lobby_count,lobby_capacity,organizer_id,season,created_at,updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (guild_id, title, game, mode, size, tier, map_name, fee, start_time, lobby_count, lobby_capacity,
         organizer_id, get_setting(conn, guild_id, "season"), now, now))
    sid = int(cur.lastrowid)
    ensure_lobbies(conn, sid)
    audit(conn, guild_id, organizer_id, "scrim.create", f"scrim:{sid}", title)
    return sid


def _check(*, lobby_count: int, lobby_capacity: int, team_size: int) -> None:
    if not 1 <= lobby_count <= 20:
        raise ServiceError("Lobby count must be between 1 and 20.")
    if not 2 <= lobby_capacity <= 100:
        raise ServiceError("Lobby capacity must be between 2 and 100 teams.")
    if not 1 <= team_size <= 10:
        raise ServiceError("Team size must be between 1 and 10.")


def ensure_lobbies(conn: sqlite3.Connection, sid: int) -> None:
    s = scrim_get(conn, sid)
    existing = {r["number"]: r for r in conn.execute("SELECT * FROM lobbies WHERE scrim_id=?", (sid,))}
    for n in range(1, s["lobby_count"] + 1):
        if n not in existing:
            conn.execute("INSERT INTO lobbies(scrim_id,number,capacity,start_time) VALUES (?,?,?,?)",
                         (sid, n, s["lobby_capacity"], s["start_time"]))
        else:
            conn.execute("UPDATE lobbies SET capacity=? WHERE id=?", (s["lobby_capacity"], existing[n]["id"]))
    for n, lob in existing.items():
        if n > s["lobby_count"]:
            if conn.execute("SELECT 1 FROM scrim_registrations WHERE lobby_id=?", (lob["id"],)).fetchone():
                raise ServiceError("Cannot remove lobbies that already have teams assigned. Move the teams first.")
            conn.execute("DELETE FROM lobbies WHERE id=?", (lob["id"],))


def scrim_update(conn: sqlite3.Connection, sid: int, actor_id: int, **fields: Any) -> dict[str, Any]:
    s = scrim_get(conn, sid)
    if s["status"] in LOCKED:
        raise ServiceError(f"A {s['status'].lower()} scrim cannot be edited.")
    bad = set(fields) - SCRIM_EDITABLE
    if bad:
        raise ServiceError("Those fields cannot be edited.")
    if "mode" in fields:
        if fields["mode"] not in MODE_SIZE:
            raise ServiceError("Mode must be solo, duo or squad.")
        fields.setdefault("team_size", MODE_SIZE[fields["mode"]])
    merged = {**s, **fields}
    _check(lobby_count=merged["lobby_count"], lobby_capacity=merged["lobby_capacity"], team_size=merged["team_size"])
    if "start_time" in fields and fields["start_time"] != s["start_time"]:
        fields.setdefault("reminded", 0)
    if fields:
        sets = ", ".join(f"{k}=?" for k in fields)
        conn.execute(f"UPDATE scrims SET {sets}, updated_at=? WHERE id=?", (*fields.values(), now_iso(), sid))  # noqa: S608
        ensure_lobbies(conn, sid)
        audit(conn, s["guild_id"], actor_id, "scrim.update", f"scrim:{sid}", ",".join(fields))
    return scrim_get(conn, sid)


def scrim_set_status(conn: sqlite3.Connection, sid: int, actor_id: int, status: str) -> None:
    s = scrim_get(conn, sid)
    allowed = {"PUBLISHED": ("DRAFT", "PAUSED"), "PAUSED": ("PUBLISHED",), "CANCELLED": ("DRAFT", "PUBLISHED", "PAUSED"),
               "COMPLETED": ("PUBLISHED", "PAUSED")}
    if status not in allowed:
        raise ServiceError("Invalid status.")
    if s["status"] not in allowed[status]:
        raise ServiceError(f"A {s['status'].lower()} scrim cannot become {status.lower()}.")
    if status == "PUBLISHED":
        if not s["start_time"]:
            raise ServiceError("Set a start time first.")
        ensure_lobbies(conn, sid)
    conn.execute("UPDATE scrims SET status=?, updated_at=? WHERE id=?", (status, now_iso(), sid))
    audit(conn, s["guild_id"], actor_id, f"scrim.{status.lower()}", f"scrim:{sid}")


def scrim_delete(conn: sqlite3.Connection, sid: int, actor_id: int) -> None:
    s = scrim_get(conn, sid)
    conn.execute("DELETE FROM matches WHERE kind='scrim' AND event_id=?", (sid,))
    conn.execute("DELETE FROM standings WHERE kind='scrim' AND event_id=?", (sid,))
    conn.execute("DELETE FROM referee_assignments WHERE kind='scrim' AND event_id=?", (sid,))
    conn.execute("DELETE FROM scrims WHERE id=?", (sid,))  # cascades lobbies + registrations
    audit(conn, s["guild_id"], actor_id, "scrim.delete", f"scrim:{sid}", s["title"])


def scrim_list_for_guild(conn: sqlite3.Connection, guild_id: int, statuses: tuple[str, ...] | None = None,
                   organizer_id: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
    sql = "SELECT * FROM scrims WHERE guild_id=?"
    params: list[Any] = [guild_id]
    if statuses:
        sql += f" AND status IN ({','.join('?' * len(statuses))})"
        params += list(statuses)
    if organizer_id:
        sql += " AND organizer_id=?"
        params.append(organizer_id)
    sql += " ORDER BY COALESCE(start_time,'9999') DESC, id DESC LIMIT ?"
    params.append(limit)
    return conn.execute(sql, params).fetchall()


def scrim_lobbies(conn: sqlite3.Connection, sid: int) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM lobbies WHERE scrim_id=? ORDER BY number", (sid,)).fetchall()
    for r in rows:
        r["assigned"] = conn.execute("SELECT COUNT(*) AS c FROM scrim_registrations WHERE lobby_id=? AND status='APPROVED'", (r["id"],)).fetchone()["c"]
    return rows


def assign_lobbies(conn: sqlite3.Connection, sid: int, actor_id: int) -> dict[str, int]:
    """Place approved, unassigned teams into lobbies with free slots (registration order) and make sure
    every lobby has a Match 1 to report results against."""
    s = scrim_get(conn, sid)
    ensure_lobbies(conn, sid)
    lobs = scrim_lobbies(conn, sid)
    teams = conn.execute("SELECT id, team_id FROM scrim_registrations WHERE scrim_id=? AND status='APPROVED' AND lobby_id IS NULL ORDER BY id", (sid,)).fetchall()
    placed = 0
    for reg in teams:
        target = next((l for l in lobs if l["assigned"] < l["capacity"]), None)
        if target is None:
            break
        conn.execute("UPDATE scrim_registrations SET lobby_id=?, updated_at=? WHERE id=?", (target["id"], now_iso(), reg["id"]))
        target["assigned"] += 1
        placed += 1
    created = 0
    for lob in lobs:
        if lob["assigned"] and not conn.execute("SELECT 1 FROM matches WHERE kind='scrim' AND event_id=? AND lobby_id=?", (sid, lob["id"])).fetchone():
            add_match(conn, guild_id=s["guild_id"], kind="scrim", event_id=sid, actor_id=actor_id,
                                    map_name=s["map"], scheduled_at=lob["start_time"] or s["start_time"], lobby_id=lob["id"])
            created += 1
    left = conn.execute("SELECT COUNT(*) AS c FROM scrim_registrations WHERE scrim_id=? AND status='APPROVED' AND lobby_id IS NULL", (sid,)).fetchone()["c"]
    audit(conn, s["guild_id"], actor_id, "scrim.assign_lobbies", f"scrim:{sid}", f"placed={placed} unplaced={left}")
    return {"placed": placed, "unplaced": left, "matches_created": created}


def move_team(conn: sqlite3.Connection, sid: int, team_id: int, lobby_number: int, actor_id: int) -> None:
    s = scrim_get(conn, sid)
    lob = conn.execute("SELECT * FROM lobbies WHERE scrim_id=? AND number=?", (sid, lobby_number)).fetchone()
    if not lob:
        raise ServiceError("That lobby does not exist.")
    reg = conn.execute("SELECT * FROM scrim_registrations WHERE scrim_id=? AND team_id=? AND status='APPROVED'", (sid, team_id)).fetchone()
    if not reg:
        raise ServiceError("That team is not approved for this scrim.")
    full = conn.execute("SELECT COUNT(*) AS c FROM scrim_registrations WHERE lobby_id=? AND status='APPROVED' AND id<>?", (lob["id"], reg["id"])).fetchone()["c"]
    if full >= lob["capacity"]:
        raise ServiceError("That lobby is full.")
    if conn.execute("SELECT 1 FROM match_results r JOIN matches m ON m.id=r.match_id WHERE r.team_id=? AND m.kind='scrim' AND m.event_id=?", (team_id, sid)).fetchone():
        raise ServiceError("This team already has results - it cannot change lobby.")
    conn.execute("UPDATE scrim_registrations SET lobby_id=?, updated_at=? WHERE id=?", (lob["id"], now_iso(), reg["id"]))
    if not conn.execute("SELECT 1 FROM matches WHERE kind='scrim' AND event_id=? AND lobby_id=?", (sid, lob["id"])).fetchone():
        add_match(conn, guild_id=s["guild_id"], kind="scrim", event_id=sid, actor_id=actor_id,
                                map_name=s["map"], scheduled_at=lob["start_time"] or s["start_time"], lobby_id=lob["id"])
    audit(conn, s["guild_id"], actor_id, "scrim.move_team", f"scrim:{sid}", f"team:{team_id} -> lobby {lobby_number}")


def add_round(conn: sqlite3.Connection, sid: int, actor_id: int) -> int:
    """Add the next match number to every lobby that has teams."""
    s = scrim_get(conn, sid)
    n = 0
    for lob in scrim_lobbies(conn, sid):
        if lob["assigned"]:
            add_match(conn, guild_id=s["guild_id"], kind="scrim", event_id=sid, actor_id=actor_id,
                                    map_name=s["map"], lobby_id=lob["id"])
            n += 1
    if not n:
        raise ServiceError("Assign teams to lobbies first.")
    return n


def mark_no_show(conn: sqlite3.Connection, reg_id: int, flag: bool, actor_id: int) -> None:
    reg = conn.execute("SELECT * FROM scrim_registrations WHERE id=?", (reg_id,)).fetchone()
    if not reg:
        raise ServiceError("Registration not found.")
    s = scrim_get(conn, reg["scrim_id"])
    conn.execute("UPDATE scrim_registrations SET no_show=?, checked_in=CASE WHEN ?=1 THEN 0 ELSE checked_in END, updated_at=? WHERE id=?",
                 (1 if flag else 0, 1 if flag else 0, now_iso(), reg_id))
    audit(conn, s["guild_id"], actor_id, "scrim.no_show", f"scrim:{s['id']}", f"team:{reg['team_id']}={flag}")


def scrim_due_reminders(conn: sqlite3.Connection, now: str, horizon: str) -> list[dict[str, Any]]:
    return conn.execute(
        "SELECT * FROM scrims WHERE status='PUBLISHED' AND reminded=0 AND start_time IS NOT NULL AND start_time>? AND start_time<=?",
        (now, horizon)).fetchall()

# --------------------------------------------------------------------------------------------------
# services/report_service.py
# Reports, disputes, appeals and incident cases. Reports are private: reporters see only their own.
# --------------------------------------------------------------------------------------------------

REPORT_KINDS = {"player": "Player report", "team": "Team report", "dispute": "Match dispute", "appeal": "Result appeal",
         "registration": "Registration complaint", "incident": "Incident", "other": "Other / help"}
OPEN_STATES = ("OPEN", "IN_REVIEW", "NEED_EVIDENCE", "REOPENED")


def create_report(conn: sqlite3.Connection, guild_id: int, reporter_id: int, kind: str, target: str | None, description: str,
                  evidence: list[str] | None = None, result_id: int | None = None) -> int:
    if kind not in REPORT_KINDS:
        raise ServiceError("Unknown report type.")
    if not description.strip():
        raise ServiceError("Please describe the problem.")
    if conn.execute("SELECT COUNT(*) AS c FROM reports WHERE guild_id=? AND reporter_id=? AND status IN ('OPEN','IN_REVIEW','NEED_EVIDENCE','REOPENED')",
                    (guild_id, reporter_id)).fetchone()["c"] >= 5:
        raise ServiceError("You already have 5 open cases. Wait for staff to resolve some before filing more.")
    if result_id is not None:
        row = conn.execute("SELECT r.id, m.guild_id FROM match_results r JOIN matches m ON m.id=r.match_id WHERE r.id=?", (result_id,)).fetchone()
        if not row or row["guild_id"] != guild_id:
            raise ServiceError("That result does not exist.")
    now = now_iso()
    cur = conn.execute(
        "INSERT INTO reports(guild_id,kind,reporter_id,target,description,evidence,result_id,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
        (guild_id, kind, reporter_id, target, description, json.dumps((evidence or [])[:10]), result_id, now, now))
    rid = int(cur.lastrowid)
    audit(conn, guild_id, reporter_id, f"report.create", f"report:{rid}", kind)
    if kind in ("dispute", "appeal") and result_id is not None:
        # Freeze the contested result so its points stop counting until staff decide.
        row = conn.execute("SELECT status FROM match_results WHERE id=?", (result_id,)).fetchone()
        if row and row["status"] == "APPROVED":
            set_result_status(conn, result_id=result_id, status="DISPUTED", actor_id=reporter_id, reason=f"Disputed in case #{rid}")
    return rid


def report_get(conn: sqlite3.Connection, rid: int) -> dict[str, Any]:
    r = conn.execute("SELECT * FROM reports WHERE id=?", (rid,)).fetchone()
    if not r:
        raise ServiceError("Case not found.")
    return r


def report_notes(conn: sqlite3.Connection, rid: int) -> list[dict[str, Any]]:
    return conn.execute("SELECT * FROM report_notes WHERE report_id=? ORDER BY id", (rid,)).fetchall()


def list_for_user(conn: sqlite3.Connection, guild_id: int, user_id: int, limit: int = 25) -> list[dict[str, Any]]:
    return conn.execute("SELECT * FROM reports WHERE guild_id=? AND reporter_id=? ORDER BY id DESC LIMIT ?", (guild_id, user_id, limit)).fetchall()


def list_cases(conn: sqlite3.Connection, guild_id: int, statuses: tuple[str, ...] | None = OPEN_STATES, limit: int = 100) -> list[dict[str, Any]]:
    sql = "SELECT * FROM reports WHERE guild_id=?"
    params: list[Any] = [guild_id]
    if statuses:
        sql += f" AND status IN ({','.join('?' * len(statuses))})"
        params += list(statuses)
    return conn.execute(sql + " ORDER BY id DESC LIMIT ?", (*params, limit)).fetchall()


def add_note(conn: sqlite3.Connection, rid: int, author_id: int, note: str, internal: bool = True) -> None:
    r = report_get(conn, rid)
    if not note.strip():
        raise ServiceError("Note cannot be empty.")
    conn.execute("INSERT INTO report_notes(report_id,author_id,note,internal,created_at) VALUES (?,?,?,?,?)",
                 (rid, author_id, note.strip()[:900], 1 if internal else 0, now_iso()))
    conn.execute("UPDATE reports SET updated_at=? WHERE id=?", (now_iso(), rid))
    audit(conn, r["guild_id"], author_id, "report.note", f"report:{rid}", "internal" if internal else "public")


def report_add_evidence(conn: sqlite3.Connection, rid: int, user_id: int, urls: list[str], *, staff: bool) -> None:
    r = report_get(conn, rid)
    if not staff and r["reporter_id"] != user_id:
        raise ServiceError("Only the reporter can add evidence to this case.")
    merged = list(dict.fromkeys(json.loads(r["evidence"] or "[]") + urls))[:10]
    status = "IN_REVIEW" if (r["status"] == "NEED_EVIDENCE" and not staff) else r["status"]
    conn.execute("UPDATE reports SET evidence=?, status=?, updated_at=? WHERE id=?", (json.dumps(merged), status, now_iso(), rid))
    audit(conn, r["guild_id"], user_id, "report.evidence", f"report:{rid}", f"+{len(urls)}")


def assign(conn: sqlite3.Connection, rid: int, staff_id: int, actor_id: int) -> None:
    r = report_get(conn, rid)
    if r["status"] == "RESOLVED":
        raise ServiceError("Reopen the case before assigning it.")
    conn.execute("UPDATE reports SET assigned_to=?, status=CASE WHEN status IN ('OPEN','REOPENED') THEN 'IN_REVIEW' ELSE status END, updated_at=? WHERE id=?",
                 (staff_id, now_iso(), rid))
    audit(conn, r["guild_id"], actor_id, "report.assign", f"report:{rid}", f"user:{staff_id}")


def request_evidence(conn: sqlite3.Connection, rid: int, actor_id: int) -> None:
    r = report_get(conn, rid)
    if r["status"] == "RESOLVED":
        raise ServiceError("This case is already resolved.")
    conn.execute("UPDATE reports SET status='NEED_EVIDENCE', updated_at=? WHERE id=?", (now_iso(), rid))
    audit(conn, r["guild_id"], actor_id, "report.request_evidence", f"report:{rid}")


def resolve(conn: sqlite3.Connection, rid: int, actor_id: int, resolution: str, *, result_action: str | None = None) -> dict[str, Any]:
    """result_action for dispute/appeal cases with a linked result: 'uphold' (re-approve) or 'reject' (void the result)."""
    r = report_get(conn, rid)
    if r["status"] == "RESOLVED":
        raise ServiceError("This case is already resolved.")
    if not resolution.strip():
        raise ServiceError("A resolution reason is required.")
    if r["result_id"]:
        cur = conn.execute("SELECT status FROM match_results WHERE id=?", (r["result_id"],)).fetchone()
        if cur and cur["status"] == "DISPUTED":
            if result_action not in ("uphold", "reject"):
                raise ServiceError("This case froze a match result. Choose whether to uphold or void that result when resolving.")
            set_result_status(conn, result_id=r["result_id"], status="APPROVED" if result_action == "uphold" else "REJECTED",
                                              actor_id=actor_id, reason=f"Case #{rid}: {resolution}"[:300])
    conn.execute("UPDATE reports SET status='RESOLVED', resolution=?, resolved_at=?, updated_at=?, assigned_to=COALESCE(assigned_to,?) WHERE id=?",
                 (resolution.strip()[:900], now_iso(), now_iso(), actor_id, rid))
    audit(conn, r["guild_id"], actor_id, "report.resolve", f"report:{rid}", resolution)
    return report_get(conn, rid)


def reopen(conn: sqlite3.Connection, rid: int, actor_id: int) -> None:
    r = report_get(conn, rid)
    if r["status"] != "RESOLVED":
        raise ServiceError("Only resolved cases can be reopened.")
    conn.execute("UPDATE reports SET status='REOPENED', resolved_at=NULL, updated_at=? WHERE id=?", (now_iso(), rid))
    audit(conn, r["guild_id"], actor_id, "report.reopen", f"report:{rid}")


def report_counts(conn: sqlite3.Connection, guild_id: int) -> dict[str, int]:
    return {r["status"]: r["c"] for r in conn.execute("SELECT status, COUNT(*) AS c FROM reports WHERE guild_id=? GROUP BY status", (guild_id,))}

# --------------------------------------------------------------------------------------------------
# services/community_service.py
# Announcements, recruitment, polls, giveaways, badges and hall of fame.
# --------------------------------------------------------------------------------------------------

# ------------------------------------------------------------------ recruitment
def post_team_recruitment(conn: sqlite3.Connection, guild_id: int, user_id: int, game: str | None, role: str | None, region: str | None, body: str) -> int:
    team = team_of_user(conn, guild_id, user_id)
    if not team or team["my_role"] not in ("captain", "cocaptain"):
        raise ServiceError("Only a team captain or co-captain can post team recruitment.")
    return _post(conn, guild_id, "team", user_id, team["id"], game, role, region, body)


def post_player_recruitment(conn: sqlite3.Connection, guild_id: int, user_id: int, game: str | None, role: str | None, region: str | None, body: str) -> int:
    if not player_get(conn, guild_id, user_id):
        raise ServiceError("Create your player profile before posting.")
    return _post(conn, guild_id, "player", user_id, None, game, role, region, body)


def _post(conn, guild_id, kind, user_id, team_id, game, role, region, body) -> int:
    if not body.strip():
        raise ServiceError("Write a short description.")
    if conn.execute("SELECT COUNT(*) AS c FROM recruitment_posts WHERE guild_id=? AND user_id=? AND kind=? AND active=1", (guild_id, user_id, kind)).fetchone()["c"] >= 2:
        raise ServiceError("You already have 2 active posts. Close one first.")
    cur = conn.execute("INSERT INTO recruitment_posts(guild_id,kind,user_id,team_id,game,role,region,body,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                       (guild_id, kind, user_id, team_id, game, role, region, body.strip()[:600], now_iso()))
    audit(conn, guild_id, user_id, "recruit.post", f"post:{cur.lastrowid}", kind)
    return int(cur.lastrowid)


def list_posts(conn: sqlite3.Connection, guild_id: int, kind: str | None = None, limit: int = 60) -> list[dict[str, Any]]:
    sql = ("SELECT p.*, t.name AS team_name FROM recruitment_posts p LEFT JOIN teams t ON t.id=p.team_id "
           "WHERE p.guild_id=? AND p.active=1 AND (p.team_id IS NULL OR t.disbanded=0)")
    params: list[Any] = [guild_id]
    if kind:
        sql += " AND p.kind=?"
        params.append(kind)
    return conn.execute(sql + " ORDER BY p.id DESC LIMIT ?", (*params, limit)).fetchall()


def close_post(conn: sqlite3.Connection, post_id: int, actor_id: int, *, staff: bool) -> None:
    p = conn.execute("SELECT * FROM recruitment_posts WHERE id=?", (post_id,)).fetchone()
    if not p or not p["active"]:
        raise ServiceError("That post is already closed.")
    if p["user_id"] != actor_id and not staff:
        raise ServiceError("You can only close your own posts.")
    conn.execute("UPDATE recruitment_posts SET active=0 WHERE id=?", (post_id,))
    audit(conn, p["guild_id"], actor_id, "recruit.close", f"post:{post_id}")


# ------------------------------------------------------------------ polls
def create_poll(conn: sqlite3.Connection, guild_id: int, creator_id: int, question: str, options: list[str]) -> int:
    options = [o.strip() for o in options if o.strip()]
    if not 2 <= len(options) <= 5:
        raise ServiceError("A poll needs 2 to 5 options.")
    if len({o.casefold() for o in options}) != len(options):
        raise ServiceError("Poll options must be different.")
    cur = conn.execute("INSERT INTO polls(guild_id,question,options,creator_id,created_at) VALUES (?,?,?,?,?)",
                       (guild_id, question.strip()[:250], json.dumps([o[:80] for o in options]), creator_id, now_iso()))
    audit(conn, guild_id, creator_id, "poll.create", f"poll:{cur.lastrowid}")
    return int(cur.lastrowid)


def vote(conn: sqlite3.Connection, poll_id: int, user_id: int, idx: int) -> dict[str, Any]:
    p = conn.execute("SELECT * FROM polls WHERE id=?", (poll_id,)).fetchone()
    if not p:
        raise ServiceError("That poll no longer exists.")
    if p["closed"]:
        raise ServiceError("This poll is closed.")
    if not 0 <= idx < len(json.loads(p["options"])):
        raise ServiceError("Invalid option.")
    conn.execute("INSERT INTO poll_votes(poll_id,user_id,option_idx) VALUES (?,?,?) ON CONFLICT(poll_id,user_id) DO UPDATE SET option_idx=excluded.option_idx",
                 (poll_id, user_id, idx))
    return tally(conn, poll_id)


def tally(conn: sqlite3.Connection, poll_id: int) -> dict[str, Any]:
    p = conn.execute("SELECT * FROM polls WHERE id=?", (poll_id,)).fetchone()
    if not p:
        raise ServiceError("That poll no longer exists.")
    opts = json.loads(p["options"])
    counts = [0] * len(opts)
    for r in conn.execute("SELECT option_idx, COUNT(*) AS c FROM poll_votes WHERE poll_id=? GROUP BY option_idx", (poll_id,)):
        if 0 <= r["option_idx"] < len(counts):
            counts[r["option_idx"]] = r["c"]
    return {"poll": p, "options": opts, "counts": counts, "total": sum(counts)}


def close_poll(conn: sqlite3.Connection, poll_id: int, actor_id: int, *, staff: bool) -> None:
    p = conn.execute("SELECT * FROM polls WHERE id=?", (poll_id,)).fetchone()
    if not p:
        raise ServiceError("Poll not found.")
    if p["creator_id"] != actor_id and not staff:
        raise ServiceError("Only the poll creator or staff can close it.")
    conn.execute("UPDATE polls SET closed=1 WHERE id=?", (poll_id,))
    audit(conn, p["guild_id"], actor_id, "poll.close", f"poll:{poll_id}")


# ------------------------------------------------------------------ giveaways
def create_giveaway(conn: sqlite3.Connection, guild_id: int, host_id: int, prize: str, ends_at: str, winners: int, require_team: bool, require_profile: bool) -> int:
    if not prize.strip():
        raise ServiceError("Prize is required.")
    if not 1 <= winners <= 20:
        raise ServiceError("Winners must be between 1 and 20.")
    cur = conn.execute("INSERT INTO giveaways(guild_id,prize,ends_at,winners,require_team,require_profile,host_id,created_at) VALUES (?,?,?,?,?,?,?,?)",
                       (guild_id, prize.strip()[:200], ends_at, winners, int(require_team), int(require_profile), host_id, now_iso()))
    audit(conn, guild_id, host_id, "giveaway.create", f"giveaway:{cur.lastrowid}", prize)
    return int(cur.lastrowid)


def eligible(conn: sqlite3.Connection, g: dict[str, Any], user_id: int) -> str | None:
    """Return None if eligible, else the reason."""
    if g["require_profile"] and not player_get(conn, g["guild_id"], user_id):
        return "You need a player profile to enter this giveaway."
    if g["require_team"] and not team_of_user(conn, g["guild_id"], user_id):
        return "You need to be in a team to enter this giveaway."
    return None


def enter_giveaway(conn: sqlite3.Connection, giveaway_id: int, user_id: int, now: str | None = None) -> int:
    g = conn.execute("SELECT * FROM giveaways WHERE id=?", (giveaway_id,)).fetchone()
    if not g:
        raise ServiceError("That giveaway no longer exists.")
    if g["ended"] or g["ends_at"] <= (now or now_iso()):
        raise ServiceError("This giveaway has ended.")
    why = eligible(conn, g, user_id)
    if why:
        raise ServiceError(why)
    if conn.execute("SELECT 1 FROM giveaway_entries WHERE giveaway_id=? AND user_id=?", (giveaway_id, user_id)).fetchone():
        raise ServiceError("You are already entered.")
    conn.execute("INSERT INTO giveaway_entries(giveaway_id,user_id) VALUES (?,?)", (giveaway_id, user_id))
    return conn.execute("SELECT COUNT(*) AS c FROM giveaway_entries WHERE giveaway_id=?", (giveaway_id,)).fetchone()["c"]


def end_giveaway(conn: sqlite3.Connection, giveaway_id: int, rng: random.Random | None = None) -> list[int]:
    """Draw winners among entrants who are STILL eligible. Idempotent: an ended giveaway returns its stored winners."""
    g = conn.execute("SELECT * FROM giveaways WHERE id=?", (giveaway_id,)).fetchone()
    if not g:
        raise ServiceError("Giveaway not found.")
    if g["ended"]:
        return json.loads(g["winner_ids"] or "[]")
    pool = [r["user_id"] for r in conn.execute("SELECT user_id FROM giveaway_entries WHERE giveaway_id=? ORDER BY user_id", (giveaway_id,))
            if eligible(conn, g, r["user_id"]) is None]
    winners = (rng or random.SystemRandom()).sample(pool, min(g["winners"], len(pool))) if pool else []
    conn.execute("UPDATE giveaways SET ended=1, winner_ids=? WHERE id=?", (json.dumps(winners), giveaway_id))
    audit(conn, g["guild_id"], g["host_id"], "giveaway.end", f"giveaway:{giveaway_id}", f"winners={len(winners)}")
    return winners


def due_giveaways(conn: sqlite3.Connection, now: str) -> list[dict[str, Any]]:
    return conn.execute("SELECT * FROM giveaways WHERE ended=0 AND ends_at<=?", (now,)).fetchall()


def giveaway_entries(conn: sqlite3.Connection, giveaway_id: int) -> int:
    return conn.execute("SELECT COUNT(*) AS c FROM giveaway_entries WHERE giveaway_id=?", (giveaway_id,)).fetchone()["c"]


# ------------------------------------------------------------------ badges / hall of fame / announcements
def award_badge(conn: sqlite3.Connection, guild_id: int, actor_id: int, badge: str, *, user_id: int | None = None, team_id: int | None = None, note: str | None = None) -> None:
    if (user_id is None) == (team_id is None):
        raise ServiceError("Pick either a player or a team.")
    if not badge.strip():
        raise ServiceError("Badge name is required.")
    conn.execute("INSERT INTO badges(guild_id,user_id,team_id,badge,note,awarded_by,created_at) VALUES (?,?,?,?,?,?,?)",
                 (guild_id, user_id, team_id, badge.strip()[:40], note, actor_id, now_iso()))
    audit(conn, guild_id, actor_id, "badge.award", f"user:{user_id}" if user_id else f"team:{team_id}", badge)


def add_hall_of_fame(conn: sqlite3.Connection, guild_id: int, actor_id: int, title: str, winner: str, season: str | None, note: str | None) -> int:
    if not title.strip() or not winner.strip():
        raise ServiceError("Title and winner are required.")
    cur = conn.execute("INSERT INTO hall_of_fame(guild_id,season,title,winner,note,created_by,created_at) VALUES (?,?,?,?,?,?,?)",
                       (guild_id, season, title.strip()[:100], winner.strip()[:150], note, actor_id, now_iso()))
    audit(conn, guild_id, actor_id, "hof.add", f"hof:{cur.lastrowid}", title)
    return int(cur.lastrowid)


def record_announcement(conn: sqlite3.Connection, guild_id: int, author_id: int, kind: str, title: str, body: str, channel_id: int | None, message_id: int | None) -> int:
    cur = conn.execute("INSERT INTO announcements(guild_id,kind,title,body,channel_id,message_id,author_id,created_at) VALUES (?,?,?,?,?,?,?,?)",
                       (guild_id, kind, title, body, channel_id, message_id, author_id, now_iso()))
    audit(conn, guild_id, author_id, "announce.post", f"announcement:{cur.lastrowid}", title)
    return int(cur.lastrowid)


def calendar(conn: sqlite3.Connection, guild_id: int, now: str, limit: int = 40) -> list[dict[str, Any]]:
    return conn.execute(
        "SELECT 'Tournament' AS kind, id, title, game, start_time FROM tournaments WHERE guild_id=? AND status IN ('PUBLISHED','LIVE') AND start_time>=? "
        "UNION ALL SELECT 'Scrim', id, title, game, start_time FROM scrims WHERE guild_id=? AND status='PUBLISHED' AND start_time>=? "
        "ORDER BY start_time LIMIT ?", (guild_id, now, guild_id, now, limit)).fetchall()

# --------------------------------------------------------------------------------------------------
# services/staff_service.py
# Staff roles, applications, duty status and referee assignments. Only administrators may grant roles.
# --------------------------------------------------------------------------------------------------

ROLES = {"staff": "Staff", "referee": "Referee", "organizer": "Tournament Organizer", "scrim_manager": "Scrim Manager"}


def grant(conn: sqlite3.Connection, guild_id: int, actor: Perms, actor_id: int, user_id: int, role: str) -> None:
    if not can_grant_roles(actor):
        raise ServiceError("Only administrators can grant staff roles.")
    if role not in ROLES:
        raise ServiceError("Unknown role.")
    if user_id == actor_id and not actor.is_owner and not actor.is_admin:
        raise ServiceError("You cannot authorize yourself.")
    conn.execute("INSERT OR IGNORE INTO staff_members(guild_id,user_id,role,granted_by,granted_at) VALUES (?,?,?,?,?)",
                 (guild_id, user_id, role, actor_id, now_iso()))
    audit(conn, guild_id, actor_id, "staff.grant", f"user:{user_id}", role)


def revoke(conn: sqlite3.Connection, guild_id: int, actor: Perms, actor_id: int, user_id: int, role: str | None = None) -> int:
    if not can_grant_roles(actor):
        raise ServiceError("Only administrators can remove staff roles.")
    if role:
        cur = conn.execute("DELETE FROM staff_members WHERE guild_id=? AND user_id=? AND role=?", (guild_id, user_id, role))
    else:
        cur = conn.execute("DELETE FROM staff_members WHERE guild_id=? AND user_id=?", (guild_id, user_id))
        conn.execute("DELETE FROM staff_duty WHERE guild_id=? AND user_id=?", (guild_id, user_id))
    if cur.rowcount == 0:
        raise ServiceError("That member does not hold that role.")
    audit(conn, guild_id, actor_id, "staff.revoke", f"user:{user_id}", role or "all")
    return cur.rowcount


def staff_members(conn: sqlite3.Connection, guild_id: int) -> list[dict[str, Any]]:
    return conn.execute("SELECT * FROM staff_members WHERE guild_id=? ORDER BY user_id, role", (guild_id,)).fetchall()


def roles_of(conn: sqlite3.Connection, guild_id: int, user_id: int) -> list[str]:
    return [r["role"] for r in conn.execute("SELECT role FROM staff_members WHERE guild_id=? AND user_id=?", (guild_id, user_id))]


def apply(conn: sqlite3.Connection, guild_id: int, user_id: int, role: str, reason: str) -> int:
    if role not in ROLES:
        raise ServiceError("Unknown role.")
    if role in roles_of(conn, guild_id, user_id):
        raise ServiceError("You already hold that role.")
    if not reason.strip():
        raise ServiceError("Tell us why you want the role.")
    try:
        cur = conn.execute("INSERT INTO staff_applications(guild_id,user_id,role_wanted,reason,created_at) VALUES (?,?,?,?,?)",
                           (guild_id, user_id, role, reason.strip()[:900], now_iso()))
    except sqlite3.IntegrityError:
        raise ServiceError("You already have a pending application.") from None
    audit(conn, guild_id, user_id, "staff.apply", f"app:{cur.lastrowid}", role)
    return int(cur.lastrowid)


def pending_applications(conn: sqlite3.Connection, guild_id: int) -> list[dict[str, Any]]:
    return conn.execute("SELECT * FROM staff_applications WHERE guild_id=? AND status='PENDING' ORDER BY id", (guild_id,)).fetchall()


def review_application(conn: sqlite3.Connection, app_id: int, actor: Perms, actor_id: int, approve: bool, note: str | None) -> dict[str, Any]:
    if not can_grant_roles(actor):
        raise ServiceError("Only administrators can review staff applications.")
    a = conn.execute("SELECT * FROM staff_applications WHERE id=?", (app_id,)).fetchone()
    if not a or a["status"] != "PENDING":
        raise ServiceError("That application is no longer pending.")
    conn.execute("UPDATE staff_applications SET status=?, reviewed_by=?, review_note=?, reviewed_at=? WHERE id=?",
                 ("APPROVED" if approve else "REJECTED", actor_id, note, now_iso(), app_id))
    if approve:
        conn.execute("INSERT OR IGNORE INTO staff_members(guild_id,user_id,role,granted_by,granted_at) VALUES (?,?,?,?,?)",
                     (a["guild_id"], a["user_id"], a["role_wanted"], actor_id, now_iso()))
    audit(conn, a["guild_id"], actor_id, f"staff.application_{'approved' if approve else 'rejected'}", f"app:{app_id}", note)
    return a


def set_duty(conn: sqlite3.Connection, guild_id: int, user_id: int, on: bool) -> None:
    conn.execute("INSERT INTO staff_duty(guild_id,user_id,on_duty,since) VALUES (?,?,?,?) "
                 "ON CONFLICT(guild_id,user_id) DO UPDATE SET on_duty=excluded.on_duty, since=excluded.since", (guild_id, user_id, 1 if on else 0, now_iso()))
    audit(conn, guild_id, user_id, "staff.duty_on" if on else "staff.duty_off")


def duty(conn: sqlite3.Connection, guild_id: int) -> list[dict[str, Any]]:
    return conn.execute("SELECT * FROM staff_duty WHERE guild_id=? AND on_duty=1 ORDER BY since", (guild_id,)).fetchall()


def is_on_duty(conn: sqlite3.Connection, guild_id: int, user_id: int) -> bool:
    r = conn.execute("SELECT on_duty FROM staff_duty WHERE guild_id=? AND user_id=?", (guild_id, user_id)).fetchone()
    return bool(r and r["on_duty"])


def assign_referee(conn: sqlite3.Connection, kind: str, event_id: int, user_id: int, actor: Perms, actor_id: int) -> None:
    table = "tournaments" if kind == "tournament" else "scrims"
    ev = conn.execute(f"SELECT guild_id, organizer_id FROM {table} WHERE id=?", (event_id,)).fetchone()  # noqa: S608
    if not ev:
        raise ServiceError("Event not found.")
    if not can_manage_event(actor, actor_id, ev["organizer_id"], kind):
        raise ServiceError("Only the organizer or an administrator can assign referees.")
    if "referee" not in roles_of(conn, ev["guild_id"], user_id):
        raise ServiceError("That member is not a referee. An administrator must grant the Referee role first.")
    try:
        conn.execute("INSERT INTO referee_assignments(guild_id,kind,event_id,user_id,assigned_by,created_at) VALUES (?,?,?,?,?,?)",
                     (ev["guild_id"], kind, event_id, user_id, actor_id, now_iso()))
    except sqlite3.IntegrityError:
        raise ServiceError("That referee is already assigned.") from None
    audit(conn, ev["guild_id"], actor_id, "staff.assign_referee", f"{kind}:{event_id}", f"user:{user_id}")


def unassign_referee(conn: sqlite3.Connection, kind: str, event_id: int, user_id: int, actor: Perms, actor_id: int) -> None:
    table = "tournaments" if kind == "tournament" else "scrims"
    ev = conn.execute(f"SELECT guild_id, organizer_id FROM {table} WHERE id=?", (event_id,)).fetchone()  # noqa: S608
    if not ev or not can_manage_event(actor, actor_id, ev["organizer_id"], kind):
        raise ServiceError("Only the organizer or an administrator can change referees.")
    conn.execute("DELETE FROM referee_assignments WHERE kind=? AND event_id=? AND user_id=?", (kind, event_id, user_id))
    audit(conn, ev["guild_id"], actor_id, "staff.unassign_referee", f"{kind}:{event_id}", f"user:{user_id}")


def referees_for(conn: sqlite3.Connection, kind: str, event_id: int) -> list[int]:
    return [r["user_id"] for r in conn.execute("SELECT user_id FROM referee_assignments WHERE kind=? AND event_id=? ORDER BY id", (kind, event_id))]


def activity(conn: sqlite3.Connection, guild_id: int, limit: int = 100) -> list[dict[str, Any]]:
    return conn.execute(
        "SELECT a.* FROM audit_logs a WHERE a.guild_id=? AND a.actor_id IN (SELECT user_id FROM staff_members WHERE guild_id=?) ORDER BY a.id DESC LIMIT ?",
        (guild_id, guild_id, limit)).fetchall()


# ==================================================================================================
# SECTION 7 - EMBEDS AND REUSABLE UI COMPONENTS (BUTTONS, SELECTS, MODALS, VIEWS)
# ==================================================================================================

# --------------------------------------------------------------------------------------------------
# ui/buttons.py
# Small reusable button types.
# --------------------------------------------------------------------------------------------------

Handler = Callable[[discord.Interaction], Awaitable[None]]


class ActionButton(discord.ui.Button):
    """A button that calls an async handler(interaction). Never decorative."""

    def __init__(self, label: str, handler: Handler, *, style: discord.ButtonStyle = discord.ButtonStyle.secondary,
                 emoji: str | None = None, row: int | None = None, disabled: bool = False, custom_id: str | None = None) -> None:
        super().__init__(label=label, style=style, emoji=emoji, row=row, disabled=disabled, custom_id=custom_id)
        self._handler = handler

    async def callback(self, interaction: discord.Interaction) -> None:
        await self._handler(interaction)


PRIMARY, SECONDARY, SUCCESS, DANGER = (discord.ButtonStyle.primary, discord.ButtonStyle.secondary,
                                       discord.ButtonStyle.success, discord.ButtonStyle.danger)

# --------------------------------------------------------------------------------------------------
# ui/embeds.py
# Reusable embed builders. Everything shown comes from database values - nothing is hardcoded live data.
# --------------------------------------------------------------------------------------------------

ELECTRIC = 0x00B4FF
GREEN, RED, AMBER, SILVER = 0x2ECC71, 0xE74C3C, 0xF1C40F, 0xC0C7D0

EVENT_ICON = {"DRAFT": "📝 Draft", "SCHEDULED": "🕒 Scheduled", "PUBLISHED": "🟢 Published", "LIVE": "🔴 Live",
              "COMPLETED": "🏁 Completed", "CANCELLED": "⛔ Cancelled", "PAUSED": "⏸️ Paused"}
REG_STATE = {"OPEN": "🟢 Open", "WAITLIST": "🟡 Waitlist only", "FULL": "🔴 Full", "NOT_OPEN": "⚪ Not open yet",
             "CLOSED": "🔴 Closed", "UNPUBLISHED": "⚫ Not published", "CANCELLED": "⛔ Cancelled", "COMPLETED": "🏁 Finished"}
REG_ICON = {"PENDING": "🟡 Pending", "APPROVED": "🟢 Approved", "REJECTED": "🔴 Rejected", "WAITLIST": "🟠 Waitlist",
            "WITHDRAWN": "⚪ Withdrawn"}
RESULT_ICON = {"PENDING": "🟡 Pending", "APPROVED": "🟢 Approved", "REJECTED": "🔴 Rejected", "DISPUTED": "🟠 Disputed"}
CASE_ICON = {"OPEN": "🟡 Open", "IN_REVIEW": "🔵 In review", "NEED_EVIDENCE": "🟠 Needs evidence",
             "RESOLVED": "🟢 Resolved", "REOPENED": "🟣 Reopened"}


@dataclass(frozen=True)
class Branding:
    name: str = "AEGIS ESPORTS"
    color: int = ELECTRIC
    footer: str = "AEGIS ESPORTS • Universal Competitive Management"
    image: str | None = None

    @classmethod
    def from_settings(cls, s: dict[str, str]) -> "Branding":
        try:
            color = int((s.get("brand_color") or "00B4FF").lstrip("#"), 16)
        except ValueError:
            color = ELECTRIC
        return cls(name=s.get("brand_name") or cls.name, color=color, footer=s.get("brand_footer") or cls.footer,
                   image=s.get("brand_image") or None)


DEFAULT_BRAND = Branding()


def base(b: Branding, section: str | None = None, description: str | None = None, *, title: str | None = None,
         color: int | None = None, timestamp: bool = True) -> discord.Embed:
    head = title or (f"{b.name} | {section}" if section else b.name)
    e = discord.Embed(title=trunc(head, 256), description=trunc(description, 4000) if description else None,
                      color=color if color is not None else b.color)
    if timestamp:
        e.timestamp = utcnow()
    e.set_footer(text=trunc(b.footer, 2000))
    return e


def notice(text: str, kind: str = "info", b: Branding = DEFAULT_BRAND) -> discord.Embed:
    icon, color = {"ok": ("✅", GREEN), "error": ("❌", RED), "warn": ("⚠️", AMBER), "info": ("ℹ️", b.color)}[kind]
    return discord.Embed(description=trunc(f"{icon} {text}", 4000), color=color)


def list_pages(b: Branding, section: str, lines: list[str], *, per_page: int = 10, empty: str = "Nothing here yet.",
               header: str | None = None) -> list[discord.Embed]:
    if not lines:
        return [base(b, section, empty)]
    pages = []
    chunks = [lines[i:i + per_page] for i in range(0, len(lines), per_page)]
    for n, chunk in enumerate(chunks, 1):
        body = "\n".join(chunk)
        text = f"{header}\n\n{body}" if header else body
        e = base(b, section, trunc(text, 4000))
        if len(chunks) > 1:
            e.set_footer(text=f"{trunc(b.footer, 200)} • Page {n}/{len(chunks)}")
        pages.append(e)
    return pages


def home_embed(b: Branding) -> discord.Embed:
    e = base(b, None, "**Universal Competitive Management System**\n"
                      "Tournaments • Scrims • Teams • Results • Rankings\n\n"
                      "Use the buttons below. Everything is private to you unless it is an announcement.")
    e.add_field(name="Get started", value="1️⃣ **Player Profile** - create your profile\n2️⃣ **Create Team** or **Find Team**\n"
                                          "3️⃣ **Tournaments / Scrims** - register your team", inline=False)
    e.add_field(name="Need help?", value="**Help & Support** explains every section. **Report Issue** reaches staff privately.", inline=False)
    if b.image:
        e.set_image(url=b.image)
    return e


def dashboard_embed(b: Branding, title: str, text: str, stats: dict[str, Any] | None = None) -> discord.Embed:
    e = base(b, None, f"**{title}**\n{text}")
    if stats:
        e.add_field(name="Live from database", value="\n".join(f"{k}: **{v}**" for k, v in stats.items()), inline=False)
    return e


def _money(x: str | None) -> str:
    return x.strip() if x and x.strip() else "Free"


def tournament_embed(b: Branding, t: dict[str, Any], c: dict[str, int], state: str, offset: int = 0) -> discord.Embed:
    active = c.get("PENDING", 0) + c.get("APPROVED", 0)
    e = base(b, "TOURNAMENT", f"## {t['title']}")
    e.add_field(name="Game", value=t["game"])
    e.add_field(name="Mode", value=f"{t['mode'].title()} ({t['team_size']} players)")
    e.add_field(name="Map", value=t["map"] or "TBA")
    e.add_field(name="Entry", value=_money(t["entry_fee"]))
    e.add_field(name="Prize", value=t["prize"] or "TBA")
    e.add_field(name="Region", value=t["region"] or "Open")
    e.add_field(name="Registered Teams", value=f"{active}/{t['max_teams']}" + (f" (+{c.get('WAITLIST', 0)} waitlist)" if c.get("WAITLIST") else ""))
    e.add_field(name="Registration", value=REG_STATE.get(state, state))
    e.add_field(name="Start Time", value=ts_both(t["start_time"]))
    if t.get("reg_open") or t.get("reg_close"):
        e.add_field(name="Registration Window", value=f"Opens {ts(t.get('reg_open'))}\nCloses {ts(t.get('reg_close') or t.get('start_time'))}", inline=False)
    e.add_field(name="Status", value=EVENT_ICON.get(t["status"], t["status"]))
    e.add_field(name="Organizer", value=f"<@{t['organizer_id']}>" + (f"\n{trunc(t['organizer_info'], 200)}" if t.get("organizer_info") else ""))
    e.add_field(name="ID", value=f"#{t['id']}")
    if t.get("poster_url"):
        e.set_image(url=t["poster_url"])
    return e


def scrim_embed(b: Branding, s: dict[str, Any], c: dict[str, int], state: str, lobbies: list[dict[str, Any]] | None = None) -> discord.Embed:
    active = c.get("PENDING", 0) + c.get("APPROVED", 0)
    cap = s["lobby_count"] * s["lobby_capacity"]
    e = base(b, "SCRIM", f"## {s['title']}")
    e.add_field(name="Game", value=s["game"])
    e.add_field(name="Tier", value=s["tier"] or "Open")
    e.add_field(name="Mode", value=f"{s['mode'].title()} ({s['team_size']} players)")
    e.add_field(name="Map", value=s["map"] or "TBA")
    e.add_field(name="Entry", value=_money(s["fee"]))
    e.add_field(name="Lobbies", value=f"{s['lobby_count']} × {s['lobby_capacity']} teams")
    e.add_field(name="Registered Teams", value=f"{active}/{cap}" + (f" (+{c.get('WAITLIST', 0)} waitlist)" if c.get("WAITLIST") else ""))
    e.add_field(name="Registration", value=REG_STATE.get(state, state))
    e.add_field(name="Start Time", value=ts_both(s["start_time"]))
    e.add_field(name="Status", value=EVENT_ICON.get(s["status"], s["status"]))
    e.add_field(name="Organizer", value=f"<@{s['organizer_id']}>")
    e.add_field(name="ID", value=f"#{s['id']}")
    if lobbies:
        e.add_field(name="Lobby fill", value="\n".join(f"Lobby {l['number']}: {l['assigned']}/{l['capacity']}" for l in lobbies[:10]), inline=False)
    return e


def team_embed(b: Branding, team: dict[str, Any], members: list[dict[str, Any]], stats: dict[str, Any],
               history: list[dict[str, Any]] | None = None) -> discord.Embed:
    tag = f" [{team['tag']}]" if team.get("tag") else ""
    e = base(b, "TEAM", f"## {team['name']}{tag}" + ("  ✅ Verified" if team["verified"] else ""))
    main = [m for m in members if m["role"] != "sub"]
    subs = [m for m in members if m["role"] == "sub"]
    e.add_field(name=f"Roster ({len(main)})", value="\n".join(f"{ROLE_LABEL[m['role']]}: <@{m['user_id']}>" for m in main) or "—", inline=False)
    if subs:
        e.add_field(name=f"Substitutes ({len(subs)})", value="\n".join(f"<@{m['user_id']}>" for m in subs), inline=False)
    e.add_field(name="Game", value=team.get("game") or "Any")
    e.add_field(name="Recruiting", value="Yes" if team["recruiting"] else "No")
    e.add_field(name="Events entered", value=str(stats["events"]))
    e.add_field(name="Approved results", value=f"{stats['played']} matches • {stats['wins']} wins")
    e.add_field(name="Points / Kills", value=f"{stats['points']:g} / {stats['kills']}")
    e.add_field(name="Scrim no-shows", value=str(stats["no_shows"]))
    if stats.get("badges"):
        e.add_field(name="Badges", value=" ".join(f"🏅 {trunc(x, 30)}" for x in stats["badges"][:10]), inline=False)
    if history:
        e.add_field(name="Recent events", value="\n".join(
            f"{h['kind']}: **{trunc(h['title'], 40)}** - {REG_ICON.get(h['status'], h['status'])}" + (f" • rank {h['rank']}" if h.get("rank") else "")
            for h in history[:6]), inline=False)
    if team.get("logo_url"):
        e.set_thumbnail(url=team["logo_url"])
    e.set_footer(text=f"{trunc(b.footer, 150)} • Team #{team['id']} • stats are from referee-approved results")
    return e


def profile_embed(b: Branding, p: dict[str, Any] | None, user: discord.abc.User | None, team: dict[str, Any] | None,
                  badges: list[str], history_count: int = 0) -> discord.Embed:
    if not p:
        return base(b, "PLAYER PROFILE", "No profile yet. Use **Create Profile**.")
    games = ", ".join(json.loads(p["games"] or "[]")) or "Not set"
    e = base(b, "PLAYER PROFILE", f"## {p['ign']}")
    e.add_field(name="Discord", value=f"<@{p['user_id']}> (`{p['user_id']}`)")
    e.add_field(name="Status", value={"ACTIVE": "🟢 Active", "LFT": "🔎 Looking for team", "INACTIVE": "⚪ Inactive"}[p["status"]])
    e.add_field(name="Team", value=team["name"] if team else "None")
    e.add_field(name="Games", value=games)
    e.add_field(name="Role", value=p["role"] or "—")
    e.add_field(name="Region / Language", value=f"{p['region'] or '—'} / {p['language'] or '—'}")
    e.add_field(name="Game UID", value=f"`{p['game_uid']}`" if p["game_uid"] else "—")
    e.add_field(name="Recorded events", value=str(history_count))
    if p.get("achievements"):
        e.add_field(name="Achievements (self-reported)", value=trunc(p["achievements"], 900), inline=False)
    if badges:
        e.add_field(name="Badges (awarded by staff)", value=" ".join(f"🏅 {trunc(x, 30)}" for x in badges[:10]), inline=False)
    if user is not None:
        e.set_thumbnail(url=user.display_avatar.url)
    e.set_footer(text=f"{trunc(b.footer, 150)} • Achievements are self-reported; statistics come only from approved results")
    return e


def standings_pages(b: Branding, title: str, rows: list[dict[str, Any]], rule_text: str | None = None,
                    per_page: int = 15, kind: str = "br") -> list[discord.Embed]:
    if not rows:
        return [base(b, "STANDINGS", f"**{title}**\nNo approved results yet.")]
    pages = []
    chunks = [rows[i:i + per_page] for i in range(0, len(rows), per_page)]
    for n, chunk in enumerate(chunks, 1):
        head = f"{'#':>2} {'Team':<16} {'Pts':>6} {'K':>4} {'W':>3} {'P':>3}"
        body = "\n".join(f"{r['rank']:>2} {trunc(r['name'], 16):<16} {r['points']:>6g} {r['kills']:>4} {r['wins']:>3} {r['played']:>3}" for r in chunk)
        e = base(b, "STANDINGS", f"**{trunc(title, 200)}**\n```\n{head}\n{body}\n```")
        if rule_text and n == 1:
            e.add_field(name="Scoring rule", value=trunc(rule_text, 900), inline=False)
        e.set_footer(text=f"{trunc(b.footer, 150)} • Approved results only • Page {n}/{len(chunks)}")
        pages.append(e)
    return pages


def result_embed(b: Branding, r: dict[str, Any]) -> discord.Embed:
    e = base(b, "RESULT", f"**{trunc(r.get('event_title') or 'Event', 120)}** • Match {r.get('number', '?')}\nTeam: **{r.get('team_name', '?')}**")
    if r.get("placement") is not None:
        e.add_field(name="Placement", value=f"#{r['placement']}")
        e.add_field(name="Kills", value=str(r["kills"]))
    if r.get("score_for") is not None:
        e.add_field(name="Score", value=f"{r['score_for']} - {r['score_against']}")
    if r.get("bonus"):
        e.add_field(name="Bonus", value=f"{r['bonus']:g}")
    e.add_field(name="Status", value=RESULT_ICON.get(r["status"], r["status"]))
    ev = json.loads(r.get("evidence") or "[]")
    e.add_field(name="Evidence", value="\n".join(f"[{i}]({u})" for i, u in enumerate(ev, 1)) if ev else "None attached", inline=False)
    if r.get("notes"):
        e.add_field(name="Team notes", value=trunc(r["notes"], 500), inline=False)
    if r.get("review_reason"):
        e.add_field(name="Referee note", value=trunc(r["review_reason"], 500), inline=False)
    e.add_field(name="Result ID", value=f"#{r['id']}")
    return e


def case_embed(b: Branding, r: dict[str, Any], notes: list[dict[str, Any]] | None = None, *, staff_view: bool = True) -> discord.Embed:
    e = base(b, f"CASE #{r['id']}", f"**{r['kind'].title()}** • {CASE_ICON.get(r['status'], r['status'])}")
    e.add_field(name="Reporter", value=f"<@{r['reporter_id']}>")
    e.add_field(name="Target", value=trunc(r["target"] or "—", 200))
    e.add_field(name="Assigned", value=f"<@{r['assigned_to']}>" if r["assigned_to"] else "Unassigned")
    e.add_field(name="Description", value=trunc(r["description"], 1000), inline=False)
    ev = json.loads(r.get("evidence") or "[]")
    e.add_field(name="Evidence", value="\n".join(f"[{i}]({u})" for i, u in enumerate(ev[:8], 1)) if ev else "None", inline=False)
    if r.get("result_id"):
        e.add_field(name="Linked result", value=f"#{r['result_id']}")
    if r.get("resolution"):
        e.add_field(name="Resolution", value=trunc(r["resolution"], 800), inline=False)
    if notes:
        shown = [n for n in notes if staff_view or not n["internal"]][-5:]
        if shown:
            e.add_field(name="Notes", value="\n".join(f"{'🔒 ' if n['internal'] else ''}<@{n['author_id']}>: {trunc(n['note'], 180)}" for n in shown), inline=False)
    return e


def event_line(kind: str, ev: dict[str, Any]) -> str:
    return f"**#{ev['id']} {trunc(ev['title'], 50)}** • {ev['game']} • {EVENT_ICON.get(ev['status'], ev['status'])} • {ts(ev['start_time'], 'f')}"


def team_line(t: dict[str, Any]) -> str:
    tag = f"[{t['tag']}] " if t.get("tag") else ""
    return f"{'✅ ' if t['verified'] else ''}**{tag}{trunc(t['name'], 40)}**" + (f" • {t['mains']} main / {t['subs']} subs" if "mains" in t else "")


# Alias for call sites where a local variable named `notice` shadows the helper above.
embed_notice = notice

# --------------------------------------------------------------------------------------------------
# ui/selects.py
# Select-menu wrappers that call an async callback(interaction, values).
# --------------------------------------------------------------------------------------------------

SelectCb = Callable[[discord.Interaction, list[Any]], Awaitable[None]]


class ChoiceSelect(discord.ui.Select):
    def __init__(self, options: list[tuple[str, str, str | None]], callback: SelectCb, *, placeholder: str = "Choose…",
                 min_values: int = 1, max_values: int = 1, row: int | None = None) -> None:
        if not options:
            raise ValueError("ChoiceSelect needs at least one option")
        opts = [discord.SelectOption(label=trunc(label, 100) or "-", value=str(value)[:100],
                                     description=trunc(desc, 100) if desc else None) for label, value, desc in options[:25]]
        super().__init__(placeholder=trunc(placeholder, 150), options=opts, min_values=min_values,
                         max_values=min(max_values, len(opts)), row=row)
        self._cb = callback

    async def callback(self, interaction: discord.Interaction) -> None:
        await self._cb(interaction, list(self.values))


class UserPicker(discord.ui.UserSelect):
    def __init__(self, callback: SelectCb, *, placeholder: str = "Pick a member", row: int | None = None) -> None:
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, row=row)
        self._cb = callback

    async def callback(self, interaction: discord.Interaction) -> None:
        await self._cb(interaction, list(self.values))


class RolePicker(discord.ui.RoleSelect):
    def __init__(self, callback: SelectCb, *, placeholder: str = "Pick a role", row: int | None = None) -> None:
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, row=row)
        self._cb = callback

    async def callback(self, interaction: discord.Interaction) -> None:
        await self._cb(interaction, list(self.values))


class ChannelPicker(discord.ui.ChannelSelect):
    def __init__(self, callback: SelectCb, *, placeholder: str = "Pick a text channel", row: int | None = None) -> None:
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, row=row,
                         channel_types=[discord.ChannelType.text, discord.ChannelType.news])
        self._cb = callback

    async def callback(self, interaction: discord.Interaction) -> None:
        await self._cb(interaction, list(self.values))


def event_options(events: list[dict[str, Any]]) -> list[tuple[str, str, str | None]]:
    return [(f"#{e['id']} {e['title']}", str(e["id"]), f"{e['game']} • {e['status'].title()}" + (f" • {e['start_time'][:10]}" if e.get("start_time") else ""))
            for e in events[:25]]

# --------------------------------------------------------------------------------------------------
# ui/modals.py
# Generic form modal: describe fields, receive a dict of raw strings, validate in the callback.
# --------------------------------------------------------------------------------------------------

Submit = Callable[[discord.Interaction, dict[str, str]], Awaitable[None]]


@dataclass
class Field:
    key: str
    label: str
    placeholder: str = ""
    required: bool = True
    paragraph: bool = False
    default: str | None = None
    max_length: int = 100
    min_length: int | None = None


class FormModal(discord.ui.Modal):
    """Validation errors (ValidationError / ServiceError) raised in `on_submit_cb` bubble up to on_error,
    which shows them privately to the user without logging a traceback."""

    def __init__(self, title: str, fields: list[Field], on_submit_cb: Submit, *, timeout: float = 900) -> None:
        super().__init__(title=trunc(title, 45), timeout=timeout)
        if not 1 <= len(fields) <= 5:
            raise ValueError("A modal holds 1-5 fields")
        self._inputs: dict[str, discord.ui.TextInput] = {}
        self._cb = on_submit_cb
        for f in fields:
            default = trunc(f.default, f.max_length) if f.default else None
            ti = discord.ui.TextInput(
                label=trunc(f.label, 45), placeholder=trunc(f.placeholder, 100) or None, required=f.required,
                style=discord.TextStyle.paragraph if f.paragraph else discord.TextStyle.short,
                default=default, max_length=f.max_length, min_length=f.min_length)
            self._inputs[f.key] = ti
            self.add_item(ti)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        values: dict[str, Any] = {k: (i.value or "") for k, i in self._inputs.items()}
        await self._cb(interaction, values)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        await interaction.client.report_error(interaction, error, f"modal:{self.title}")  # type: ignore[attr-defined]

# --------------------------------------------------------------------------------------------------
# ui/helpers.py
# Display helpers shared by cogs.
# --------------------------------------------------------------------------------------------------

def _labels_sync(conn: sqlite3.Connection, guild_id: int, ids: list[int]) -> dict[int, str]:
    if not ids:
        return {}
    marks = ",".join("?" * len(ids))
    rows = conn.execute(f"SELECT user_id, ign FROM player_profiles WHERE guild_id=? AND user_id IN ({marks})", (guild_id, *ids)).fetchall()  # noqa: S608
    return {r["user_id"]: r["ign"] for r in rows}


async def labels_for(bot: "AegisBot", guild: discord.Guild | None, ids: Iterable[int]) -> dict[int, str]:
    """Plain-text names for select menus: profile IGN, else cached Discord name, else the ID."""
    uniq = list(dict.fromkeys(ids))
    gid = guild.id if guild else 0
    igns = await bot.db.read(lambda c: _labels_sync(c, gid, uniq))
    out: dict[int, str] = {}
    for uid in uniq:
        if uid in igns:
            out[uid] = igns[uid]
            continue
        m = guild.get_member(uid) if guild else None
        u = m or bot.get_user(uid)
        out[uid] = (m.display_name if m else u.name if u else f"User {uid}")
    return out


def is_mod_bot(user: discord.abc.User) -> bool:
    return bool(getattr(user, "bot", False))


def resolve_game(games: list[str], text: str | None) -> str:
    """Match user text to a configured game (case-insensitive). Raises ValidationError with the allowed list."""
    t = (text or "").strip().casefold()
    for g in games:
        if g.casefold() == t:
            return g
    raise ValidationError("Game must be one of: " + ", ".join(games[:12]) + ("…" if len(games) > 12 else "") + ".")

# --------------------------------------------------------------------------------------------------
# ui/views.py
# Base views and interaction helpers used by every cog.
#
# Rules these helpers enforce:
#   * never pass view=None to send_message / followup.send (discord.py rejects it) - kwargs are built without it
#   * navigation edits the current ephemeral menu in place; anything else opens a new private message
#   * every menu is bound to the user who opened it
# --------------------------------------------------------------------------------------------------

log_ui = logging.getLogger("aegis.ui")


def _kwargs(content: str | None, embed: discord.Embed | None, embeds: list[discord.Embed] | None,
            view: discord.ui.View | None) -> dict[str, Any]:
    kw: dict[str, Any] = {}
    if content is not None:
        kw["content"] = content
    if embed is not None:
        kw["embed"] = embed
    if embeds is not None:
        kw["embeds"] = embeds
    if view is not None:
        kw["view"] = view
    return kw


async def respond(interaction: discord.Interaction, *, content: str | None = None, embed: discord.Embed | None = None,
                  embeds: list[discord.Embed] | None = None, view: discord.ui.View | None = None,
                  ephemeral: bool = True) -> None:
    """Send a NEW message (initial response or follow-up)."""
    kw = _kwargs(content, embed, embeds, view)
    if interaction.response.is_done():
        await interaction.followup.send(ephemeral=ephemeral, **kw)
    else:
        await interaction.response.send_message(ephemeral=ephemeral, **kw)
    if view is not None and isinstance(view, BaseView):
        view.source = interaction


def _is_ephemeral_component(interaction: discord.Interaction) -> bool:
    msg = interaction.message
    return msg is not None and msg.flags.ephemeral


async def show(interaction: discord.Interaction, embed: discord.Embed | None = None, view: discord.ui.View | None = None, *,
               content: str | None = None, embeds: list[discord.Embed] | None = None, new: bool = False) -> None:
    """Navigate: edit the current private menu in place when possible, else send a new private message."""
    if not new and _is_ephemeral_component(interaction):
        kw: dict[str, Any] = {"embed": embed, "view": view, "content": content}
        if embeds is not None:
            kw["embeds"] = embeds
            kw["embed"] = None
        if interaction.response.is_done():
            await interaction.edit_original_response(**kw)
        else:
            await interaction.response.edit_message(**kw)
        if isinstance(view, BaseView):
            view.source = interaction
        return
    await respond(interaction, content=content, embed=embed, embeds=embeds, view=view, ephemeral=True)


async def say(interaction: discord.Interaction, text: str, kind: str = "info", *, view: discord.ui.View | None = None,
              new: bool = False) -> None:
    """Show a one-line notice (replacing the current menu when possible)."""
    await show(interaction, notice(text, kind), view, new=new)


class BaseView(discord.ui.View):
    """Private menu bound to the opener. Items must never be decorative: every item has a handler."""

    def __init__(self, bot: "AegisBot", owner_id: int | None = None, *, timeout: float | None = 600) -> None:
        super().__init__(timeout=timeout)
        self.bot = bot
        self.owner_id = owner_id
        self.source: discord.Interaction | None = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self.owner_id and interaction.user.id != self.owner_id:
            await interaction.response.send_message(embed=notice("This menu belongs to someone else. Open your own from the panel.", "warn"), ephemeral=True)
            return False
        problem = self.bot.gate(interaction)
        if problem:
            await interaction.response.send_message(embed=notice(problem, "warn"), ephemeral=True)
            return False
        return True

    async def on_error(self, interaction: discord.Interaction, error: Exception, item: discord.ui.Item) -> None:
        await self.bot.report_error(interaction, error, f"view:{type(self).__name__}")

    async def on_timeout(self) -> None:
        for child in self.children:
            if hasattr(child, "disabled"):
                child.disabled = True  # type: ignore[attr-defined]
        if self.source is not None:
            try:
                await self.source.edit_original_response(view=self)
            except discord.HTTPException:
                pass  # message gone or token expired - nothing to update


def home_button(bot: "AegisBot", row: int | None = None) -> ActionButton:
    async def go(i: discord.Interaction) -> None:
        await bot.open(i, "home")
    return ActionButton("Home", go, emoji="🏠", row=row)


def back_button(handler: Handler, row: int | None = None, label: str = "Back") -> ActionButton:
    return ActionButton(label, handler, emoji="◀️", row=row)


def add_nav(view: BaseView, back: Handler | None = None, row: int = 4) -> None:
    if back is not None:
        view.add_item(back_button(back, row))
    view.add_item(home_button(view.bot, row))


class MenuView(BaseView):
    """A grid of action buttons (optionally with a Back handler). `items` = (label, emoji, handler, style)."""

    def __init__(self, bot: "AegisBot", owner_id: int, items: list[tuple[str, str | None, Handler, discord.ButtonStyle]], *,
                 back: Handler | None = None) -> None:
        super().__init__(bot, owner_id)
        for idx, (label, emoji, handler, style) in enumerate(items[:20]):
            self.add_item(ActionButton(label, handler, emoji=emoji, style=style, row=idx // 4))
        add_nav(self, back, row=4)


class Pager(BaseView):
    """Paginated embeds with optional extra items (rows 1-3) and Back/Home."""

    def __init__(self, bot: "AegisBot", owner_id: int, pages: list[discord.Embed], *, extra: list[discord.ui.Item] | None = None,
                 back: Handler | None = None, refresh: Callable[[discord.Interaction], Awaitable[None]] | None = None) -> None:
        super().__init__(bot, owner_id)
        self.pages = pages or [discord.Embed(description="Nothing here.")]
        self.index = 0
        self._prev = ActionButton("Prev", self._on_prev, emoji="⬅️", row=0)
        self._next = ActionButton("Next", self._on_next, emoji="➡️", row=0)
        self._count = ActionButton("1/1", self._noop_refresh, row=0, disabled=True)
        if len(self.pages) > 1:
            self.add_item(self._prev)
            self.add_item(self._count)
            self.add_item(self._next)
        if refresh is not None:
            self.add_item(ActionButton("Refresh", refresh, emoji="🔄", row=0))
        for it in extra or []:
            self.add_item(it)
        add_nav(self, back, row=4)
        self._sync()

    def _sync(self) -> None:
        self._prev.disabled = self.index <= 0
        self._next.disabled = self.index >= len(self.pages) - 1
        self._count.label = f"{self.index + 1}/{len(self.pages)}"

    async def _noop_refresh(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

    async def _on_prev(self, interaction: discord.Interaction) -> None:
        self.index = max(0, self.index - 1)
        self._sync()
        await interaction.response.edit_message(embed=self.pages[self.index], view=self)

    async def _on_next(self, interaction: discord.Interaction) -> None:
        self.index = min(len(self.pages) - 1, self.index + 1)
        self._sync()
        await interaction.response.edit_message(embed=self.pages[self.index], view=self)

    async def start(self, interaction: discord.Interaction, *, new: bool = False) -> None:
        await show(interaction, self.pages[0], self, new=new)


class PickList(BaseView):
    """Paginated list where the current page's rows can be picked from a select menu.
    rows: (label, value, description, line_for_embed)."""

    def __init__(self, bot: "AegisBot", owner_id: int, brand: Branding, section: str,
                 rows: list[tuple[str, str, str | None, str]], on_pick: Callable[[discord.Interaction, str], Awaitable[None]], *,
                 placeholder: str = "Select one to open…", per_page: int = 10, back: Handler | None = None,
                 empty: str = "Nothing here yet.", header: str | None = None,
                 extra: list[discord.ui.Item] | None = None, refresh: Handler | None = None) -> None:
        super().__init__(bot, owner_id)
        self.brand, self.section, self.rows = brand, section, rows
        self.on_pick, self.placeholder, self.empty, self.header = on_pick, placeholder, empty, header
        self.per_page = max(1, min(per_page, 25))
        self.index = 0
        self._select: ChoiceSelect | None = None
        self._prev = ActionButton("Prev", self._go_prev, emoji="⬅️", row=0)
        self._next = ActionButton("Next", self._go_next, emoji="➡️", row=0)
        self._count = ActionButton("1/1", self._ack, row=0, disabled=True)
        if len(rows) > self.per_page:
            for it in (self._prev, self._count, self._next):
                self.add_item(it)
        if refresh is not None:
            self.add_item(ActionButton("Refresh", refresh, emoji="🔄", row=0))
        for it in extra or []:
            self.add_item(it)
        add_nav(self, back, row=4)
        self._rebuild()

    @property
    def pages(self) -> int:
        return max(1, -(-len(self.rows) // self.per_page))

    def _page_rows(self) -> list[tuple[str, str, str | None, str]]:
        return self.rows[self.index * self.per_page:(self.index + 1) * self.per_page]

    def _rebuild(self) -> None:
        if self._select is not None:
            self.remove_item(self._select)
            self._select = None
        page = self._page_rows()
        if page:
            self._select = ChoiceSelect([(l, v, d) for l, v, d, _ in page], self._picked, placeholder=self.placeholder, row=1)
            self.add_item(self._select)
        self._prev.disabled = self.index <= 0
        self._next.disabled = self.index >= self.pages - 1
        self._count.label = f"{self.index + 1}/{self.pages}"

    def embed(self) -> discord.Embed:
        page = self._page_rows()
        if not page:
            return base(self.brand, self.section, self.empty)
        body = "\n".join(line for *_, line in page)
        e = base(self.brand, self.section, f"{self.header}\n\n{body}" if self.header else body)
        if self.pages > 1:
            e.set_footer(text=f"{self.brand.footer[:200]} • Page {self.index + 1}/{self.pages} • {len(self.rows)} total")
        return e

    async def _picked(self, interaction: discord.Interaction, values: list[str]) -> None:
        await self.on_pick(interaction, values[0])

    async def _ack(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer()

    async def _go_prev(self, interaction: discord.Interaction) -> None:
        self.index = max(0, self.index - 1)
        self._rebuild()
        await interaction.response.edit_message(embed=self.embed(), view=self)

    async def _go_next(self, interaction: discord.Interaction) -> None:
        self.index = min(self.pages - 1, self.index + 1)
        self._rebuild()
        await interaction.response.edit_message(embed=self.embed(), view=self)

    async def start(self, interaction: discord.Interaction, *, new: bool = False) -> None:
        await show(interaction, self.embed(), self, new=new)


class ConfirmView(BaseView):
    def __init__(self, bot: "AegisBot", owner_id: int, on_yes: Handler, *, yes_label: str, danger: bool, on_no: Handler | None) -> None:
        super().__init__(bot, owner_id, timeout=120)
        self._on_yes = on_yes
        self._on_no = on_no
        self.add_item(ActionButton(yes_label, self._yes, style=DANGER if danger else SUCCESS, emoji="✔️"))
        self.add_item(ActionButton("Cancel", self._no, style=SECONDARY, emoji="✖️"))
        self._done = False

    async def _yes(self, interaction: discord.Interaction) -> None:
        if self._done:  # double-click protection
            await interaction.response.defer()
            return
        self._done = True
        self.stop()
        await self._on_yes(interaction)

    async def _no(self, interaction: discord.Interaction) -> None:
        self._done = True
        self.stop()
        if self._on_no is not None:
            await self._on_no(interaction)
        else:
            await interaction.response.edit_message(embed=notice("Cancelled. Nothing was changed.", "info"), view=None)


async def confirm(interaction: discord.Interaction, bot: "AegisBot", text: str, on_yes: Handler, *, yes_label: str = "Confirm",
                  danger: bool = True, on_no: Handler | None = None) -> None:
    view = ConfirmView(bot, interaction.user.id, on_yes, yes_label=yes_label, danger=danger, on_no=on_no)
    await show(interaction, notice(text, "warn"), view)

# --------------------------------------------------------------------------------------------------
# ui/announce.py
# Event announcement messages: persistent (restart-safe) buttons and live-updating embeds.
#
# The buttons use discord.py DynamicItem, so a single registered class handles every announced event
# without needing one View instance per message after a restart.
# --------------------------------------------------------------------------------------------------

log_announce = logging.getLogger("aegis.announce")

KIND_CODE = {"tournament": "t", "scrim": "s"}
CODE_KIND = {v: k for k, v in KIND_CODE.items()}
ACTIONS = {
    "view": ("View Details", discord.ButtonStyle.secondary, "🔍"),
    "reg": ("Register Team", discord.ButtonStyle.success, "📝"),
    "rules": ("Rules", discord.ButtonStyle.secondary, "📜"),
    "mine": ("My Registration", discord.ButtonStyle.primary, "🎫"),
}


class EventButton(discord.ui.DynamicItem[discord.ui.Button], template=r"aegis:ev:(?P<kind>[ts]):(?P<action>[a-z]+):(?P<id>[0-9]+)"):
    def __init__(self, kind_code: str, action: str, event_id: int) -> None:
        label, style, emoji = ACTIONS[action]
        super().__init__(discord.ui.Button(label=label, style=style, emoji=emoji, custom_id=f"aegis:ev:{kind_code}:{action}:{event_id}"))
        self.kind_code, self.action, self.event_id = kind_code, action, event_id

    @classmethod
    async def from_custom_id(cls, interaction: discord.Interaction, item: discord.ui.Item, match: re.Match[str], /) -> "EventButton":
        if match["action"] not in ACTIONS:
            raise ValueError("unknown action")
        return cls(match["kind"], match["action"], int(match["id"]))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        bot: "AegisBot" = interaction.client  # type: ignore[assignment]
        problem = bot.gate(interaction)
        if problem:
            await interaction.response.send_message(embed=notice(problem, "warn"), ephemeral=True)
            return False
        return True

    async def callback(self, interaction: discord.Interaction) -> None:
        bot: "AegisBot" = interaction.client  # type: ignore[assignment]
        handler = bot.event_handlers.get(self.kind_code)
        if handler is None:
            await interaction.response.send_message(embed=notice("That event type is not available.", "error"), ephemeral=True)
            return
        await handler(interaction, self.action, self.event_id)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        await interaction.client.report_error(interaction, error, "event-button")  # type: ignore[attr-defined]


def event_view(kind: str, event_id: int) -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    for action in ("view", "reg", "rules", "mine"):
        view.add_item(EventButton(KIND_CODE[kind], action, event_id))
    return view


def _fetch(conn, kind: str, event_id: int) -> dict[str, Any] | None:
    table = "tournaments" if kind == "tournament" else "scrims"
    return conn.execute(f"SELECT * FROM {table} WHERE id=?", (event_id,)).fetchone()  # noqa: S608


async def build_embed(bot: "AegisBot", kind: str, event_id: int) -> tuple[discord.Embed, dict[str, Any]] | None:
    def q(c):
        ev = _fetch(c, kind, event_id)
        if not ev:
            return None
        counts = registration_counts(c, kind, event_id)
        wl = get_bool(c, ev["guild_id"], "waitlist")
        return ev, counts, registration_state(kind, ev, counts, wl), (scrim_lobbies(c, event_id) if kind == "scrim" else None)
    data = await bot.db.read(q)
    if not data:
        return None
    ev, counts, state, lobbies = data
    brand = await bot.brand(ev["guild_id"])
    if kind == "tournament":
        return tournament_embed(brand, ev, counts, state), ev
    return scrim_embed(brand, ev, counts, state, lobbies), ev


async def publish_event_message(bot: "AegisBot", guild: discord.Guild | None, kind: str, event_id: int) -> discord.Message | None:
    """Post (or re-post) the public announcement in the configured channel. Returns None when no channel is usable."""
    built = await build_embed(bot, kind, event_id)
    if built is None or guild is None:
        return None
    embed, ev = built
    key = "ch_tournaments" if kind == "tournament" else "ch_scrims"
    channel_id = await bot.db.read(lambda c: int(get_setting(c, guild.id, key) or 0) or int(get_setting(c, guild.id, "ch_esports") or 0))
    await delete_event_message(bot, guild, kind, event_id)
    msg = await bot.post(guild, channel_id, embed=embed, view=event_view(kind, event_id))
    table = "tournaments" if kind == "tournament" else "scrims"
    await bot.db.run(lambda c: c.execute(f"UPDATE {table} SET channel_id=?, message_id=? WHERE id=?",  # noqa: S608
                                         (msg.channel.id if msg else None, msg.id if msg else None, event_id)))
    return msg


async def _get_message(bot: "AegisBot", guild: discord.Guild | None, ev: dict[str, Any]) -> discord.Message | None:
    if guild is None or not ev.get("channel_id") or not ev.get("message_id"):
        return None
    ch = guild.get_channel(ev["channel_id"])
    if ch is None or not hasattr(ch, "fetch_message"):
        return None
    try:
        return await ch.fetch_message(ev["message_id"])  # type: ignore[union-attr]
    except discord.HTTPException:
        return None


async def refresh_event_message(bot: "AegisBot", kind: str, event_id: int) -> None:
    """Best-effort: keep the public embed in sync with the database (counts, status, schedule)."""
    try:
        built = await build_embed(bot, kind, event_id)
        if built is None:
            return
        embed, ev = built
        guild = bot.get_guild(ev["guild_id"])
        msg = await _get_message(bot, guild, ev)
        if msg is None:
            return
        await msg.edit(embed=embed, view=event_view(kind, event_id) if ev["status"] in ("PUBLISHED", "LIVE", "PAUSED") else None)
    except Exception:  # noqa: BLE001 - cosmetic refresh must never break the caller
        log_announce.debug("could not refresh %s %s message", kind, event_id, exc_info=True)


async def delete_event_message(bot: "AegisBot", guild: discord.Guild | None, kind: str, event_id: int) -> None:
    ev = await bot.db.read(lambda c: _fetch(c, kind, event_id))
    if not ev:
        return
    msg = await _get_message(bot, guild, ev)
    if msg is not None:
        try:
            await msg.delete()
        except discord.HTTPException:
            pass


def minutes_until(start_iso: str | None, now_iso_value: str) -> float | None:
    if not start_iso:
        return None
    return (from_iso(start_iso) - from_iso(now_iso_value)).total_seconds() / 60


# ==================================================================================================
# SECTION 8-12 - COMMANDS, INTERACTIVE PANELS AND EVENT LISTENERS (COGS)
# ==================================================================================================

# --------------------------------------------------------------------------------------------------
# cogs/home.py
# Main persistent panel, ESPORTS HOME, the nine role dashboards, and help.
# --------------------------------------------------------------------------------------------------

log_home = logging.getLogger("aegis.home")

# key, label, emoji, style, entry   (custom_id = aegis:panel:<key>; the layout is 5 buttons per row)
PANEL: list[tuple[str, str, str, discord.ButtonStyle, str]] = [
    ("home", "ESPORTS HOME", "🏠", PRIMARY, "home"),
    ("tournaments", "Tournaments", "🏆", PRIMARY, "tournaments"),
    ("scrims", "Scrims", "🎯", PRIMARY, "scrims"),
    ("register", "Register Team", "📝", SUCCESS, "reg.start"),
    ("myteam", "My Team", "👥", SECONDARY, "team.mine"),
    ("create", "Create Team", "➕", SECONDARY, "team.create"),
    ("find", "Find Team", "🔎", SECONDARY, "team.find"),
    ("results", "Match Results", "🧾", SECONDARY, "results"),
    ("leaderboards", "Leaderboards", "📊", SECONDARY, "leaderboards"),
    ("profile", "Player Profile", "🪪", SECONDARY, "player.profile"),
    ("report", "Report Issue", "🚨", DANGER, "report.start"),
    ("help", "Help & Support", "❓", SECONDARY, "help"),
    ("staff", "Staff Panel", "🛡️", SECONDARY, "staff"),
]

# Dashboard definitions: section -> (title, description, [(label, emoji, entry, style)])
DASHBOARDS: dict[str, tuple[str, str, list[tuple[str, str, str, discord.ButtonStyle]]]] = {
    "public": ("PUBLIC DASHBOARD", "Open to everyone: browse events and rankings.", [
        ("Tournaments", "🏆", "tournaments", PRIMARY), ("Scrims", "🎯", "scrims", PRIMARY), ("Leaderboards", "📊", "leaderboards", SECONDARY),
        ("Match Results", "🧾", "results", SECONDARY), ("Calendar", "📅", "community.calendar", SECONDARY), ("Recruitment", "📢", "community.recruit", SECONDARY),
        ("Polls", "🗳️", "community.polls", SECONDARY), ("Giveaways", "🎁", "community.giveaways", SECONDARY), ("Hall of Fame", "👑", "lb.hall", SECONDARY),
        ("Find Team", "🔎", "team.find", SECONDARY), ("Help", "❓", "help", SECONDARY)]),
    "player": ("PLAYER DASHBOARD", "Your profile, team, registrations and results.", [
        ("My Profile", "🪪", "player.profile", PRIMARY), ("My Team", "👥", "team.mine", PRIMARY), ("Invites", "✉️", "team.invites", SECONDARY),
        ("My Registrations", "🎫", "reg.mine", SECONDARY), ("My Schedule", "🗓️", "sched.mine", SECONDARY), ("Check-in", "✅", "reg.checkin", SUCCESS),
        ("My Results", "🧾", "result.mine", SECONDARY), ("Event History", "📜", "player.history", SECONDARY), ("My Stats", "📈", "player.stats", SECONDARY),
        ("Badges", "🏅", "community.badges", SECONDARY), ("My Reports", "📂", "report.mine", SECONDARY), ("Report Issue", "🚨", "report.start", DANGER)]),
    "captain": ("CAPTAIN DASHBOARD", "Tools for team captains and co-captains.", [
        ("Register Team", "📝", "reg.start", SUCCESS), ("Check-in", "✅", "reg.checkin", SUCCESS), ("Submit Result", "📤", "result.submit", PRIMARY),
        ("My Team", "👥", "team.mine", PRIMARY), ("Invites & Requests", "✉️", "team.invites", SECONDARY), ("Recruit Players", "📢", "community.recruit", SECONDARY),
        ("My Results / Appeals", "🧾", "result.mine", SECONDARY), ("Schedule", "🗓️", "sched.mine", SECONDARY)]),
    "organizer": ("ORGANIZER DASHBOARD", "Create and run tournaments.", [
        ("Create Tournament", "➕", "tour.create", SUCCESS), ("Manage Tournaments", "🛠️", "tour.manage", PRIMARY), ("Review Results", "🧾", "result.review", PRIMARY),
        ("Case Queue", "🧰", "report.cases", SECONDARY), ("Announcements", "📣", "community.announce", SECONDARY), ("Polls", "🗳️", "community.polls", SECONDARY),
        ("Giveaways", "🎁", "community.giveaways", SECONDARY), ("Calendar", "📅", "community.calendar", SECONDARY)]),
    "scrim": ("SCRIM MANAGER DASHBOARD", "Create lobbies, assign slots, manage scrims.", [
        ("Create Scrim", "➕", "scrim.create", SUCCESS), ("Manage Scrims", "🛠️", "scrim.manage", PRIMARY), ("Review Results", "🧾", "result.review", PRIMARY),
        ("Assign Referees", "🧑‍⚖️", "staff.referees", SECONDARY), ("Announcements", "📣", "community.announce", SECONDARY), ("Leaderboards", "📊", "lb.scrims", SECONDARY)]),
    "referee": ("REFEREE DASHBOARD", "Review results, handle disputes and evidence.", [
        ("Review Results", "🧾", "result.review", PRIMARY), ("Case Queue", "🧰", "report.cases", PRIMARY), ("Referee Assignments", "🧑‍⚖️", "staff.referees", SECONDARY),
        ("Recent Results", "📰", "result.recent", SECONDARY)]),
    "staff": ("STAFF DASHBOARD", "Moderation and verification tools.", [
        ("Staff Panel", "🛡️", "staff", PRIMARY), ("Case Queue", "🧰", "report.cases", PRIMARY), ("Verify Teams", "✅", "team.directory", SECONDARY),
        ("Announcements", "📣", "community.announce", SECONDARY), ("Staff Activity", "🕵️", "staff.activity", SECONDARY), ("Player Lookup", "🔎", "player.find", SECONDARY)]),
    "admin": ("ADMIN DASHBOARD", "Server configuration and data management.", [
        ("Admin Panel", "⚙️", "admin", PRIMARY), ("Staff Roster", "👥", "staff.members", SECONDARY), ("Applications", "📨", "staff.applications", SECONDARY),
        ("Backups", "💾", "admin.backups", SECONDARY), ("Case Queue", "🧰", "report.cases", SECONDARY), ("Hall of Fame", "👑", "community.hof", SECONDARY)]),
    "owner": ("OWNER DASHBOARD", "Bot-level health and control (OWNER_ID only).", [
        ("Owner Panel", "👑", "owner", PRIMARY), ("Diagnostics", "🩺", "owner.diagnostics", SECONDARY), ("Error Log", "🐞", "owner.errors", SECONDARY),
        ("Backups & Restore", "💾", "admin.backups", SECONDARY)]),
}
SECTION_LABEL = {"public": "Public", "player": "Player", "captain": "Captain", "organizer": "Organizer", "scrim": "Scrim Manager",
                 "referee": "Referee", "staff": "Staff", "admin": "Admin", "owner": "Owner"}


def referenced_entries() -> set[str]:
    out = {p[4] for p in PANEL}
    for _, _, items in DASHBOARDS.values():
        out.update(it[2] for it in items)
    out.update({"home", "help"})
    return out


class PanelView(discord.ui.View):
    """The persistent public panel. Every button is a fixed custom_id, so the same view serves every restart."""

    def __init__(self, bot: AegisBot) -> None:
        super().__init__(timeout=None)
        self.bot = bot
        for idx, (key, label, emoji, style, entry) in enumerate(PANEL):
            self.add_item(ActionButton(label, self._opener(entry), emoji=emoji, style=style, row=idx // 5, custom_id=f"aegis:panel:{key}"))

    def _opener(self, entry: str):
        async def go(i: discord.Interaction) -> None:
            problem = self.bot.gate(i)
            if problem:
                await respond(i, embed=notice(problem, "warn"))
                return
            await self.bot.open(i, entry)
        return go

    async def on_error(self, interaction: discord.Interaction, error: Exception, item: discord.ui.Item) -> None:
        await self.bot.report_error(interaction, error, "panel")


class Home(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        self.bot.register_entry("home", self.home)
        self.bot.register_entry("help", self.help)
        self.bot.add_view(PanelView(self.bot))  # restart-safe: persistent custom_ids, timeout=None

    # ------------------------------------------------------------------ home
    async def _context(self, i: discord.Interaction) -> tuple[Perms, bool, bool]:
        perms = await self.bot.perms(i)
        team = await self.bot.db.read(lambda c: team_of_user(c, i.guild_id, i.user.id))
        return perms, bool(team and team["my_role"] in ("captain", "cocaptain")), team is not None

    async def home(self, i: discord.Interaction) -> None:
        perms, is_cap, has_team = await self._context(i)
        brand = await self.bot.brand(i.guild_id)
        sections = dashboard_sections(perms, is_cap, has_team)
        e = home_embed(brand)
        e.add_field(name="Your dashboards", value=" • ".join(SECTION_LABEL[s] for s in sections), inline=False)
        view = BaseView(self.bot, i.user.id)

        async def chosen(x: discord.Interaction, vals: list[str]) -> None:
            await self.dashboard(x, vals[0])
        view.add_item(ChoiceSelect([(SECTION_LABEL[s] + " dashboard", s, DASHBOARDS[s][1]) for s in sections], chosen, placeholder="Open a dashboard…", row=0))
        quick = [("Tournaments", "🏆", "tournaments"), ("Scrims", "🎯", "scrims"), ("My Team", "👥", "team.mine"), ("Leaderboards", "📊", "leaderboards")]
        for idx, (label, emoji, entry) in enumerate(quick):
            view.add_item(ActionButton(label, (lambda en: lambda x: self.bot.open(x, en))(entry), emoji=emoji, style=SECONDARY, row=1))
        view.add_item(ActionButton("Refresh", self.home, emoji="🔄", row=4))
        await show(i, e, view)

    # ------------------------------------------------------------------ dashboards
    def _stats(self, c, guild_id: int, user_id: int, section: str) -> dict[str, int | str]:
        def n(sql: str, *args) -> int:
            return c.execute(sql, args).fetchone()["n"]
        if section == "public":
            return {"Open tournaments": n("SELECT COUNT(*) AS n FROM tournaments WHERE guild_id=? AND status='PUBLISHED'", guild_id),
                    "Open scrims": n("SELECT COUNT(*) AS n FROM scrims WHERE guild_id=? AND status='PUBLISHED'", guild_id),
                    "Teams": n("SELECT COUNT(*) AS n FROM teams WHERE guild_id=? AND disbanded=0", guild_id)}
        if section in ("player", "captain"):
            return {"Pending invites": n("SELECT COUNT(*) AS n FROM team_invites WHERE guild_id=? AND user_id=? AND status='PENDING'", guild_id, user_id),
                    "Open reports by you": n("SELECT COUNT(*) AS n FROM reports WHERE guild_id=? AND reporter_id=? AND status!='RESOLVED'", guild_id, user_id)}
        if section == "organizer":
            return {"Your live tournaments": n("SELECT COUNT(*) AS n FROM tournaments WHERE guild_id=? AND organizer_id=? AND status IN ('PUBLISHED','LIVE')", guild_id, user_id),
                    "Registrations awaiting approval": n("SELECT COUNT(*) AS n FROM tournament_registrations r JOIN tournaments t ON t.id=r.tournament_id "
                                                         "WHERE t.guild_id=? AND r.status='PENDING'", guild_id)}
        if section == "scrim":
            return {"Your live scrims": n("SELECT COUNT(*) AS n FROM scrims WHERE guild_id=? AND organizer_id=? AND status IN ('PUBLISHED','PAUSED')", guild_id, user_id)}
        if section in ("referee", "staff", "admin"):
            return {"Results awaiting review": n("SELECT COUNT(*) AS n FROM match_results r JOIN matches m ON m.id=r.match_id WHERE m.guild_id=? AND r.status='PENDING'", guild_id),
                    "Open cases": n("SELECT COUNT(*) AS n FROM reports WHERE guild_id=? AND status!='RESOLVED'", guild_id)}
        return {}

    async def dashboard(self, i: discord.Interaction, section: str) -> None:
        perms, is_cap, has_team = await self._context(i)
        if section not in dashboard_sections(perms, is_cap, has_team):  # server-side: never trust the select value
            raise ServiceError("You don't have access to that dashboard.")
        brand = await self.bot.brand(i.guild_id)
        title, text, items = DASHBOARDS[section]
        stats = await self.bot.db.read(lambda c: self._stats(c, i.guild_id, i.user.id, section))
        view = MenuView(self.bot, i.user.id, [(label, emoji, (lambda en: lambda x: self.bot.open(x, en))(entry), style) for label, emoji, entry, style in items], back=self.home)
        await show(i, dashboard_embed(brand, title, text, stats or None), view)

    # ------------------------------------------------------------------ help
    async def help(self, i: discord.Interaction) -> None:
        brand = await self.bot.brand(i.guild_id)
        e = base(brand, "HELP & SUPPORT", "Everything is button-driven and private to you.")
        e.add_field(name="Getting started", value="1. **Player Profile** - set your in-game name, role and games.\n2. **Create Team** or **Find Team**.\n"
                                                   "3. Captains: **Register Team** for a tournament or scrim, then **Check-in** when it opens.", inline=False)
        e.add_field(name="Results", value="Captains submit results with screenshots; referees approve them. Standings only count **approved** results.", inline=False)
        e.add_field(name="Problems", value="Use **Report Issue** for players, disputes, appeals or bugs. Staff reply privately.", inline=False)
        e.add_field(name="Slash commands", value="`/aegis` open your home • `/profile` view a profile • `/evidence` attach result proof • `/panel` (admins) post the panel", inline=False)
        items = [("Report Issue", "🚨", lambda x: self.bot.open(x, "report.start"), DANGER), ("My Reports", "📂", lambda x: self.bot.open(x, "report.mine"), SECONDARY),
                 ("Apply for Staff", "📝", lambda x: self.bot.open(x, "staff.apply"), SECONDARY)]
        await show(i, e, MenuView(self.bot, i.user.id, items))

    # ------------------------------------------------------------------ slash commands
    async def _slash_ok(self, i: discord.Interaction) -> bool:
        problem = self.bot.gate(i)
        if problem:
            await respond(i, embed=notice(problem, "warn"))
        return problem is None

    @app_commands.command(name="aegis", description="Open your AEGIS esports home (private)")
    @app_commands.guild_only()
    async def aegis_cmd(self, i: discord.Interaction) -> None:
        if await self._slash_ok(i):
            await self.home(i)

    @app_commands.command(name="panel", description="Admins: post the main AEGIS panel in this channel")
    @app_commands.guild_only()
    @app_commands.default_permissions(administrator=True)
    async def panel_cmd(self, i: discord.Interaction) -> None:
        if not await self._slash_ok(i) or not await self.bot.require(i, lambda p: p.is_admin, "Only administrators can post the panel."):
            return
        await self._post_panel(i, i.channel_id)

    async def publish_flow(self, i: discord.Interaction) -> None:
        """Admin panel entry: pick the channel to post the persistent panel in."""
        if not await self.bot.require(i, lambda p: p.is_admin, "Only administrators can post the panel."):
            return
        brand = await self.bot.brand(i.guild_id)

        async def picked(x: discord.Interaction, vals: list) -> None:
            await self._post_panel(x, vals[0].id)
        view = BaseView(self.bot, i.user.id)
        view.add_item(ChannelPicker(picked, placeholder="Channel for the main panel", row=0))
        add_nav(view, lambda x: self.bot.open(x, "admin"))
        await show(i, base(brand, "PUBLISH MAIN PANEL", "Pick the channel. If a panel already exists it is updated in place instead of duplicated."), view)

    async def _post_panel(self, i: discord.Interaction, channel_id: int | None) -> None:
        if not await self.bot.require(i, lambda p: p.is_admin, "Only administrators can post the panel."):
            return
        guild = i.guild
        ch = guild.get_channel(channel_id) if guild and channel_id else None
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            raise ServiceError("Pick a normal text channel.")
        need = ch.permissions_for(guild.me)
        missing = [n for n, ok in (("View Channel", need.view_channel), ("Send Messages", need.send_messages), ("Embed Links", need.embed_links)) if not ok]
        if missing:
            raise ServiceError(f"I'm missing **{', '.join(missing)}** in {ch.mention}.")
        brand = await self.bot.brand(i.guild_id)
        embed, view = home_embed(brand), PanelView(self.bot)
        old = await self.bot.db.read(lambda c: c.execute("SELECT channel_id,message_id FROM persistent_panel_settings WHERE guild_id=? AND panel='main'", (i.guild_id,)).fetchone())
        msg = None
        if old and old["channel_id"] == ch.id:
            try:
                msg = await ch.fetch_message(old["message_id"])
                await msg.edit(embed=embed, view=view)
            except discord.HTTPException:
                msg = None  # deleted or not editable: post a fresh one
        if msg is None:
            try:
                msg = await ch.send(embed=embed, view=view)
            except discord.HTTPException as exc:
                raise ServiceError(f"I couldn't post in {ch.mention}: {exc.text or exc.status}") from exc
            if old and old["channel_id"] != ch.id:
                prev = guild.get_channel(old["channel_id"])
                if isinstance(prev, (discord.TextChannel, discord.Thread)):
                    try:
                        await (await prev.fetch_message(old["message_id"])).delete()  # remove the superseded panel
                    except discord.HTTPException:
                        pass
        message_id = msg.id

        def save(c):
            c.execute("INSERT INTO persistent_panel_settings(guild_id,panel,channel_id,message_id,updated_at) VALUES (?,?,?,?,?) "
                      "ON CONFLICT(guild_id,panel) DO UPDATE SET channel_id=excluded.channel_id, message_id=excluded.message_id, updated_at=excluded.updated_at",
                      (i.guild_id, "main", ch.id, message_id, now_iso()))
            audit(c, i.guild_id, i.user.id, "admin.panel", str(ch.id), str(message_id))
        await self.bot.db.run(save)
        await show(i, notice(f"Main panel is live in {ch.mention}. Its buttons keep working after restarts.", "ok"))

# --------------------------------------------------------------------------------------------------
# cogs/teams.py
# Teams: create, roster, invites, join requests, transfers, verification, directory.
# --------------------------------------------------------------------------------------------------

MANAGERS = ("captain", "cocaptain")


class Teams(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for name, fn in {
            "team.mine": self.my_team, "team.create": self.create, "team.find": self.find, "team.directory": self.directory,
            "team.invites": self.my_invites,
        }.items():
            self.bot.register_entry(name, fn)

    # ------------------------------------------------------------------ my team
    async def my_team(self, i: discord.Interaction) -> None:
        gid, uid = i.guild_id, i.user.id

        def q(c):
            t = team_of_user(c, gid, uid)
            if not t:
                return None
            return (t, team_members(c, t["id"]), team_stats(c, t["id"]), team_history(c, t["id"]),
                    len(pending_for_team(c, t["id"])),
                    c.execute("SELECT 1 FROM reports WHERE guild_id=? AND kind='team' AND target=? AND status<>'RESOLVED'", (gid, f"team:{t['id']}")).fetchone() is not None)
        data = await self.bot.db.read(q)
        brand = await self.bot.brand(gid)
        if data is None:
            await self._no_team(i, brand)
            return
        team, members, stats, history, pending, verif_pending = data
        role = team["my_role"]
        view = BaseView(self.bot, uid)
        tid = team["id"]
        if role in MANAGERS:
            view.add_item(ActionButton("Invite Player", lambda x: self.invite_picker(x, tid), emoji="➕", style=PRIMARY, row=0))
            view.add_item(ActionButton(f"Join Requests ({pending})", lambda x: self.requests(x, tid), emoji="📥", row=0))
            view.add_item(ActionButton("Manage Roster", lambda x: self.roster(x, tid), emoji="👥", row=0))
            view.add_item(ActionButton("Edit Team", lambda x: self.edit_modal(x, tid), emoji="✏️", row=1))
            view.add_item(ActionButton("Recruiting: " + ("ON" if team["recruiting"] else "OFF"), lambda x: self.toggle_recruiting(x, tid),
                                       emoji="📢", style=SUCCESS if team["recruiting"] else SECONDARY, row=1))
            if not team["verified"]:
                view.add_item(ActionButton("Verification requested" if verif_pending else "Request Verification",
                                           lambda x: self.request_verification(x, tid), emoji="✅", row=1, disabled=verif_pending))
            view.add_item(ActionButton("Register for Events", lambda x: self.bot.open(x, "reg.start"), emoji="🏆", style=PRIMARY, row=2))
        if role == "captain":
            view.add_item(ActionButton("Disband", lambda x: self.disband(x, tid), emoji="🗑️", style=DANGER, row=2))
        else:
            view.add_item(ActionButton("Leave Team", self.leave, emoji="🚪", style=DANGER, row=2))
        view.add_item(ActionButton("Refresh", self.my_team, emoji="🔄", row=2))
        add_nav(view)
        await show(i, team_embed(brand, team, members, stats, history), view)

    async def _no_team(self, i: discord.Interaction, brand: Branding) -> None:
        invites = await self.bot.db.read(lambda c: pending_for_user(c, i.guild_id, i.user.id))
        view = MenuView(self.bot, i.user.id, [
            ("Create Team", "🛡️", self.create, PRIMARY), ("Find Team", "🔎", self.find, SECONDARY),
            (f"My Invites ({len(invites)})", "✉️", self.my_invites, SECONDARY), ("Team Directory", "📋", self.directory, SECONDARY)])
        await show(i, base(brand, "MY TEAM", "You are not in a team yet.\nCreate one, accept an invite, or browse teams that are recruiting."), view)

    # ------------------------------------------------------------------ create / edit
    async def create(self, i: discord.Interaction) -> None:
        games = await self.bot.db.read(lambda c: get_games(c, i.guild_id))

        async def submit(inter: discord.Interaction, f: dict[str, str]) -> None:
            name = validate_team_name(f["name"])
            tag = validate_tag(f["tag"])
            logo = validate_url(f["logo"], field="Logo URL")
            game = optional(f["game"], field="Game", max_len=40)
            await self.bot.db.run(lambda c: create_team(c, inter.guild_id, inter.user.id, name, tag, logo, game))
            await self.my_team(inter)
        await i.response.send_modal(FormModal("Create Team", [
            Field("name", "Team name", "3-32 characters", max_length=32, min_length=3),
            Field("tag", "Tag (optional)", "2-5 letters/numbers", required=False, max_length=5),
            Field("logo", "Logo URL (optional)", "https://...", required=False, max_length=500),
            Field("game", "Main game (optional)", ", ".join(games[:3]), required=False, max_length=40)], submit))

    async def edit_modal(self, i: discord.Interaction, tid: int) -> None:
        team = await self.bot.db.read(lambda c: get_team(c, tid))
        if not team:
            await say(i, "That team no longer exists.", "error")
            return

        async def submit(inter: discord.Interaction, f: dict[str, str]) -> None:
            tag = validate_tag(f["tag"])
            logo = validate_url(f["logo"], field="Logo URL")
            game = optional(f["game"], field="Game", max_len=40)
            await self.bot.db.run(lambda c: update_team(c, tid, inter.user.id, tag=tag, logo_url=logo, game=game))
            await self.my_team(inter)
        await i.response.send_modal(FormModal("Edit Team", [
            Field("tag", "Tag", required=False, max_length=5, default=team["tag"]),
            Field("logo", "Logo URL", required=False, max_length=500, default=team["logo_url"]),
            Field("game", "Main game", required=False, max_length=40, default=team["game"])], submit))

    async def toggle_recruiting(self, i: discord.Interaction, tid: int) -> None:
        def go(c):
            t = get_team(c, tid)
            if not t:
                raise ServiceError("That team no longer exists.")
            update_team(c, tid, i.user.id, recruiting=0 if t["recruiting"] else 1)
        await self.bot.db.run(go)
        await self.my_team(i)

    async def request_verification(self, i: discord.Interaction, tid: int) -> None:
        def go(c):
            t = get_team(c, tid)
            m = c.execute("SELECT role FROM team_members WHERE team_id=? AND user_id=?", (tid, i.user.id)).fetchone()
            if not t or not m or m["role"] not in MANAGERS:
                raise ServiceError("Only the captain or co-captain can request verification.")
            if t["verified"]:
                raise ServiceError("This team is already verified.")
            return create_report(c, i.guild_id, i.user.id, "team", f"team:{tid}",
                                                f"Verification request for team '{t['name']}' (#{tid}).")
        rid = await self.bot.db.run(go)
        await self.bot.staff_log(i.guild, f"✅ New verification request: case **#{rid}** from <@{i.user.id}>.")
        await self.my_team(i)

    # ------------------------------------------------------------------ invites
    async def invite_picker(self, i: discord.Interaction, tid: int) -> None:
        async def picked(inter: discord.Interaction, vals: list) -> None:
            user = vals[0]
            if getattr(user, "bot", False):
                raise ServiceError("You cannot invite a bot.")
            await self.bot.db.run(lambda c: create_invite(c, team_id=tid, actor_id=inter.user.id, target_id=user.id, kind="invite"))
            team = await self.bot.db.read(lambda c: get_team(c, tid))
            brand = await self.bot.brand(inter.guild_id)
            sent = await self.bot.dm(user.id, embed=base(brand, "TEAM INVITE", f"You were invited to join **{team['name']}** in **{inter.guild.name}**.\n"
                                                                 "Open the AEGIS panel → **Find Team → My Invites** to accept or decline."))
            await say(inter, f"Invite sent to <@{user.id}>." + ("" if sent else " (Their DMs are closed - ask them to open **My Invites** on the panel.)"), "ok",
                      view=self._back_to_team())
        view = BaseView(self.bot, i.user.id)
        view.add_item(UserPicker(picked, placeholder="Pick the player to invite", row=0))
        add_nav(view, self.my_team)
        brand = await self.bot.brand(i.guild_id)
        await show(i, base(brand, "INVITE PLAYER", "Pick a server member. They get a private invite they can accept from the panel."), view)

    def _back_to_team(self) -> BaseView:
        view = BaseView(self.bot, None)
        view.add_item(ActionButton("Back to My Team", self.my_team, emoji="🛡️", style=PRIMARY))
        return view

    async def my_invites(self, i: discord.Interaction) -> None:
        invites = await self.bot.db.read(lambda c: pending_for_user(c, i.guild_id, i.user.id))
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"{inv['team_name']}", str(inv["id"]), f"Invite #{inv['id']}", f"✉️ **{inv['team_name']}** • invite #{inv['id']}") for inv in invites]

        async def picked(inter: discord.Interaction, value: str) -> None:
            inv = next((x for x in invites if str(x["id"]) == value), None)
            if not inv:
                await self.my_invites(inter)
                return
            view = BaseView(self.bot, inter.user.id)
            view.add_item(ActionButton("Accept", lambda x: self._answer_invite(x, inv["id"], True), style=SUCCESS, emoji="✅"))
            view.add_item(ActionButton("Decline", lambda x: self._answer_invite(x, inv["id"], False), style=DANGER, emoji="✖️"))
            add_nav(view, self.my_invites)
            await show(inter, base(brand, "TEAM INVITE", f"**{inv['team_name']}** invited you to join."), view)
        await PickList(self.bot, i.user.id, brand, "MY INVITES", rows, picked, placeholder="Open an invite…", back=self.my_team,
                       empty="You have no pending invites.").start(i)

    async def _answer_invite(self, i: discord.Interaction, invite_id: int, accept: bool) -> None:
        res = await self.bot.db.run(lambda c: resolve_invite(c, invite_id, i.user.id, accept))
        if accept:
            await self.bot.dm(res["team"]["captain_id"], content=f"<@{i.user.id}> joined your team **{res['team']['name']}**.")
            await self.my_team(i)
        else:
            await say(i, "Invite declined.", "info", view=self._back_to_team())

    async def requests(self, i: discord.Interaction, tid: int) -> None:
        reqs = await self.bot.db.read(lambda c: pending_for_team(c, tid))
        names = await labels_for(self.bot, i.guild, [r["user_id"] for r in reqs])
        brand = await self.bot.brand(i.guild_id)
        rows = [(names[r["user_id"]], str(r["id"]), "Transfer request" if r["kind"] == "transfer" else "Join request",
                 f"{'🔁' if r['kind'] == 'transfer' else '📥'} <@{r['user_id']}> ({names[r['user_id']]}) • {r['kind']}") for r in reqs]

        async def picked(inter: discord.Interaction, value: str) -> None:
            rid = int(value)
            view = BaseView(self.bot, inter.user.id)
            view.add_item(ActionButton("Approve", lambda x: self._answer_request(x, tid, rid, True), style=SUCCESS, emoji="✅"))
            view.add_item(ActionButton("Decline", lambda x: self._answer_request(x, tid, rid, False), style=DANGER, emoji="✖️"))
            add_nav(view, lambda x: self.requests(x, tid))
            await show(inter, base(brand, "JOIN REQUEST", f"Decide on request #{rid}."), view)
        await PickList(self.bot, i.user.id, brand, "JOIN REQUESTS", rows, picked, placeholder="Pick a request…", back=self.my_team,
                       empty="No pending requests.").start(i)

    async def _answer_request(self, i: discord.Interaction, tid: int, rid: int, accept: bool) -> None:
        res = await self.bot.db.run(lambda c: resolve_invite(c, rid, i.user.id, accept))
        await self.bot.dm(res["invite"]["user_id"], content=f"Your request to join **{res['team']['name']}** was {'approved ✅' if accept else 'declined'}.")
        await self.requests(i, tid)

    # ------------------------------------------------------------------ roster
    async def roster(self, i: discord.Interaction, tid: int) -> None:
        members = await self.bot.db.read(lambda c: team_members(c, tid))
        names = await labels_for(self.bot, i.guild, [m["user_id"] for m in members])
        brand = await self.bot.brand(i.guild_id)
        rows = [(names[m["user_id"]], str(m["user_id"]), ROLE_LABEL[m["role"]],
                 f"**{ROLE_LABEL[m['role']]}** <@{m['user_id']}> ({names[m['user_id']]})") for m in members]

        async def picked(inter: discord.Interaction, value: str) -> None:
            await self.member_menu(inter, tid, int(value))
        await PickList(self.bot, i.user.id, brand, "MANAGE ROSTER", rows, picked, placeholder="Pick a member to manage…", back=self.my_team).start(i)

    async def member_menu(self, i: discord.Interaction, tid: int, target: int) -> None:
        m = await self.bot.db.read(lambda c: c.execute("SELECT * FROM team_members WHERE team_id=? AND user_id=?", (tid, target)).fetchone())
        if not m:
            await self.roster(i, tid)
            return
        brand = await self.bot.brand(i.guild_id)
        view = BaseView(self.bot, i.user.id)

        def setter(role: str):
            async def go(x: discord.Interaction) -> None:
                await self.bot.db.run(lambda c: set_member_role(c, tid, target, role, x.user.id))
                await self.member_menu(x, tid, target)
            return go
        if m["role"] != "captain":
            view.add_item(ActionButton("Co-Captain", setter("cocaptain"), emoji="⭐", disabled=m["role"] == "cocaptain", row=0))
            view.add_item(ActionButton("Main Player", setter("player"), emoji="🎮", disabled=m["role"] == "player", row=0))
            view.add_item(ActionButton("Substitute", setter("sub"), emoji="🔁", disabled=m["role"] == "sub", row=0))
            view.add_item(ActionButton("Remove", lambda x: self._confirm_remove(x, tid, target), emoji="🚫", style=DANGER, row=1))
            view.add_item(ActionButton("Make Captain", lambda x: self._confirm_transfer(x, tid, target), emoji="👑", style=DANGER, row=1))
        add_nav(view, lambda x: self.roster(x, tid))
        await show(i, base(brand, "MEMBER", f"<@{target}> • **{ROLE_LABEL[m['role']]}**\nChoose what to change."), view)

    async def _confirm_remove(self, i: discord.Interaction, tid: int, target: int) -> None:
        async def yes(x: discord.Interaction) -> None:
            await self.bot.db.run(lambda c: remove_member(c, tid, target, x.user.id))
            await self.roster(x, tid)
        await confirm(i, self.bot, f"Remove <@{target}> from the team?", yes, yes_label="Remove", on_no=lambda x: self.member_menu(x, tid, target))

    async def _confirm_transfer(self, i: discord.Interaction, tid: int, target: int) -> None:
        async def yes(x: discord.Interaction) -> None:
            await self.bot.db.run(lambda c: transfer_captaincy(c, tid, target, x.user.id))
            await self.my_team(x)
        await confirm(i, self.bot, f"Make <@{target}> the captain? You will become a regular player. Only they can undo this.", yes,
                      yes_label="Transfer captaincy", on_no=lambda x: self.member_menu(x, tid, target))

    async def leave(self, i: discord.Interaction) -> None:
        async def yes(x: discord.Interaction) -> None:
            team = await self.bot.db.run(lambda c: leave_team(c, x.guild_id, x.user.id))
            await self.bot.dm(team["captain_id"], content=f"<@{x.user.id}> left your team **{team['name']}**.")
            await self._no_team(x, await self.bot.brand(x.guild_id))
        await confirm(i, self.bot, "Leave your team? You will be removed from the roster (event rosters already locked in stay as registered).", yes,
                      yes_label="Leave team", on_no=self.my_team)

    async def disband(self, i: discord.Interaction, tid: int) -> None:
        async def yes(x: discord.Interaction) -> None:
            await self.bot.db.run(lambda c: disband_team(c, tid, x.user.id))
            await self._no_team(x, await self.bot.brand(x.guild_id))
        await confirm(i, self.bot, "**Disband this team?** All members are removed and active event registrations are withdrawn. This cannot be undone.", yes,
                      yes_label="Disband team", on_no=self.my_team)

    # ------------------------------------------------------------------ browsing
    async def find(self, i: discord.Interaction) -> None:
        teams = await self.bot.db.read(lambda c: recruiting_teams(c, i.guild_id))
        await self._team_list(i, teams, "FIND TEAM", "No teams are recruiting right now. Check back later or create your own!", back=self._find_back())

    def _find_back(self):
        async def go(x: discord.Interaction) -> None:
            await self.my_team(x)
        return go

    async def directory(self, i: discord.Interaction) -> None:
        teams = await self.bot.db.read(lambda c: list_teams(c, i.guild_id))
        await self._team_list(i, teams, "TEAM DIRECTORY", "No teams have been created yet.", back=self._find_back())

    async def _team_list(self, i: discord.Interaction, teams: list[dict], section: str, empty: str, *, back) -> None:
        brand = await self.bot.brand(i.guild_id)
        rows = [(t["name"], str(t["id"]), f"{'✅ ' if t['verified'] else ''}{t.get('game') or 'Any game'}", team_line(t)) for t in teams]

        async def picked(inter: discord.Interaction, value: str) -> None:
            await self.team_detail(inter, int(value), back_handler=lambda x: self._team_list(x, teams, section, empty, back=back))

        async def invites(x: discord.Interaction) -> None:
            await self.my_invites(x)
        extra = [ActionButton("My Invites", invites, emoji="✉️", row=2)]
        await PickList(self.bot, i.user.id, brand, section, rows, picked, placeholder="Open a team…", back=back, empty=empty, extra=extra).start(i)

    async def team_detail(self, i: discord.Interaction, tid: int, *, back_handler) -> None:
        def q(c):
            t = get_team(c, tid)
            if not t or t["disbanded"]:
                return None
            return t, team_members(c, tid), team_stats(c, tid), team_history(c, tid), team_of_user(c, i.guild_id, i.user.id)
        data = await self.bot.db.read(q)
        if not data:
            await say(i, "That team no longer exists.", "error", view=None)
            return
        team, members, stats, history, mine = data
        perms = await self.bot.perms(i)
        brand = await self.bot.brand(i.guild_id)
        view = BaseView(self.bot, i.user.id)
        if mine is None and team["recruiting"]:
            view.add_item(ActionButton("Request to Join", lambda x: self._join_request(x, tid, "request"), emoji="📥", style=PRIMARY, row=0))
        elif mine is not None and mine["id"] != tid and mine["my_role"] != "captain" and team["recruiting"]:
            view.add_item(ActionButton("Request Transfer", lambda x: self._join_request(x, tid, "transfer"), emoji="🔁", row=0))
        if can_verify_team(perms):
            if team["verified"]:
                view.add_item(ActionButton("Remove Verification", lambda x: self._verify(x, tid, False, back_handler), emoji="❎", row=1))
            else:
                view.add_item(ActionButton("Verify Team", lambda x: self._verify(x, tid, True, back_handler), emoji="✅", style=SUCCESS, row=1))
        if perms.is_admin:
            view.add_item(ActionButton("Force Disband (admin)", lambda x: self._admin_disband(x, tid, back_handler), emoji="🗑️", style=DANGER, row=1))
        add_nav(view, back_handler)
        await show(i, team_embed(brand, team, members, stats, history), view)

    async def _join_request(self, i: discord.Interaction, tid: int, kind: str) -> None:
        await self.bot.db.run(lambda c: create_invite(c, team_id=tid, actor_id=i.user.id, target_id=i.user.id, kind=kind))
        team = await self.bot.db.read(lambda c: get_team(c, tid))
        await self.bot.dm(team["captain_id"], content=f"<@{i.user.id}> asked to {'transfer to' if kind == 'transfer' else 'join'} **{team['name']}**. Review it under My Team → Join Requests.")
        await say(i, f"Request sent to **{team['name']}**. You'll be notified when the captain answers.", "ok", view=self._back_to_team())

    async def _verify(self, i: discord.Interaction, tid: int, flag: bool, back_handler) -> None:
        if not await self.bot.require(i, can_verify_team, "Only staff can verify teams."):
            return
        await self.bot.db.run(lambda c: set_verified(c, tid, i.user.id, flag))
        await self.team_detail(i, tid, back_handler=back_handler)

    async def _admin_disband(self, i: discord.Interaction, tid: int, back_handler) -> None:
        if not await self.bot.require(i, lambda p: p.is_admin, "Administrators only."):
            return

        async def yes(x: discord.Interaction) -> None:
            await self.bot.db.run(lambda c: disband_team(c, tid, x.user.id, admin=True))
            await say(x, "Team disbanded.", "ok", view=None)
        await confirm(i, self.bot, "Force-disband this team? Members are removed and registrations withdrawn.", yes, yes_label="Disband", on_no=lambda x: self.team_detail(x, tid, back_handler=back_handler))

# --------------------------------------------------------------------------------------------------
# cogs/players.py
# Player profiles, discovery, history and statistics.
# --------------------------------------------------------------------------------------------------

class Players(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for name, fn in {"player.profile": self.profile, "player.find": self.find, "player.history": self.history,
                         "player.stats": self.stats, "player.events": self.events}.items():
            self.bot.register_entry(name, fn)

    # ------------------------------------------------------------------ own profile
    async def profile(self, i: discord.Interaction) -> None:
        gid, uid = i.guild_id, i.user.id

        def q(c):
            p = player_get(c, gid, uid)
            return (p, team_of_user(c, gid, uid), badges_for_user(c, gid, uid),
                    len(event_history(c, gid, uid, 100)))
        p, team, badges, n = await self.bot.db.read(q)
        brand = await self.bot.brand(gid)
        view = BaseView(self.bot, uid)
        if p is None:
            view.add_item(ActionButton("Create Profile", self.edit_modal, emoji="📝", style=PRIMARY, row=0))
            view.add_item(ActionButton("Find Team", lambda x: self.bot.open(x, "team.find"), emoji="🔎", row=0))
            add_nav(view)
            await show(i, profile_embed(brand, None, i.user, None, []), view)
            return
        view.add_item(ActionButton("Edit Profile", self.edit_modal, emoji="✏️", style=PRIMARY, row=0))
        view.add_item(ActionButton("Achievements", self.achievements_modal, emoji="🏆", row=0))
        view.add_item(ActionButton("Match History", self.history, emoji="📜", row=0))
        view.add_item(ActionButton("My Statistics", self.stats, emoji="📊", row=0))
        games = await self.bot.db.read(lambda c: get_games(c, gid))
        view.add_item(ChoiceSelect([(g, g, None) for g in games[:25]], self._set_games, placeholder="Set my games (replaces current list)",
                                   max_values=min(5, len(games)), row=1))
        view.add_item(ChoiceSelect([(label, key, None) for key, label in STATUSES.items()], self._set_status,
                                   placeholder=f"Status: {STATUSES[p['status']]}", row=2))
        view.add_item(ActionButton("Find Players", self.find, emoji="👥", row=3))
        view.add_item(ActionButton("Find Team", lambda x: self.bot.open(x, "team.find"), emoji="🔎", row=3))
        view.add_item(ActionButton("My Invites", lambda x: self.bot.open(x, "team.invites"), emoji="✉️", row=3))
        add_nav(view)
        await show(i, profile_embed(brand, p, i.user, team, badges, n), view)

    async def edit_modal(self, i: discord.Interaction) -> None:
        p = await self.bot.db.read(lambda c: player_get(c, i.guild_id, i.user.id))

        async def submit(inter: discord.Interaction, f: dict[str, str]) -> None:
            ign = clean(f["ign"], field="In-game name", min_len=2, max_len=32)
            uid_text = optional(f["uid"], field="Game UID", max_len=24)
            if uid_text and not uid_text.replace("-", "").isalnum():
                raise ValidationError("Game UID may contain only letters, numbers and dashes.")
            role = optional(f["role"], field="Role", max_len=30)
            region = optional(f["region"], field="Region", max_len=30)
            language = optional(f["language"], field="Language", max_len=30)
            existing = p["achievements"] if p else None
            await self.bot.db.run(lambda c: upsert(c, inter.guild_id, inter.user.id, ign=ign, game_uid=uid_text, role=role,
                                                                  region=region, language=language, achievements=existing))
            await self.profile(inter)
        await i.response.send_modal(FormModal("Player Profile", [
            Field("ign", "In-game name", "Your name in game", max_length=32, min_length=2, default=p["ign"] if p else None),
            Field("uid", "Game UID (optional)", "Numbers/letters only", required=False, max_length=24, default=p["game_uid"] if p else None),
            Field("role", "Role (optional)", "IGL, Assaulter, Sniper, Support…", required=False, max_length=30, default=p["role"] if p else None),
            Field("region", "Region (optional)", "e.g. India - West", required=False, max_length=30, default=p["region"] if p else None),
            Field("language", "Language (optional)", "e.g. English, Hindi", required=False, max_length=30, default=p["language"] if p else None)], submit))

    async def achievements_modal(self, i: discord.Interaction) -> None:
        p = await self.bot.db.read(lambda c: player_get(c, i.guild_id, i.user.id))
        if not p:
            raise ServiceError("Create your profile first.")

        async def submit(inter: discord.Interaction, f: dict[str, str]) -> None:
            text = optional(f["text"], field="Achievements", max_len=800, multiline=True)
            await self.bot.db.run(lambda c: c.execute("UPDATE player_profiles SET achievements=? WHERE guild_id=? AND user_id=?", (text, inter.guild_id, inter.user.id)))
            await self.profile(inter)
        await i.response.send_modal(FormModal("Achievements (self-reported)", [
            Field("text", "Your achievements", "Tournaments won, ranks, etc.", required=False, paragraph=True, max_length=800, default=p["achievements"])], submit))

    async def _set_games(self, i: discord.Interaction, values: list[str]) -> None:
        await self.bot.db.run(lambda c: set_games(c, i.guild_id, i.user.id, values))
        await self.profile(i)

    async def _set_status(self, i: discord.Interaction, values: list[str]) -> None:
        await self.bot.db.run(lambda c: player_set_status(c, i.guild_id, i.user.id, values[0]))
        await self.profile(i)

    # ------------------------------------------------------------------ discovery
    async def find(self, i: discord.Interaction, *, game: str | None = None, lft: bool = True) -> None:
        profiles = await self.bot.db.read(lambda c: search(c, i.guild_id, game=game, lft_only=lft, exclude_user=i.user.id))
        games = await self.bot.db.read(lambda c: get_games(c, i.guild_id))
        brand = await self.bot.brand(i.guild_id)
        mine = await self.bot.db.read(lambda c: team_of_user(c, i.guild_id, i.user.id))
        rows = []
        for p in profiles:
            g = ", ".join(json.loads(p["games"] or "[]")) or "no games set"
            tag = "🔎 LFT" if p["status"] == "LFT" else ("👥 in team" if p["has_team"] else "🟢 free")
            rows.append((p["ign"], str(p["user_id"]), f"{p['role'] or '—'} • {g}"[:100], f"{tag} • **{p['ign']}** <@{p['user_id']}> • {p['role'] or '—'} • {g}"))

        async def picked(inter: discord.Interaction, value: str) -> None:
            await self.player_detail(inter, int(value), mine, lambda x: self.find(x, game=game, lft=lft))

        async def set_game(inter: discord.Interaction, vals: list[str]) -> None:
            await self.find(inter, game=None if vals[0] == "__any__" else vals[0], lft=lft)

        async def toggle(inter: discord.Interaction) -> None:
            await self.find(inter, game=game, lft=not lft)
        extra: list[discord.ui.Item] = [
            ChoiceSelect([("Any game", "__any__", None)] + [(g, g, None) for g in games[:24]], set_game, placeholder=f"Game: {game or 'any'}", row=2),
            ActionButton("Showing: looking-for-team only" if lft else "Showing: all active players", toggle, emoji="🔀", row=3)]
        header = f"Filter: **{game or 'any game'}** • {'looking for a team' if lft else 'all active players'}"
        await PickList(self.bot, i.user.id, brand, "FIND PLAYERS", rows, picked, placeholder="Open a player…", back=self.profile, header=header,
                       empty="No players match this filter. Try another game or show all active players.", extra=extra).start(i)

    async def player_detail(self, i: discord.Interaction, user_id: int, mine: dict | None, back) -> None:
        def q(c):
            return (player_get(c, i.guild_id, user_id), team_of_user(c, i.guild_id, user_id),
                    badges_for_user(c, i.guild_id, user_id), len(event_history(c, i.guild_id, user_id, 100)))
        p, team, badges, n = await self.bot.db.read(q)
        if not p:
            raise ServiceError("That profile no longer exists.")
        brand = await self.bot.brand(i.guild_id)
        user = i.guild.get_member(user_id) if i.guild else None
        view = BaseView(self.bot, i.user.id)
        if mine and mine["my_role"] in MANAGERS and team is None and user_id != i.user.id:
            async def invite(x: discord.Interaction) -> None:
                await self.bot.db.run(lambda c: create_invite(c, team_id=mine["id"], actor_id=x.user.id, target_id=user_id, kind="invite"))
                await self.bot.dm(user_id, content=f"You were invited to join **{mine['name']}** in **{x.guild.name}**. Open the AEGIS panel → Find Team → My Invites.")
                await self.player_detail(x, user_id, mine, back)
            view.add_item(ActionButton(f"Invite to {mine['name'][:30]}", invite, emoji="➕", style=PRIMARY))
        add_nav(view, back)
        await show(i, profile_embed(brand, p, user, team, badges, n), view)

    # ------------------------------------------------------------------ history / statistics
    async def history(self, i: discord.Interaction) -> None:
        rows = await self.bot.db.read(lambda c: user_match_history(c, i.guild_id, i.user.id, 60))
        brand = await self.bot.brand(i.guild_id)
        lines = []
        for r in rows:
            res = (f"#{r['placement']} • {r['kills']} kills" if r["placement"] is not None else f"{r['score_for']}-{r['score_against']}")
            lines.append(f"**{trunc(r['event_title'], 40)}** M{r['number']} • {r['team_name']} • {res}")
        pages = list_pages(brand, "MATCH HISTORY", lines, empty="No approved results yet. Results appear here once a referee approves them.")
        await Pager(self.bot, i.user.id, pages, back=self.profile).start(i)

    async def stats(self, i: discord.Interaction) -> None:
        def q(c):
            rows = user_match_history(c, i.guild_id, i.user.id, 500)
            return rows, event_history(c, i.guild_id, i.user.id, 500)
        rows, events = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        wins = sum(1 for r in rows if r["placement"] == 1 or (r["score_for"] is not None and r["score_for"] > (r["score_against"] or 0)))
        kills = sum(r["kills"] for r in rows)
        e = base(brand, "MY STATISTICS", "Team-level results from **referee-approved** matches you were rostered for. "
                                              "Individual kills are not tracked, so none are shown.")
        e.add_field(name="Events entered", value=str(len(events)))
        e.add_field(name="Approved matches", value=str(len(rows)))
        e.add_field(name="Team wins", value=str(wins))
        e.add_field(name="Team kills in those matches", value=str(kills))
        view = BaseView(self.bot, i.user.id)
        view.add_item(ActionButton("Match History", self.history, emoji="📜"))
        view.add_item(ActionButton("Event History", self.events, emoji="🏆"))
        add_nav(view, self.profile)
        await show(i, e, view)

    async def events(self, i: discord.Interaction) -> None:
        rows = await self.bot.db.read(lambda c: event_history(c, i.guild_id, i.user.id, 60))
        brand = await self.bot.brand(i.guild_id)
        lines = [f"{r['kind']}: **{trunc(r['title'], 40)}** • {r['team_name']} • {REG_ICON.get(r['status'], r['status'])}" + (f" • rank {r['rank']}" if r["rank"] else "")
                 for r in rows]
        await Pager(self.bot, i.user.id, list_pages(brand, "EVENT HISTORY", lines, empty="You have not been on a registered roster yet."),
                    back=self.stats).start(i)

    # ------------------------------------------------------------------ slash
    @app_commands.command(name="profile", description="View a player's AEGIS esports profile")
    @app_commands.guild_only()
    async def profile_cmd(self, i: discord.Interaction, member: discord.Member | None = None) -> None:
        target = member or i.user
        brand = await self.bot.brand(i.guild_id)

        def q(c):
            return (player_get(c, i.guild_id, target.id), team_of_user(c, i.guild_id, target.id),
                    badges_for_user(c, i.guild_id, target.id), len(event_history(c, i.guild_id, target.id, 100)))
        p, team, badges, n = await self.bot.db.read(q)
        if p is None and target.id != i.user.id:
            raise ServiceError(f"{target.display_name} has no player profile yet.")
        await i.response.send_message(embed=profile_embed(brand, p, target, team, badges, n), ephemeral=True)

# --------------------------------------------------------------------------------------------------
# cogs/tournaments.py
# Tournaments: browse, details, create, organizer management, matches and room credentials.
# --------------------------------------------------------------------------------------------------

MODES = {"solo": "solo", "duo": "duo", "squad": "squad", "1": "solo", "2": "duo", "4": "squad"}


class Tournaments(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for name, fn in {"tournaments": self.hub, "tour.browse": self.browse, "tour.create": self.create, "tour.manage": self.manage_list}.items():
            self.bot.register_entry(name, fn)
        self.bot.register_event_handler("t", self.event_action)

    async def _offset(self, gid: int) -> int:
        s = await self.bot.settings(gid)
        return parse_offset(s.get("utc_offset", "+05:30"))

    # ------------------------------------------------------------------ hub / browse
    async def hub(self, i: discord.Interaction) -> None:
        perms = await self.bot.perms(i)
        brand = await self.bot.brand(i.guild_id)
        o = self.bot.open
        items = [("Browse Tournaments", "🏆", self.browse, PRIMARY), ("Register Team", "📝", lambda x: o(x, "reg.start"), SECONDARY),
                 ("My Registrations", "📋", lambda x: o(x, "reg.mine"), SECONDARY), ("Check In", "✅", lambda x: o(x, "reg.checkin"), SECONDARY),
                 ("Match Schedule", "🗓️", lambda x: o(x, "sched.mine"), SECONDARY), ("Submit Result", "📤", lambda x: o(x, "result.submit"), SECONDARY),
                 ("View Standings", "📊", lambda x: o(x, "lb.tournaments"), SECONDARY)]
        if perms.is_organizer:
            items += [("Create Tournament", "➕", self.create, SUCCESS), ("Edit / Manage", "🛠️", self.manage_list, PRIMARY)]
        await show(i, base(brand, "TOURNAMENTS", "Browse events, register your team, check in and follow standings."), MenuView(self.bot, i.user.id, items))

    async def browse(self, i: discord.Interaction) -> None:
        def q(c):
            wl = get_bool(c, i.guild_id, "waitlist")
            out = []
            for t in tournament_list_for_guild(c, i.guild_id, ("PUBLISHED", "LIVE", "COMPLETED"), limit=60):
                cnt = registration_counts(c, "tournament", t["id"])
                out.append((t, cnt, registration_state("tournament", t, cnt, wl)))
            return out
        data = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"#{t['id']} {t['title']}", str(t["id"]), f"{t['game']} • {REG_STATE.get(st, st)}"[:100],
                 f"{event_line('tournament', t)}\n┗ {cnt.get('PENDING', 0) + cnt.get('APPROVED', 0)}/{t['max_teams']} teams • {REG_STATE.get(st, st)}")
                for t, cnt, st in data]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.detail(x, int(value), back=self.browse)
        await PickList(self.bot, i.user.id, brand, "TOURNAMENTS", rows, picked, placeholder="Open a tournament…", back=self.hub, per_page=6,
                       empty="No tournaments are published yet.", refresh=self.browse).start(i)

    async def event_action(self, i: discord.Interaction, action: str, tid: int) -> None:
        if action == "view":
            await self.detail(i, tid, back=None)
        elif action == "reg":
            await self.bot.get_cog("Registrations").register_flow(i, "tournament", tid)  # type: ignore[union-attr]
        elif action == "rules":
            await self.rules(i, tid)
        elif action == "mine":
            await self.bot.get_cog("Registrations").my_registration(i, "tournament", tid)  # type: ignore[union-attr]

    async def detail(self, i: discord.Interaction, tid: int, *, back) -> None:
        def q(c):
            t = tournament_get(c, tid)
            cnt = registration_counts(c, "tournament", tid)
            return t, cnt, registration_state("tournament", t, cnt, get_bool(c, t["guild_id"], "waitlist"))
        t, cnt, state = await self.bot.db.read(q)
        if t["guild_id"] != i.guild_id or t["status"] in ("DRAFT", "SCHEDULED"):
            perms = await self.bot.perms(i)
            if t["guild_id"] != i.guild_id or not can_manage_event(perms, i.user.id, t["organizer_id"], "tournament"):
                raise ServiceError("That tournament is not published.")
        perms = await self.bot.perms(i)
        brand = await self.bot.brand(i.guild_id)
        view = BaseView(self.bot, i.user.id)
        reg = lambda x: self.bot.get_cog("Registrations").register_flow(x, "tournament", tid)  # noqa: E731
        view.add_item(ActionButton("Register Team", reg, emoji="📝", style=SUCCESS, disabled=state not in ("OPEN", "WAITLIST"), row=0))
        view.add_item(ActionButton("Rules", lambda x: self.rules(x, tid), emoji="📜", row=0))
        view.add_item(ActionButton("My Registration", lambda x: self.bot.get_cog("Registrations").my_registration(x, "tournament", tid), emoji="🎫", row=0))
        view.add_item(ActionButton("Standings", lambda x: self.bot.get_cog("Leaderboards").show_event(x, "tournament", tid, lambda y: self.detail(y, tid, back=back)), emoji="📊", row=1))
        view.add_item(ActionButton("Matches", lambda x: self.public_matches(x, tid, back), emoji="🗓️", row=1))
        if can_manage_event(perms, i.user.id, t["organizer_id"], "tournament"):
            view.add_item(ActionButton("Manage", lambda x: self.manage(x, tid), emoji="🛠️", style=PRIMARY, row=1))
        add_nav(view, back)
        await show(i, tournament_embed(brand, t, cnt, state), view)

    async def rules(self, i: discord.Interaction, tid: int) -> None:
        t = await self.bot.db.read(lambda c: tournament_get(c, tid))
        brand = await self.bot.brand(i.guild_id)
        e = base(brand, "RULES", f"**{t['title']}**\n\n{trunc(t['rules'], 3800) if t['rules'] else 'No rules have been posted yet. Ask the organizer.'}")
        view = BaseView(self.bot, i.user.id)
        view.add_item(ActionButton("Back to tournament", lambda x: self.detail(x, tid, back=self.browse), emoji="🏆"))
        add_nav(view)
        await show(i, e, view)

    async def public_matches(self, i: discord.Interaction, tid: int, back) -> None:
        t, matches = await self.bot.db.read(lambda c: (tournament_get(c, tid), list_matches(c, "tournament", tid)))
        brand = await self.bot.brand(i.guild_id)
        lines = [f"**{match_label(m)}** • {ts(m['scheduled_at'], 'f') if m['scheduled_at'] else 'time TBA'} • "
                 f"{'✅ done' if m['status'] == 'COMPLETED' else '⛔ cancelled' if m['status'] == 'CANCELLED' else '⏳ scheduled'}" for m in matches]
        pages = list_pages(brand, f"MATCHES • {trunc(t['title'], 40)}", lines, empty="No matches have been scheduled yet.")
        view = BaseView(self.bot, i.user.id)
        add_nav(view, lambda x: self.detail(x, tid, back=back))
        await show(i, pages[0], view)

    # ------------------------------------------------------------------ create
    async def create(self, i: discord.Interaction) -> None:
        if not await self.bot.require(i, lambda p: can_create_event(p, "tournament"), "Only tournament organizers can create tournaments."):
            return
        s = await self.bot.settings(i.guild_id)
        games = await self.bot.db.read(lambda c: get_games(c, i.guild_id))
        offset = parse_offset(s.get("utc_offset", "+05:30"))

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not can_create_event(await self.bot.perms(x), "tournament"):
                raise ServiceError("Only tournament organizers can create tournaments.")
            title = clean(f["title"], field="Title", min_len=3, max_len=80)
            game = resolve_game(games, f["game"])
            mode = parse_choice(f["mode"], field="Mode", choices=MODES, default="squad")
            start = parse_local(f["start"], offset, field="Start time")
            map_name = optional(f["map"], field="Map", max_len=40)
            tid = await self.bot.db.run(lambda c: tournament_create_draft(c, x.guild_id, x.user.id, title=title, game=game, mode=mode, map_name=map_name, start_time=start))
            await self.manage(x, tid)
        await i.response.send_modal(FormModal("Create Tournament", [
            Field("title", "Title", "e.g. Weekend Cup #1", max_length=80, min_length=3),
            Field("game", "Game", games[0], max_length=30, default=games[0]),
            Field("mode", "Mode (solo / duo / squad)", "squad", max_length=5, default="squad"),
            Field("map", "Map (optional)", "Erangel", required=False, max_length=40),
            Field("start", f"Start (DD-MM-YYYY HH:MM, UTC{s.get('utc_offset', '+05:30')})", "25-12-2026 21:00", max_length=20)], submit))

    # ------------------------------------------------------------------ manage list / panel
    async def manage_list(self, i: discord.Interaction) -> None:
        perms = await self.bot.perms(i)
        if not perms.is_organizer:
            await say(i, "Only tournament organizers can manage tournaments.", "error")
            return
        rows_db = await self.bot.db.read(lambda c: tournament_list_for_guild(c, i.guild_id, None, None if perms.is_admin else i.user.id, 60))
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"#{t['id']} {t['title']}", str(t["id"]), f"{t['status'].title()} • {t['game']}", event_line("tournament", t)) for t in rows_db]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.manage(x, int(value))
        extra = [ActionButton("Create Tournament", self.create, emoji="➕", style=SUCCESS, row=2)]
        await PickList(self.bot, i.user.id, brand, "MANAGE TOURNAMENTS", rows, picked, placeholder="Open a tournament…", back=self.hub, per_page=8,
                       header="All tournaments" if perms.is_admin else "Your tournaments", empty="Nothing to manage yet - create your first tournament.",
                       extra=extra, refresh=self.manage_list).start(i)

    async def manage(self, i: discord.Interaction, tid: int) -> None:
        t = await self.bot.managed_event(i, "tournament", tid)

        def q(c):
            cnt = registration_counts(c, "tournament", tid)
            return cnt, registration_state("tournament", t, cnt, get_bool(c, t["guild_id"], "waitlist")), len(list_matches(c, "tournament", tid))
        cnt, state, n_matches = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        e = tournament_embed(brand, t, cnt, state)
        e.add_field(name="Pending approvals", value=str(cnt.get("PENDING", 0)))
        e.add_field(name="Matches", value=str(n_matches))
        e.add_field(name="Publish at", value=ts(t["publish_at"]) if t["publish_at"] else "—")
        st = t["status"]
        locked = st in ("COMPLETED", "CANCELLED")
        view = BaseView(self.bot, i.user.id)
        view.add_item(ActionButton("Edit Details", lambda x: self.edit_details(x, tid), emoji="✏️", disabled=locked, row=0))
        view.add_item(ActionButton("Edit Settings", lambda x: self.edit_settings(x, tid), emoji="⚙️", disabled=locked, row=0))
        view.add_item(ActionButton("Edit Schedule", lambda x: self.edit_schedule(x, tid), emoji="🕒", disabled=locked, row=0))
        view.add_item(ActionButton("Rules & Info", lambda x: self.edit_rules(x, tid), emoji="📜", disabled=locked, row=0))
        if st in ("DRAFT", "SCHEDULED"):
            view.add_item(ActionButton("Publish", lambda x: self.publish(x, tid), emoji="🚀", style=SUCCESS, row=1))
        if st in ("PUBLISHED", "SCHEDULED"):
            view.add_item(ActionButton("Unpublish", lambda x: self.unpublish(x, tid), emoji="📥", row=1))
        if st == "PUBLISHED":
            view.add_item(ActionButton("Go Live", lambda x: self.set_status(x, tid, "LIVE"), emoji="🔴", style=PRIMARY, row=1))
        if st in ("PUBLISHED", "LIVE"):
            view.add_item(ActionButton("Complete", lambda x: self.set_status(x, tid, "COMPLETED"), emoji="🏁", row=1))
        if not locked:
            view.add_item(ActionButton("Cancel Event", lambda x: self.set_status(x, tid, "CANCELLED"), emoji="⛔", style=DANGER, row=1))
        view.add_item(ActionButton("Participants", lambda x: self.bot.get_cog("Registrations").participants(x, "tournament", tid, lambda y: self.manage(y, tid)), emoji="👥", style=PRIMARY, row=2))
        view.add_item(ActionButton("Matches & Rooms", lambda x: self.matches(x, tid), emoji="🎮", style=PRIMARY, row=2))
        view.add_item(ActionButton("Scoring Rule", lambda x: self.pick_rule(x, tid), emoji="🧮", disabled=locked, row=2))
        view.add_item(ActionButton("Review Results", lambda x: self.bot.get_cog("Matches").review_list(x, "tournament", tid, lambda y: self.manage(y, tid)), emoji="🧾", row=2))
        if st in ("PUBLISHED", "LIVE"):
            view.add_item(ActionButton("Re-post announcement", lambda x: self.repost(x, tid), emoji="📣", row=3))
        view.add_item(ActionButton("Delete", lambda x: self.delete(x, tid), emoji="🗑️", style=DANGER, row=3))
        view.add_item(ActionButton("Refresh", lambda x: self.manage(x, tid), emoji="🔄", row=3))
        add_nav(view, self.manage_list)
        await show(i, e, view)

    # ------------------------------------------------------------------ edit modals (permission re-checked on submit)
    async def edit_details(self, i: discord.Interaction, tid: int) -> None:
        t = await self.bot.managed_event(i, "tournament", tid)
        games = await self.bot.db.read(lambda c: get_games(c, i.guild_id))
        off = await self._offset(i.guild_id)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            await self.bot.managed_event(x, "tournament", tid)
            fields = dict(title=clean(f["title"], field="Title", min_len=3, max_len=80), game=resolve_game(games, f["game"]),
                          map=optional(f["map"], field="Map", max_len=40), region=optional(f["region"], field="Region", max_len=30),
                          start_time=parse_local(f["start"], off, field="Start time"))
            new = await self.bot.db.run(lambda c: tournament_update(c, tid, x.user.id, **fields))
            if new["start_time"] != t["start_time"]:
                await self._notify_captains(x, tid, f"⏰ **{new['title']}** was rescheduled to {ts(new['start_time'], 'F')}.")
            await refresh_event_message(self.bot, "tournament", tid)
            await self.manage(x, tid)
        await i.response.send_modal(FormModal("Edit Details", [
            Field("title", "Title", max_length=80, default=t["title"]), Field("game", "Game", max_length=30, default=t["game"]),
            Field("map", "Map", required=False, max_length=40, default=t["map"]), Field("region", "Region", required=False, max_length=30, default=t["region"]),
            Field("start", "Start (DD-MM-YYYY HH:MM)", max_length=20, default=to_local_text(t["start_time"], off))], submit))

    async def edit_settings(self, i: discord.Interaction, tid: int) -> None:
        t = await self.bot.managed_event(i, "tournament", tid)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            await self.bot.managed_event(x, "tournament", tid)
            fields = dict(mode=parse_choice(f["mode"], field="Mode", choices=MODES), max_teams=parse_int(f["max"], field="Max teams", lo=2, hi=512),
                          reserve_slots=parse_int(f["reserve"], field="Reserve slots", lo=0, hi=100, default=0),
                          entry_fee=optional(f["fee"], field="Entry fee", max_len=40), prize=optional(f["prize"], field="Prize pool", max_len=200))
            await self.bot.db.run(lambda c: tournament_update(c, tid, x.user.id, **fields))
            await refresh_event_message(self.bot, "tournament", tid)
            await self.manage(x, tid)
        await i.response.send_modal(FormModal("Edit Settings", [
            Field("mode", "Mode (solo / duo / squad)", max_length=5, default=t["mode"]), Field("max", "Max teams", max_length=3, default=str(t["max_teams"])),
            Field("reserve", "Reserve / waitlist slots", max_length=3, default=str(t["reserve_slots"])),
            Field("fee", "Entry fee (blank = free)", "e.g. ₹50 per team", required=False, max_length=40, default=t["entry_fee"]),
            Field("prize", "Prize pool (text)", "e.g. ₹5,000 + merch", required=False, max_length=200, default=t["prize"])], submit))

    async def edit_schedule(self, i: discord.Interaction, tid: int) -> None:
        t = await self.bot.managed_event(i, "tournament", tid)
        off = await self._offset(i.guild_id)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            await self.bot.managed_event(x, "tournament", tid)
            fields = dict(reg_open=parse_local(f["open"], off, field="Registration opens", required=False),
                          reg_close=parse_local(f["close"], off, field="Registration closes", required=False),
                          publish_at=parse_local(f["publish"], off, field="Publish at", required=False))
            await self.bot.db.run(lambda c: tournament_update(c, tid, x.user.id, **fields))
            await refresh_event_message(self.bot, "tournament", tid)
            await self.manage(x, tid)
        await i.response.send_modal(FormModal("Edit Schedule (blank clears)", [
            Field("open", "Registration opens", "DD-MM-YYYY HH:MM", required=False, max_length=20, default=to_local_text(t["reg_open"], off)),
            Field("close", "Registration closes", "DD-MM-YYYY HH:MM", required=False, max_length=20, default=to_local_text(t["reg_close"], off)),
            Field("publish", "Auto-publish at (scheduled tournaments)", "DD-MM-YYYY HH:MM", required=False, max_length=20, default=to_local_text(t["publish_at"], off))], submit))

    async def edit_rules(self, i: discord.Interaction, tid: int) -> None:
        t = await self.bot.managed_event(i, "tournament", tid)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            await self.bot.managed_event(x, "tournament", tid)
            fields = dict(rules=optional(f["rules"], field="Rules", max_len=1800, multiline=True), poster_url=validate_url(f["poster"], field="Poster URL"),
                          organizer_info=optional(f["info"], field="Organizer info", max_len=200))
            await self.bot.db.run(lambda c: tournament_update(c, tid, x.user.id, **fields))
            await refresh_event_message(self.bot, "tournament", tid)
            await self.manage(x, tid)
        await i.response.send_modal(FormModal("Rules & Info", [
            Field("rules", "Rules & tournament policy", paragraph=True, required=False, max_length=1800, default=t["rules"]),
            Field("poster", "Poster / image URL", "https://...", required=False, max_length=500, default=t["poster_url"]),
            Field("info", "Organizer info", "Contact, Discord, socials", required=False, max_length=200, default=t["organizer_info"])], submit))

    # ------------------------------------------------------------------ lifecycle
    async def publish(self, i: discord.Interaction, tid: int) -> None:
        await self.bot.managed_event(i, "tournament", tid)
        await i.response.defer()  # posting to a channel can take a moment
        status = await self.bot.db.run(lambda c: publish(c, tid, i.user.id))
        note = ""
        if status == "PUBLISHED":
            msg = await publish_event_message(self.bot, i.guild, "tournament", tid)
            note = f"Announcement posted in {msg.channel.mention}." if msg else "⚠️ No usable announcement channel is set (Admin → Channels). Players can still find it under Tournaments → Browse."
        else:
            note = "Scheduled - it will publish automatically at the chosen time."
        await i.edit_original_response(embed=notice(f"Tournament is now **{status.lower()}**. {note}", "ok" if "⚠️" not in note else "warn"),
                                       view=self._back_view(tid))

    def _back_view(self, tid: int) -> BaseView:
        view = BaseView(self.bot, None)
        view.add_item(ActionButton("Back to tournament", lambda x: self.manage(x, tid), emoji="🛠️", style=PRIMARY))
        return view

    async def unpublish(self, i: discord.Interaction, tid: int) -> None:
        await self.bot.managed_event(i, "tournament", tid)
        await self.bot.db.run(lambda c: unpublish(c, tid, i.user.id))
        await delete_event_message(self.bot, i.guild, "tournament", tid)
        await self.manage(i, tid)

    async def set_status(self, i: discord.Interaction, tid: int, status: str) -> None:
        await self.bot.managed_event(i, "tournament", tid)

        async def apply(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, "tournament", tid)
            await self.bot.db.run(lambda c: tournament_set_status(c, tid, x.user.id, status))
            if status == "CANCELLED":
                await self._notify_captains(x, tid, "⛔ This tournament was cancelled by the organizer.")
            await refresh_event_message(self.bot, "tournament", tid)
            await self.manage(x, tid)
        if status in ("CANCELLED", "COMPLETED"):
            await confirm(i, self.bot, f"{'Cancel' if status == 'CANCELLED' else 'Mark as completed'}? This cannot be undone and the tournament becomes read-only.", apply,
                          yes_label="Yes, continue", on_no=lambda x: self.manage(x, tid))
        else:
            await apply(i)

    async def repost(self, i: discord.Interaction, tid: int) -> None:
        await self.bot.managed_event(i, "tournament", tid)
        await i.response.defer()
        msg = await publish_event_message(self.bot, i.guild, "tournament", tid)
        await i.edit_original_response(embed=notice(f"Announcement re-posted in {msg.channel.mention}." if msg else "No usable announcement channel is configured.", "ok" if msg else "warn"),
                                       view=self._back_view(tid))

    async def delete(self, i: discord.Interaction, tid: int) -> None:
        await self.bot.managed_event(i, "tournament", tid)

        async def yes(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, "tournament", tid)
            await delete_event_message(self.bot, x.guild, "tournament", tid)
            await self.bot.db.run(lambda c: tournament_delete(c, tid, x.user.id))
            await say(x, "Tournament deleted.", "ok", view=self._list_view())
        await confirm(i, self.bot, "**Delete this tournament permanently?** Registrations, matches and results for it are removed. Consider **Cancel Event** to keep history.", yes,
                      yes_label="Delete forever", on_no=lambda x: self.manage(x, tid))

    def _list_view(self) -> BaseView:
        view = BaseView(self.bot, None)
        view.add_item(ActionButton("Back to list", self.manage_list, emoji="📋"))
        return view

    async def _notify_captains(self, i: discord.Interaction, tid: int, text: str) -> None:
        caps = await self.bot.db.read(lambda c: [r["captain_id"] for r in list_regs(c, "tournament", tid, ("PENDING", "APPROVED", "WAITLIST"))])
        t = await self.bot.db.read(lambda c: tournament_get(c, tid))
        for cid in caps:
            await self.bot.dm(cid, content=f"{text}\n**{t['title']}**")

    async def pick_rule(self, i: discord.Interaction, tid: int) -> None:
        await self.bot.managed_event(i, "tournament", tid)
        rules = await self.bot.db.read(lambda c: list_rules(c, i.guild_id))
        t = await self.bot.db.read(lambda c: tournament_get(c, tid))
        brand = await self.bot.brand(i.guild_id)

        async def chosen(x: discord.Interaction, vals: list[str]) -> None:
            await self.bot.managed_event(x, "tournament", tid)

            def go(c):
                tournament_update(c, tid, x.user.id, score_rule_id=int(vals[0]))
                recalc_standings(c, x.guild_id, "tournament", tid)
            await self.bot.db.run(go)
            await self.manage(x, tid)
        view = BaseView(self.bot, i.user.id)
        view.add_item(ChoiceSelect([(r["name"], str(r["id"]), f"{r['kind']} rule") for r in rules], chosen, placeholder="Choose the scoring rule", row=0))
        add_nav(view, lambda x: self.manage(x, tid))
        current = next((r["name"] for r in rules if r["id"] == t["score_rule_id"]), "server default")
        await show(i, base(brand, "SCORING RULE", f"Current: **{current}**\nChanging the rule recalculates standings from approved results. Rules are edited in **Admin → Scoring**."), view)

    # ------------------------------------------------------------------ matches & rooms (shared with scrims via `kind`)
    async def matches(self, i: discord.Interaction, tid: int, kind: str = "tournament", back=None) -> None:
        back = back or (lambda x: self.manage(x, tid))
        t = await self.bot.managed_event(i, kind, tid)
        ms = await self.bot.db.read(lambda c: list_matches(c, kind, tid))
        brand = await self.bot.brand(i.guild_id)
        rows = [(match_label(m), str(m["id"]), f"{m['status'].title()} • room {'released' if m['room_released'] else 'hidden'}",
                 f"**{match_label(m)}** • {ts(m['scheduled_at'], 'f') if m['scheduled_at'] else 'time TBA'} • "
                 f"{'🔓 released' if m['room_released'] else '🔒 hidden' if m['room_id'] else '— no room'}") for m in ms]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.match_manage(x, tid, int(value), kind, back)
        extra = []
        if kind == "tournament":
            extra.append(ActionButton("Add Match", lambda x: self.add_match(x, tid), emoji="➕", style=SUCCESS, row=2))
        await PickList(self.bot, i.user.id, brand, f"MATCHES • {trunc(t['title'], 40)}", rows, picked, placeholder="Manage a match…", back=back,
                       empty="No matches yet." + (" Add Match 1 to start collecting results." if kind == "tournament" else " Assign teams to lobbies to create them."),
                       extra=extra).start(i)

    async def add_match(self, i: discord.Interaction, tid: int) -> None:
        t = await self.bot.managed_event(i, "tournament", tid)
        off = await self._offset(i.guild_id)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            await self.bot.managed_event(x, "tournament", tid)
            when = parse_local(f["when"], off, field="Match time", required=False)
            map_name = optional(f["map"], field="Map", max_len=40) or t["map"]
            await self.bot.db.run(lambda c: add_match(c, guild_id=x.guild_id, kind="tournament", event_id=tid, actor_id=x.user.id, map_name=map_name, scheduled_at=when))
            await self.matches(x, tid)
        await i.response.send_modal(FormModal("Add Match", [
            Field("map", "Map", required=False, max_length=40, default=t["map"]),
            Field("when", "Match time (DD-MM-YYYY HH:MM)", required=False, max_length=20, default=to_local_text(t["start_time"], off))], submit))

    async def match_manage(self, i: discord.Interaction, tid: int, mid: int, kind: str = "tournament", back=None) -> None:
        back = back or (lambda x: self.manage(x, tid))
        await self.bot.managed_event(i, kind, tid)
        m = await self.bot.db.read(lambda c: get_match(c, mid))
        if m["kind"] != kind or m["event_id"] != tid:
            raise ServiceError("Match not found.")
        eligible = await self.bot.db.read(lambda c: len(eligible_team_ids(c, m)))
        brand = await self.bot.brand(i.guild_id)
        e = base(brand, "MATCH", f"**{match_label(m)}**")
        e.add_field(name="Scheduled", value=ts(m["scheduled_at"], "F") if m["scheduled_at"] else "TBA")
        e.add_field(name="Status", value=m["status"].title())
        e.add_field(name="Room", value=("🔓 Released" if m["room_released"] else "🔒 Set, hidden") if m["room_id"] else "Not set")
        if m["room_id"]:
            e.add_field(name="Credentials (organizer view)", value=f"ID `{m['room_id']}` • Pass ||`{m['room_password']}`||", inline=False)
        e.add_field(name="Eligible teams", value=f"{eligible} approved + checked-in")
        view = BaseView(self.bot, i.user.id)
        again = lambda y: self.match_manage(y, tid, mid, kind, back)  # noqa: E731

        async def set_room(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, kind, tid)

            async def submit(y: discord.Interaction, f: dict[str, str]) -> None:
                await self.bot.managed_event(y, kind, tid)
                rid = clean(f["rid"], field="Room ID", min_len=1, max_len=40)
                pw = clean(f["pw"], field="Password", min_len=1, max_len=40)
                await self.bot.db.run(lambda c: match_set_room(c, mid, rid, pw, y.user.id))
                await again(y)
            await x.response.send_modal(FormModal("Room credentials", [Field("rid", "Room ID", max_length=40), Field("pw", "Password", max_length=40)], submit))
        view.add_item(ActionButton("Set Room ID / Password", set_room, emoji="🔑", row=0))

        async def release(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, kind, tid)
            await x.response.defer()
            await self.bot.db.run(lambda c: release_room(c, mid, not m["room_released"], x.user.id))
            sent = 0
            if not m["room_released"]:
                fresh = await self.bot.db.read(lambda c: get_match(c, mid))
                users = await self.bot.db.read(lambda c: eligible_user_ids(c, fresh))
                for u in users:
                    if await self.bot.dm(u, content=f"🔑 Room details for **{match_label(m)}** are live. Open the panel → My Registrations → *Room Details* to view them privately."):
                        sent += 1
            await again(x)
            if not m["room_released"]:
                await x.followup.send(embed=notice(f"Released. Notified {sent} players by DM; everyone eligible can also open **Room Details** on their registration.", "ok"), ephemeral=True)
        view.add_item(ActionButton("Hide Room" if m["room_released"] else "Release Room", release, emoji="🔓", style=SUCCESS if not m["room_released"] else SECONDARY,
                                   disabled=not m["room_id"], row=0))

        async def edit(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, kind, tid)
            off = await self._offset(x.guild_id)

            async def submit(y: discord.Interaction, f: dict[str, str]) -> None:
                await self.bot.managed_event(y, kind, tid)
                when = parse_local(f["when"], off, field="Match time", required=False)
                map_name = optional(f["map"], field="Map", max_len=40)
                await self.bot.db.run(lambda c: c.execute("UPDATE matches SET map=?, scheduled_at=? WHERE id=?", (map_name, when, mid)))
                await again(y)
            await x.response.send_modal(FormModal("Edit Match", [Field("map", "Map", required=False, max_length=40, default=m["map"]),
                                                                Field("when", "Time (DD-MM-YYYY HH:MM)", required=False, max_length=20, default=to_local_text(m["scheduled_at"], off))], submit))
        view.add_item(ActionButton("Edit Match", edit, emoji="✏️", row=1))

        async def finish(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, kind, tid)
            new = "SCHEDULED" if m["status"] != "SCHEDULED" else "COMPLETED"
            await self.bot.db.run(lambda c: c.execute("UPDATE matches SET status=? WHERE id=?", (new, mid)))
            await again(x)
        view.add_item(ActionButton("Reopen" if m["status"] == "COMPLETED" else "Mark complete", finish, emoji="🏁", row=1, disabled=m["status"] == "CANCELLED"))
        add_nav(view, lambda x: self.matches(x, tid, kind, back))
        await show(i, e, view)

# --------------------------------------------------------------------------------------------------
# cogs/scrims.py
# Scrims: browse, create, organizer management, lobbies, rooms, results.
# --------------------------------------------------------------------------------------------------

class Scrims(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for name, fn in {"scrims": self.hub, "scrim.browse": self.browse, "scrim.create": self.create, "scrim.manage": self.manage_list}.items():
            self.bot.register_entry(name, fn)
        self.bot.register_event_handler("s", self.event_action)

    async def _offset(self, gid: int) -> int:
        return parse_offset((await self.bot.settings(gid)).get("utc_offset", "+05:30"))

    # ------------------------------------------------------------------ hub / browse
    async def hub(self, i: discord.Interaction) -> None:
        perms = await self.bot.perms(i)
        brand = await self.bot.brand(i.guild_id)
        o = self.bot.open
        items = [("Browse Scrims", "🎯", self.browse, PRIMARY), ("Join Scrim", "📝", lambda x: o(x, "reg.start"), SECONDARY),
                 ("My Scrims", "📋", lambda x: o(x, "reg.mine"), SECONDARY), ("My Lobby", "🚪", self.my_lobby, SECONDARY),
                 ("Submit Result", "📤", lambda x: o(x, "result.submit"), SECONDARY), ("View Leaderboard", "📊", lambda x: o(x, "lb.scrims"), SECONDARY),
                 ("Report a Problem", "🚩", lambda x: o(x, "report.start"), SECONDARY)]
        if perms.is_scrim_manager:
            items += [("Create Scrim", "➕", self.create, SUCCESS), ("Edit / Manage", "🛠️", self.manage_list, PRIMARY)]
        await show(i, base(brand, "SCRIMS & PRACTICE", "Find practice lobbies, join with your team and track results."), MenuView(self.bot, i.user.id, items))

    async def browse(self, i: discord.Interaction) -> None:
        def q(c):
            wl = get_bool(c, i.guild_id, "waitlist")
            out = []
            for s in scrim_list_for_guild(c, i.guild_id, ("PUBLISHED", "PAUSED", "COMPLETED"), limit=60):
                cnt = registration_counts(c, "scrim", s["id"])
                out.append((s, cnt, registration_state("scrim", s, cnt, wl)))
            return out
        data = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"#{s['id']} {s['title']}", str(s["id"]), f"{s['tier'] or 'Open'} • {REG_STATE.get(st, st)}"[:100],
                 f"{event_line('scrim', s)}\n┗ {s['tier'] or 'Open'} • {cnt.get('PENDING', 0) + cnt.get('APPROVED', 0)}/{s['lobby_count'] * s['lobby_capacity']} teams • {REG_STATE.get(st, st)}")
                for s, cnt, st in data]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.detail(x, int(value), back=self.browse)
        await PickList(self.bot, i.user.id, brand, "SCRIMS", rows, picked, placeholder="Open a scrim…", back=self.hub, per_page=6,
                       empty="No scrims are published right now.", refresh=self.browse).start(i)

    async def event_action(self, i: discord.Interaction, action: str, sid: int) -> None:
        if action == "view":
            await self.detail(i, sid, back=None)
        elif action == "reg":
            await self.bot.get_cog("Registrations").register_flow(i, "scrim", sid)  # type: ignore[union-attr]
        elif action == "rules":
            await self.rules(i, sid)
        elif action == "mine":
            await self.bot.get_cog("Registrations").my_registration(i, "scrim", sid)  # type: ignore[union-attr]

    async def detail(self, i: discord.Interaction, sid: int, *, back) -> None:
        def q(c):
            s = scrim_get(c, sid)
            cnt = registration_counts(c, "scrim", sid)
            return s, cnt, registration_state("scrim", s, cnt, get_bool(c, s["guild_id"], "waitlist")), scrim_lobbies(c, sid)
        s, cnt, state, lobbies = await self.bot.db.read(q)
        perms = await self.bot.perms(i)
        can_manage = can_manage_event(perms, i.user.id, s["organizer_id"], "scrim")
        if s["guild_id"] != i.guild_id or (s["status"] == "DRAFT" and not can_manage):
            raise ServiceError("That scrim is not published.")
        brand = await self.bot.brand(i.guild_id)
        view = BaseView(self.bot, i.user.id)
        view.add_item(ActionButton("Join Scrim", lambda x: self.bot.get_cog("Registrations").register_flow(x, "scrim", sid), emoji="📝", style=SUCCESS,
                                   disabled=state not in ("OPEN", "WAITLIST"), row=0))
        view.add_item(ActionButton("Rules", lambda x: self.rules(x, sid), emoji="📜", row=0))
        view.add_item(ActionButton("My Registration", lambda x: self.bot.get_cog("Registrations").my_registration(x, "scrim", sid), emoji="🎫", row=0))
        view.add_item(ActionButton("Leaderboard", lambda x: self.bot.get_cog("Leaderboards").show_event(x, "scrim", sid, lambda y: self.detail(y, sid, back=back)), emoji="📊", row=1))
        if can_manage:
            view.add_item(ActionButton("Manage", lambda x: self.manage(x, sid), emoji="🛠️", style=PRIMARY, row=1))
        add_nav(view, back)
        await show(i, scrim_embed(brand, s, cnt, state, lobbies), view)

    async def rules(self, i: discord.Interaction, sid: int) -> None:
        s = await self.bot.db.read(lambda c: scrim_get(c, sid))
        rule = await self.bot.db.read(lambda c: get_rule(c, s["guild_id"], s["score_rule_id"]))
        brand = await self.bot.brand(i.guild_id)
        text = (f"**{s['title']}** • tier **{s['tier'] or 'Open'}**\n\n**Scoring ({rule.name})**\n{rule.describe()}\n\n"
                "• Register with a full roster; check in before the start time.\n• Teams that do not check in are marked **no-show**.\n"
                "• Room ID/password are released privately to checked-in teams in your lobby.\n• Submit your result with screenshot evidence; a referee approves it.")
        view = BaseView(self.bot, i.user.id)
        view.add_item(ActionButton("Back to scrim", lambda x: self.detail(x, sid, back=self.browse), emoji="🎯"))
        add_nav(view)
        await show(i, base(brand, "SCRIM RULES", text), view)

    async def my_lobby(self, i: discord.Interaction) -> None:
        team = await self.bot.db.read(lambda c: team_of_user(c, i.guild_id, i.user.id))
        if not team:
            await say(i, "Join or create a team first.", "warn")
            return

        def q(c):
            out = []
            for r in team_registrations(c, "scrim", team["id"]):
                if r["status"] == "APPROVED" and r["event_status"] in ("PUBLISHED", "PAUSED"):
                    lob = c.execute("SELECT number FROM lobbies WHERE id=?", (r["lobby_id"],)).fetchone() if r["lobby_id"] else None
                    out.append((r, lob))
            return out
        regs = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        lines = [f"🎯 **{trunc(r['title'], 40)}** • {('Lobby ' + str(lob['number'])) if lob else 'lobby not assigned yet'} • starts {ts(r['start_time'], 'R')}" for r, lob in regs]
        view = BaseView(self.bot, i.user.id)
        view.add_item(ActionButton("Room Details", lambda x: self._room_pick(x, regs), emoji="🔑", disabled=not regs))
        add_nav(view, self.hub)
        await show(i, base(brand, "MY LOBBY", "\n".join(lines) or "Your team has no approved scrim registrations."), view)

    async def _room_pick(self, i: discord.Interaction, regs: list) -> None:
        if len(regs) == 1:
            await self.bot.get_cog("Registrations").room(i, "scrim", regs[0][0]["event_id"])  # type: ignore[union-attr]
            return
        brand = await self.bot.brand(i.guild_id)
        view = BaseView(self.bot, i.user.id)

        async def chosen(x: discord.Interaction, vals: list[str]) -> None:
            await self.bot.get_cog("Registrations").room(x, "scrim", int(vals[0]))  # type: ignore[union-attr]
        view.add_item(ChoiceSelect([(r["title"], str(r["event_id"]), None) for r, _ in regs], chosen, placeholder="Which scrim?", row=0))
        add_nav(view, self.my_lobby)
        await show(i, base(brand, "ROOM DETAILS", "Pick the scrim."), view)

    # ------------------------------------------------------------------ create
    async def create(self, i: discord.Interaction) -> None:
        if not await self.bot.require(i, lambda p: can_create_event(p, "scrim"), "Only scrim managers can create scrims."):
            return
        s = await self.bot.settings(i.guild_id)
        games = await self.bot.db.read(lambda c: get_games(c, i.guild_id))
        off = parse_offset(s.get("utc_offset", "+05:30"))

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not can_create_event(await self.bot.perms(x), "scrim"):
                raise ServiceError("Only scrim managers can create scrims.")
            title = clean(f["title"], field="Title", min_len=3, max_len=80)
            game = resolve_game(games, f["game"])
            mode = parse_choice(f["mode"], field="Mode", choices=MODES, default="squad")
            tier = parse_choice(f["tier"], field="Tier", choices={**TIERS, **{t.lower(): t for t in TIERS.values()}}, default="Custom")
            start = parse_local(f["start"], off, field="Start time")
            sid = await self.bot.db.run(lambda c: scrim_create_draft(c, x.guild_id, x.user.id, title=title, game=game, mode=mode, tier=tier, map_name=None,
                                                                  start_time=start, fee=None, lobby_count=1, lobby_capacity=20, team_size=None))
            await self.manage(x, sid)
        await i.response.send_modal(FormModal("Create Scrim", [
            Field("title", "Title", "e.g. Night Scrims T3", max_length=80, min_length=3), Field("game", "Game", games[0], max_length=30, default=games[0]),
            Field("mode", "Mode (solo / duo / squad)", max_length=5, default="squad"),
            Field("tier", "Tier (SQ, T3, T2Q, T2, Custom)", max_length=10, default="T3"),
            Field("start", f"Start (DD-MM-YYYY HH:MM, UTC{s.get('utc_offset', '+05:30')})", "25-12-2026 21:00", max_length=20)], submit))

    # ------------------------------------------------------------------ manage
    async def manage_list(self, i: discord.Interaction) -> None:
        perms = await self.bot.perms(i)
        if not perms.is_scrim_manager:
            await say(i, "Only scrim managers can manage scrims.", "error")
            return
        rows_db = await self.bot.db.read(lambda c: scrim_list_for_guild(c, i.guild_id, None, None if perms.is_admin else i.user.id, 60))
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"#{s['id']} {s['title']}", str(s["id"]), f"{s['status'].title()} • {s['tier'] or 'Open'}", event_line("scrim", s)) for s in rows_db]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.manage(x, int(value))
        extra = [ActionButton("Create Scrim", self.create, emoji="➕", style=SUCCESS, row=2)]
        await PickList(self.bot, i.user.id, brand, "MANAGE SCRIMS", rows, picked, placeholder="Open a scrim…", back=self.hub, per_page=8,
                       header="All scrims" if perms.is_admin else "Your scrims", empty="Nothing to manage yet - create your first scrim.",
                       extra=extra, refresh=self.manage_list).start(i)

    async def manage(self, i: discord.Interaction, sid: int) -> None:
        s = await self.bot.managed_event(i, "scrim", sid)

        def q(c):
            cnt = registration_counts(c, "scrim", sid)
            return cnt, registration_state("scrim", s, cnt, get_bool(c, s["guild_id"], "waitlist")), scrim_lobbies(c, sid)
        cnt, state, lobbies = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        e = scrim_embed(brand, s, cnt, state, lobbies)
        e.add_field(name="Pending approvals", value=str(cnt.get("PENDING", 0)))
        st = s["status"]
        locked = st in LOCKED
        view = BaseView(self.bot, i.user.id)
        view.add_item(ActionButton("Edit Details", lambda x: self.edit_details(x, sid), emoji="✏️", disabled=locked, row=0))
        view.add_item(ActionButton("Edit Settings", lambda x: self.edit_settings(x, sid), emoji="⚙️", disabled=locked, row=0))
        view.add_item(ActionButton("Scoring Rule", lambda x: self.pick_rule(x, sid), emoji="🧮", disabled=locked, row=0))
        if st in ("DRAFT", "PAUSED"):
            view.add_item(ActionButton("Publish" if st == "DRAFT" else "Resume", lambda x: self.set_status(x, sid, "PUBLISHED"), emoji="🚀", style=SUCCESS, row=1))
        if st == "PUBLISHED":
            view.add_item(ActionButton("Pause", lambda x: self.set_status(x, sid, "PAUSED"), emoji="⏸️", row=1))
        if st in ("PUBLISHED", "PAUSED"):
            view.add_item(ActionButton("Complete", lambda x: self.set_status(x, sid, "COMPLETED"), emoji="🏁", row=1))
        if not locked:
            view.add_item(ActionButton("Cancel Scrim", lambda x: self.set_status(x, sid, "CANCELLED"), emoji="⛔", style=DANGER, row=1))
        view.add_item(ActionButton("Participants", lambda x: self.bot.get_cog("Registrations").participants(x, "scrim", sid, lambda y: self.manage(y, sid)), emoji="👥", style=PRIMARY, row=2))
        view.add_item(ActionButton("Lobbies", lambda x: self.lobbies(x, sid), emoji="🚪", style=PRIMARY, row=2))
        view.add_item(ActionButton("Matches & Rooms", lambda x: self.bot.get_cog("Tournaments").matches(x, sid, "scrim", lambda y: self.manage(y, sid)), emoji="🎮", row=2))
        view.add_item(ActionButton("Review Results", lambda x: self.bot.get_cog("Matches").review_list(x, "scrim", sid, lambda y: self.manage(y, sid)), emoji="🧾", row=2))
        view.add_item(ActionButton("Delete", lambda x: self.delete(x, sid), emoji="🗑️", style=DANGER, row=3))
        view.add_item(ActionButton("Refresh", lambda x: self.manage(x, sid), emoji="🔄", row=3))
        add_nav(view, self.manage_list)
        await show(i, e, view)

    async def edit_details(self, i: discord.Interaction, sid: int) -> None:
        s = await self.bot.managed_event(i, "scrim", sid)
        games = await self.bot.db.read(lambda c: get_games(c, i.guild_id))
        off = await self._offset(i.guild_id)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            await self.bot.managed_event(x, "scrim", sid)
            tier = parse_choice(f["tier"], field="Tier", choices={**TIERS, **{t.lower(): t for t in TIERS.values()}}, default="Custom")
            fields = dict(title=clean(f["title"], field="Title", min_len=3, max_len=80), game=resolve_game(games, f["game"]), tier=tier,
                          map=optional(f["map"], field="Map", max_len=40), start_time=parse_local(f["start"], off, field="Start time"))
            new = await self.bot.db.run(lambda c: scrim_update(c, sid, x.user.id, **fields))
            if new["start_time"] != s["start_time"]:
                for r in await self.bot.db.read(lambda c: list_regs(c, "scrim", sid, ("PENDING", "APPROVED", "WAITLIST"))):
                    await self.bot.dm(r["captain_id"], content=f"⏰ **{new['title']}** was rescheduled to {ts(new['start_time'], 'F')}.")
            await refresh_event_message(self.bot, "scrim", sid)
            await self.manage(x, sid)
        await i.response.send_modal(FormModal("Edit Scrim", [
            Field("title", "Title", max_length=80, default=s["title"]), Field("game", "Game", max_length=30, default=s["game"]),
            Field("tier", "Tier (SQ, T3, T2Q, T2, Custom)", max_length=10, default=s["tier"]),
            Field("map", "Map", required=False, max_length=40, default=s["map"]),
            Field("start", "Start (DD-MM-YYYY HH:MM)", max_length=20, default=to_local_text(s["start_time"], off))], submit))

    async def edit_settings(self, i: discord.Interaction, sid: int) -> None:
        s = await self.bot.managed_event(i, "scrim", sid)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            await self.bot.managed_event(x, "scrim", sid)
            fields = dict(mode=parse_choice(f["mode"], field="Mode", choices=MODES), fee=optional(f["fee"], field="Entry fee", max_len=40),
                          lobby_count=parse_int(f["lobbies"], field="Lobbies", lo=1, hi=20),
                          lobby_capacity=parse_int(f["cap"], field="Teams per lobby", lo=2, hi=100),
                          team_size=parse_int(f["size"], field="Players per team", lo=1, hi=10))
            await self.bot.db.run(lambda c: scrim_update(c, sid, x.user.id, **fields))
            await refresh_event_message(self.bot, "scrim", sid)
            await self.manage(x, sid)
        await i.response.send_modal(FormModal("Scrim Settings", [
            Field("mode", "Mode (solo / duo / squad)", max_length=5, default=s["mode"]),
            Field("fee", "Entry fee (blank = free)", required=False, max_length=40, default=s["fee"]),
            Field("lobbies", "Number of lobbies", max_length=2, default=str(s["lobby_count"])),
            Field("cap", "Teams per lobby", max_length=3, default=str(s["lobby_capacity"])),
            Field("size", "Players per team", max_length=2, default=str(s["team_size"]))], submit))

    async def set_status(self, i: discord.Interaction, sid: int, status: str) -> None:
        await self.bot.managed_event(i, "scrim", sid)

        async def apply(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, "scrim", sid)
            await x.response.defer()
            await self.bot.db.run(lambda c: scrim_set_status(c, sid, x.user.id, status))
            if status == "PUBLISHED":
                s = await self.bot.db.read(lambda c: scrim_get(c, sid))
                if s["message_id"]:
                    await refresh_event_message(self.bot, "scrim", sid)
                else:
                    await publish_event_message(self.bot, x.guild, "scrim", sid)
            else:
                if status == "CANCELLED":
                    for r in await self.bot.db.read(lambda c: list_regs(c, "scrim", sid, ("PENDING", "APPROVED", "WAITLIST"))):
                        await self.bot.dm(r["captain_id"], content="⛔ A scrim your team registered for was cancelled.")
                await refresh_event_message(self.bot, "scrim", sid)
            await self.manage(x, sid)
        if status in ("CANCELLED", "COMPLETED"):
            await confirm(i, self.bot, f"{'Cancel' if status == 'CANCELLED' else 'Mark as completed'}? The scrim becomes read-only.", apply,
                          yes_label="Yes, continue", on_no=lambda x: self.manage(x, sid))
        else:
            await apply(i)

    async def delete(self, i: discord.Interaction, sid: int) -> None:
        await self.bot.managed_event(i, "scrim", sid)

        async def yes(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, "scrim", sid)
            await delete_event_message(self.bot, x.guild, "scrim", sid)
            await self.bot.db.run(lambda c: scrim_delete(c, sid, x.user.id))
            view = BaseView(self.bot, None)
            view.add_item(ActionButton("Back to list", self.manage_list, emoji="📋"))
            await say(x, "Scrim deleted.", "ok", view=view)
        await confirm(i, self.bot, "**Delete this scrim permanently?** Registrations, lobbies and results are removed. Use **Cancel Scrim** to keep history.", yes,
                      yes_label="Delete forever", on_no=lambda x: self.manage(x, sid))

    async def pick_rule(self, i: discord.Interaction, sid: int) -> None:
        await self.bot.managed_event(i, "scrim", sid)
        rules = await self.bot.db.read(lambda c: list_rules(c, i.guild_id))
        s = await self.bot.db.read(lambda c: scrim_get(c, sid))
        brand = await self.bot.brand(i.guild_id)

        async def chosen(x: discord.Interaction, vals: list[str]) -> None:
            await self.bot.managed_event(x, "scrim", sid)

            def go(c):
                scrim_update(c, sid, x.user.id, score_rule_id=int(vals[0]))
                recalc_standings(c, x.guild_id, "scrim", sid)
            await self.bot.db.run(go)
            await self.manage(x, sid)
        view = BaseView(self.bot, i.user.id)
        view.add_item(ChoiceSelect([(r["name"], str(r["id"]), f"{r['kind']} rule") for r in rules], chosen, placeholder="Choose the scoring rule", row=0))
        add_nav(view, lambda x: self.manage(x, sid))
        current = next((r["name"] for r in rules if r["id"] == s["score_rule_id"]), "server default")
        await show(i, base(brand, "SCORING RULE", f"Current: **{current}**\nKill points and placement points come from the rule. Edit rules in **Admin → Scoring**."), view)

    # ------------------------------------------------------------------ lobbies
    async def lobbies(self, i: discord.Interaction, sid: int) -> None:
        s = await self.bot.managed_event(i, "scrim", sid)
        lobs = await self.bot.db.read(lambda c: scrim_lobbies(c, sid))
        brand = await self.bot.brand(i.guild_id)
        e = base(brand, "LOBBIES", f"**{s['title']}**\nAssign approved teams to lobbies in registration order. Each lobby gets its own Match 1.")
        for l in lobs[:12]:
            e.add_field(name=f"Lobby {l['number']}", value=f"{l['assigned']}/{l['capacity']} teams")
        view = BaseView(self.bot, i.user.id)

        async def assign(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, "scrim", sid)
            out = await self.bot.db.run(lambda c: assign_lobbies(c, sid, x.user.id))
            await refresh_event_message(self.bot, "scrim", sid)
            await self.lobbies(x, sid)
            await x.followup.send(embed=notice(f"Placed {out['placed']} team(s); {out['unplaced']} could not be placed (lobbies full). "
                                                      f"{out['matches_created']} new match(es) created.", "ok" if not out["unplaced"] else "warn"), ephemeral=True)

        async def next_round(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, "scrim", sid)
            n = await self.bot.db.run(lambda c: add_round(c, sid, x.user.id))
            await self.lobbies(x, sid)
            await x.followup.send(embed=notice(f"Added the next match to {n} lobby(ies).", "ok"), ephemeral=True)
        view.add_item(ActionButton("Assign teams to lobbies", assign, emoji="🧩", style=PRIMARY, row=0))
        view.add_item(ActionButton("Add next match to all lobbies", next_round, emoji="➕", row=0))
        view.add_item(ActionButton("Refresh", lambda x: self.lobbies(x, sid), emoji="🔄", row=0))
        add_nav(view, lambda x: self.manage(x, sid))
        await show(i, e, view)

# --------------------------------------------------------------------------------------------------
# cogs/registrations.py
# Registration flows for tournaments AND scrims: register, my registrations, check-in, room details,
# schedule, plus the organizer-side participant manager (approve / reject / waitlist / no-show / lobby move).
# --------------------------------------------------------------------------------------------------

KIND_LABEL = {"tournament": "Tournament", "scrim": "Scrim"}


class Registrations(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for name, fn in {"reg.start": self.start, "reg.mine": self.mine, "reg.checkin": self.checkin, "sched.mine": self.schedule}.items():
            self.bot.register_entry(name, fn)

    # ------------------------------------------------------------------ helpers
    async def _team(self, i: discord.Interaction) -> dict | None:
        return await self.bot.db.read(lambda c: team_of_user(c, i.guild_id, i.user.id))

    async def _need_team(self, i: discord.Interaction, *, manager: bool = False) -> dict | None:
        team = await self._team(i)
        if team is None:
            view = MenuView(self.bot, i.user.id, [("Create Team", "🛡️", lambda x: self.bot.open(x, "team.create"), PRIMARY),
                                                  ("Find Team", "🔎", lambda x: self.bot.open(x, "team.find"), SECONDARY)])
            await show(i, notice("You need to be in a team first. Create one or join one that is recruiting.", "warn"), view)
            return None
        if manager and team["my_role"] not in MANAGERS:
            await say(i, "Only your captain or co-captain can do that.", "error")
            return None
        return team

    # ------------------------------------------------------------------ register
    async def start(self, i: discord.Interaction) -> None:
        brand = await self.bot.brand(i.guild_id)
        view = MenuView(self.bot, i.user.id, [("Tournaments", "🏆", lambda x: self.open_events(x, "tournament"), PRIMARY),
                                              ("Scrims", "🎯", lambda x: self.open_events(x, "scrim"), PRIMARY),
                                              ("My Registrations", "📋", self.mine, SECONDARY)])
        await show(i, base(brand, "REGISTER TEAM", "Pick what you want to enter. Only your **captain or co-captain** can register the team."), view)

    async def open_events(self, i: discord.Interaction, kind: str) -> None:
        def q(c):
            rows = []
            table = "tournaments" if kind == "tournament" else "scrims"
            wl = get_bool(c, i.guild_id, "waitlist")
            for ev in c.execute(f"SELECT * FROM {table} WHERE guild_id=? AND status='PUBLISHED' ORDER BY COALESCE(start_time,'9999')", (i.guild_id,)).fetchall():  # noqa: S608
                cnt = registration_counts(c, kind, ev["id"])
                state = registration_state(kind, ev, cnt, wl)
                if state in ("OPEN", "WAITLIST"):
                    rows.append((ev, cnt, state))
            return rows
        data = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"#{ev['id']} {ev['title']}", str(ev["id"]), f"{ev['game']} • {REG_STATE[state]}"[:100],
                 f"{event_line(kind, ev)} • {REG_STATE[state]}") for ev, cnt, state in data]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.register_flow(x, kind, int(value))
        await PickList(self.bot, i.user.id, brand, f"REGISTER • {KIND_LABEL[kind].upper()}S", rows, picked, placeholder="Pick one to register for…",
                       back=self.start, empty=f"No {KIND_LABEL[kind].lower()}s are open for registration right now.").start(i)

    async def register_flow(self, i: discord.Interaction, kind: str, event_id: int) -> None:
        team = await self._need_team(i, manager=True)
        if team is None:
            return
        ev = await self.bot.db.read(lambda c: get_event(c, kind, event_id))
        members = await self.bot.db.read(lambda c: team_members(c, team["id"]))
        mains = [m for m in members if m["role"] != "sub"]
        text = (f"Register **{team['name']}** for **{ev['title']}**?\n\n"
                f"Roster that will be locked in: {len(mains)} main + {len(members) - len(mains)} sub(s). "
                f"{ev['team_size']} starters are needed.\nEntry: **{(ev.get('entry_fee') if kind == 'tournament' else ev.get('fee')) or 'Free'}**")

        async def yes(x: discord.Interaction) -> None:
            res = await self.bot.db.run(lambda c: register(c, kind, event_id, team["id"], x.user.id))
            await self.bot.dm(res["event"]["organizer_id"], content=f"📝 **{team['name']}** registered for **{res['event']['title']}** ({res['status'].title()}).")
            await refresh_event_message(self.bot, kind, event_id)
            await self.reg_detail(x, kind, res["reg_id"])
        await confirm(i, self.bot, text, yes, yes_label="Register team", danger=False, on_no=self.start)

    # ------------------------------------------------------------------ my registrations
    async def mine(self, i: discord.Interaction) -> None:
        team = await self._need_team(i)
        if team is None:
            return

        def q(c):
            return team_registrations(c, "tournament", team["id"]), team_registrations(c, "scrim", team["id"])
        t_regs, s_regs = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        rows = []
        for kind, regs in (("tournament", t_regs), ("scrim", s_regs)):
            for r in regs:
                rows.append((f"{KIND_LABEL[kind]}: {r['title']}", f"{kind[0]}:{r['id']}", REG_ICON.get(r["status"], r["status"]),
                             f"{'🏆' if kind == 'tournament' else '🎯'} **{trunc(r['title'], 40)}** • {REG_ICON.get(r['status'], r['status'])}"
                             f"{' • ✅ checked in' if r['checked_in'] else ''} • {ts(r['start_time'], 'f')}"))

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.reg_detail(x, "tournament" if value[0] == "t" else "scrim", int(value[2:]))
        await PickList(self.bot, i.user.id, brand, f"MY REGISTRATIONS • {team['name']}", rows, picked, placeholder="Open a registration…",
                       empty="Your team has not registered for anything yet.", refresh=self.mine).start(i)

    async def my_registration(self, i: discord.Interaction, kind: str, event_id: int) -> None:
        team = await self._need_team(i)
        if team is None:
            return
        reg = await self.bot.db.read(lambda c: team_reg(c, kind, event_id, team["id"]))
        if reg is None:
            await say(i, "Your team has not registered for this event yet.", "info")
            return
        await self.reg_detail(i, kind, reg["id"])

    async def reg_detail(self, i: discord.Interaction, kind: str, reg_id: int) -> None:
        def q(c):
            reg = get_reg(c, kind, reg_id)
            k = REGISTRATION_KINDS[kind]
            ev = get_event(c, kind, reg[k["fk"]])
            team = get_team(c, reg["team_id"])
            players = c.execute(f"SELECT user_id, is_sub FROM {k['players']} WHERE reg_id=?", (reg_id,)).fetchall()  # noqa: S608
            lobby = c.execute("SELECT number FROM lobbies WHERE id=?", (reg.get("lobby_id"),)).fetchone() if kind == "scrim" and reg.get("lobby_id") else None
            mine = team_of_user(c, i.guild_id, i.user.id)
            return reg, ev, team, players, lobby, mine
        reg, ev, team, players, lobby, mine = await self.bot.db.read(q)
        perms = await self.bot.perms(i)
        in_team = mine is not None and mine["id"] == reg["team_id"]
        if not in_team and not can_manage_event(perms, i.user.id, ev["organizer_id"], kind):
            raise ServiceError("That registration belongs to another team.")
        brand = await self.bot.brand(i.guild_id)
        e = base(brand, "REGISTRATION", f"**{ev['title']}** • {KIND_LABEL[kind]}\nTeam: **{team['name']}**")
        e.add_field(name="Status", value=REG_ICON.get(reg["status"], reg["status"]))
        e.add_field(name="Check-in", value="✅ Checked in" if reg["checked_in"] else "⬜ Not yet")
        e.add_field(name="Starts", value=ts(ev["start_time"], "F"))
        if lobby:
            e.add_field(name="Lobby", value=f"Lobby {lobby['number']}")
        if kind == "scrim" and reg["no_show"]:
            e.add_field(name="Attendance", value="🚫 Marked no-show")
        starters = [p["user_id"] for p in players if not p["is_sub"]]
        subs = [p["user_id"] for p in players if p["is_sub"]]
        e.add_field(name=f"Starters ({len(starters)})", value="\n".join(f"<@{u}>" for u in starters) or "—", inline=False)
        if subs:
            e.add_field(name=f"Substitutes ({len(subs)})", value="\n".join(f"<@{u}>" for u in subs), inline=False)
        if reg["status"] == "PENDING":
            e.set_footer(text="Waiting for organizer approval")
        view = BaseView(self.bot, i.user.id)
        is_mgr = in_team and mine["my_role"] in MANAGERS
        active = reg["status"] in ("PENDING", "APPROVED", "WAITLIST")
        if is_mgr and reg["status"] == "APPROVED" and not reg["checked_in"]:
            view.add_item(ActionButton("Check In", lambda x: self._do_checkin(x, kind, reg_id), emoji="✅", style=SUCCESS, row=0))
        if in_team and reg["status"] == "APPROVED":
            view.add_item(ActionButton("Room Details", lambda x: self.room(x, kind, ev["id"]), emoji="🔑", row=0))
            view.add_item(ActionButton("Submit Result", lambda x: self.bot.open(x, "result.submit"), emoji="📤", row=0))
        if is_mgr and active:
            view.add_item(ActionButton("Withdraw", lambda x: self._withdraw(x, kind, ev["id"], team["id"], reg_id), emoji="🚪", style=DANGER, row=1))
        view.add_item(ActionButton("Refresh", lambda x: self.reg_detail(x, kind, reg_id), emoji="🔄", row=1))
        add_nav(view, self.mine)
        await show(i, e, view)

    async def _withdraw(self, i: discord.Interaction, kind: str, event_id: int, team_id: int, reg_id: int) -> None:
        async def yes(x: discord.Interaction) -> None:
            await self.bot.db.run(lambda c: withdraw(c, kind, event_id, team_id, x.user.id))
            await refresh_event_message(self.bot, kind, event_id)
            await self.reg_detail(x, kind, reg_id)
        await confirm(i, self.bot, "Withdraw your team from this event? Your slot may go to the waitlist.", yes, yes_label="Withdraw",
                      on_no=lambda x: self.reg_detail(x, kind, reg_id))

    # ------------------------------------------------------------------ check-in
    async def checkin(self, i: discord.Interaction) -> None:
        team = await self._need_team(i, manager=True)
        if team is None:
            return

        def q(c):
            out = []
            for kind in ("tournament", "scrim"):
                for r in team_registrations(c, kind, team["id"]):
                    if r["status"] == "APPROVED" and not r["checked_in"] and r["event_status"] in ("PUBLISHED", "LIVE"):
                        out.append((kind, r))
            return out
        pending = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"{KIND_LABEL[k]}: {r['title']}", f"{k[0]}:{r['id']}", f"Starts {r['start_time'][:16] if r['start_time'] else 'TBA'}",
                 f"{'🏆' if k == 'tournament' else '🎯'} **{trunc(r['title'], 40)}** • starts {ts(r['start_time'], 'R')}") for k, r in pending]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self._do_checkin(x, "tournament" if value[0] == "t" else "scrim", int(value[2:]))
        await PickList(self.bot, i.user.id, brand, "CHECK IN", rows, picked, placeholder="Check in for…", back=self.start,
                       header="Check-in opens shortly before the start time (see server settings).",
                       empty="Nothing to check in for. Your team needs an **approved** registration first.").start(i)

    async def _do_checkin(self, i: discord.Interaction, kind: str, reg_id: int) -> None:
        def go(c):
            reg = get_reg(c, kind, reg_id)
            fk = REGISTRATION_KINDS[kind]["fk"]
            check_in(c, kind, reg[fk], reg["team_id"], i.user.id)
            return reg[fk]
        event_id = await self.bot.db.run(go)
        await refresh_event_message(self.bot, kind, event_id)
        await self.reg_detail(i, kind, reg_id)

    # ------------------------------------------------------------------ room credentials
    async def room(self, i: discord.Interaction, kind: str, event_id: int) -> None:
        """Private response with released room details - only for rostered players of approved, checked-in teams."""
        def q(c):
            ev = get_event(c, kind, event_id)
            out = []
            for m in list_matches(c, kind, event_id):
                if user_can_see_room(c, m, i.user.id):
                    out.append(m)
            return ev, out
        ev, matches = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        if not matches:
            await show(i, base(brand, "ROOM DETAILS", "No room details are available to you yet.\nThey are released by the organizer to **checked-in, approved teams only**."),
                       self._back_view(kind, ev["id"]))
            return
        e = base(brand, "ROOM DETAILS", f"**{ev['title']}**\n⚠️ Private - do not share these details.")
        for m in matches[:8]:
            e.add_field(name=match_label(m), value=f"Room ID: `{m['room_id']}`\nPassword: ||`{m['room_password']}`||", inline=False)
        await show(i, e, self._back_view(kind, ev["id"]))

    def _back_view(self, kind: str, event_id: int) -> BaseView:
        view = BaseView(self.bot, None)
        view.add_item(ActionButton("Back to registration", self.mine, emoji="📋"))
        return view

    # ------------------------------------------------------------------ schedule
    async def schedule(self, i: discord.Interaction) -> None:
        team = await self._need_team(i)
        if team is None:
            return

        def q(c):
            lines = []
            for kind in ("tournament", "scrim"):
                for r in team_registrations(c, kind, team["id"]):
                    if r["status"] not in ("APPROVED", "PENDING") or r["event_status"] in ("CANCELLED",):
                        continue
                    lines.append((r["start_time"] or "9999", f"{'🏆' if kind == 'tournament' else '🎯'} **{trunc(r['title'], 40)}** • starts {ts(r['start_time'], 'F')} "
                                                           f"• {REG_ICON.get(r['status'], r['status'])}"))
                    for m in list_matches(c, kind, r["event_id"]):
                        if kind == "scrim" and m["lobby_id"] and m["lobby_id"] != r.get("lobby_id"):
                            continue
                        lines.append((r["start_time"] or "9999", f"   ↳ {match_label(m)} • {ts(m['scheduled_at'], 'f') if m['scheduled_at'] else 'time TBA'}"))
            return lines
        lines = [l for _, l in await self.bot.db.read(q)]
        brand = await self.bot.brand(i.guild_id)
        await Pager(self.bot, i.user.id, list_pages(brand, "MATCH SCHEDULE", lines, empty="No scheduled matches. Register your team for a tournament or scrim first."),
                    refresh=self.schedule).start(i)

    # ================================================================== organizer side
    async def participants(self, i: discord.Interaction, kind: str, event_id: int, back) -> None:
        ev = await self.bot.managed_event(i, kind, event_id)

        def q(c):
            return list_regs(c, kind, event_id), registration_counts(c, kind, event_id)
        regs, counts = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        order = {"PENDING": 0, "WAITLIST": 1, "APPROVED": 2, "REJECTED": 3, "WITHDRAWN": 4}
        regs.sort(key=lambda r: (order.get(r["status"], 9), r["id"]))
        rows = [(r["name"], str(r["id"]), REG_ICON.get(r["status"], r["status"]),
                 f"{REG_ICON.get(r['status'], r['status'])} **{trunc(r['name'], 30)}**{' ✅' if r['verified'] else ''}"
                 f"{' • checked in' if r['checked_in'] else ''}{' • NO-SHOW' if r.get('no_show') else ''}") for r in regs]
        header = " • ".join(f"{REG_ICON[s]}: {n}" for s, n in sorted(counts.items(), key=lambda kv: order.get(kv[0], 9)) if s in REG_ICON)

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.reg_manage(x, kind, event_id, int(value), back)

        async def approve_all(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, kind, event_id)

            def go(c):
                n = 0
                for r in list_regs(c, kind, event_id, ("PENDING",)):
                    try:
                        registration_set_status(c, kind, r["id"], "APPROVED", x.user.id)
                        n += 1
                    except ServiceError:
                        break
                return n
            n = await self.bot.db.run(go)
            await refresh_event_message(self.bot, kind, event_id)
            await self.participants(x, kind, event_id, back)

        async def close_reg(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, kind, event_id)

            async def yes(y: discord.Interaction) -> None:
                await self.bot.db.run(lambda c: close_registration(c, event_id, y.user.id))
                await refresh_event_message(self.bot, kind, event_id)
                await self.participants(y, kind, event_id, back)
            await confirm(x, self.bot, "Close registration now? New teams will be refused (existing registrations are kept).", yes, yes_label="Close registration",
                          on_no=lambda y: self.participants(y, kind, event_id, back))
        extra = [ActionButton("Approve all pending", approve_all, emoji="✅", style=SUCCESS, row=2)]
        if kind == "tournament":
            extra.append(ActionButton("Close registration", close_reg, emoji="🔒", row=2))
        await PickList(self.bot, i.user.id, brand, f"PARTICIPANTS • {trunc(ev['title'], 40)}", rows, picked, placeholder="Review a team…", back=back,
                       header=header or None, empty="No teams have registered yet.", extra=extra, refresh=lambda x: self.participants(x, kind, event_id, back)).start(i)

    async def reg_manage(self, i: discord.Interaction, kind: str, event_id: int, reg_id: int, back) -> None:
        ev = await self.bot.managed_event(i, kind, event_id)

        def q(c):
            reg = get_reg(c, kind, reg_id)
            team = get_team(c, reg["team_id"])
            return reg, team, player_ids(c, kind, reg_id), team_members(c, reg["team_id"])
        reg, team, players, members = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        e = base(brand, "REVIEW TEAM", f"**{team['name']}**{' ✅ verified' if team['verified'] else ''} • registered for **{trunc(ev['title'], 50)}**")
        e.add_field(name="Status", value=REG_ICON.get(reg["status"], reg["status"]))
        e.add_field(name="Check-in", value="✅" if reg["checked_in"] else "⬜")
        e.add_field(name="Captain", value=f"<@{team['captain_id']}>")
        e.add_field(name="Locked roster", value="\n".join(f"<@{u}>" for u in players) or "—", inline=False)
        view = BaseView(self.bot, i.user.id)

        def decide(status: str, label: str):
            async def go(x: discord.Interaction) -> None:
                await self.bot.managed_event(x, kind, event_id)
                await self.bot.db.run(lambda c: registration_set_status(c, kind, reg_id, status, x.user.id))
                await self.bot.dm(team["captain_id"], content=f"Your registration for **{ev['title']}**: **{label}**.")
                await refresh_event_message(self.bot, kind, event_id)
                await self.reg_manage(x, kind, event_id, reg_id, back)
            return go
        view.add_item(ActionButton("Approve", decide("APPROVED", "approved ✅"), style=SUCCESS, emoji="✅", disabled=reg["status"] == "APPROVED", row=0))
        view.add_item(ActionButton("Reject", decide("REJECTED", "rejected ❌"), style=DANGER, emoji="❌", disabled=reg["status"] == "REJECTED", row=0))
        view.add_item(ActionButton("Waitlist", decide("WAITLIST", "moved to the waitlist 🟠"), emoji="🟠", disabled=reg["status"] == "WAITLIST", row=0))

        async def toggle_checkin(x: discord.Interaction) -> None:
            await self.bot.managed_event(x, kind, event_id)
            await self.bot.db.run(lambda c: set_checked_in(c, kind, reg_id, not reg["checked_in"], x.user.id))
            await self.reg_manage(x, kind, event_id, reg_id, back)
        view.add_item(ActionButton("Undo check-in" if reg["checked_in"] else "Force check-in", toggle_checkin, emoji="🔁", row=1,
                                   disabled=reg["status"] != "APPROVED"))
        if kind == "scrim":
            async def no_show(x: discord.Interaction) -> None:
                await self.bot.managed_event(x, kind, event_id)
                await self.bot.db.run(lambda c: mark_no_show(c, reg_id, not reg["no_show"], x.user.id))
                await self.reg_manage(x, kind, event_id, reg_id, back)
            view.add_item(ActionButton("Clear no-show" if reg["no_show"] else "Mark no-show", no_show, emoji="🚫", row=1))

            async def move(x: discord.Interaction) -> None:
                await self.bot.managed_event(x, kind, event_id)

                async def submit(y: discord.Interaction, f: dict[str, str]) -> None:
                    await self.bot.managed_event(y, kind, event_id)
                    n = parse_int(f["n"], field="Lobby number", lo=1, hi=20)
                    await self.bot.db.run(lambda c: move_team(c, event_id, reg["team_id"], n, y.user.id))
                    await self.reg_manage(y, kind, event_id, reg_id, back)
                await x.response.send_modal(FormModal("Move to lobby", [Field("n", "Lobby number", "1", max_length=2)], submit))
            view.add_item(ActionButton("Move lobby", move, emoji="➡️", row=1, disabled=reg["status"] != "APPROVED"))
        add_nav(view, lambda x: self.participants(x, kind, event_id, back))
        await show(i, e, view)

# --------------------------------------------------------------------------------------------------
# cogs/matches.py
# Match results: submission, evidence, referee review, corrections, disputes/appeals.
# --------------------------------------------------------------------------------------------------

ALLOWED_EVIDENCE_TYPES = ("image/png", "image/jpeg", "image/webp", "image/gif")
MAX_EVIDENCE_BYTES = 8 * 1024 * 1024


def parse_evidence(text: str | None) -> list[str]:
    parts = [p for p in (text or "").replace(",", "\n").split() if p]
    if len(parts) > 3:
        raise ValidationError("Attach at most 3 evidence links.")
    return [validate_url(p, field="Evidence link") for p in parts]  # type: ignore[misc]


class Matches(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for name, fn in {"results": self.hub, "result.submit": self.submit_start, "result.recent": self.recent, "result.mine": self.mine,
                         "result.review": self.review_list}.items():
            self.bot.register_entry(name, fn)

    # ------------------------------------------------------------------ hub / recent
    async def hub(self, i: discord.Interaction) -> None:
        perms = await self.bot.perms(i)
        brand = await self.bot.brand(i.guild_id)
        items = [("Recent Results", "📰", self.recent, PRIMARY), ("Submit Result", "📤", self.submit_start, SECONDARY),
                 ("My Submissions", "🗂️", self.mine, SECONDARY), ("Standings", "📊", lambda x: self.bot.open(x, "leaderboards"), SECONDARY)]
        if perms.is_referee or perms.is_organizer or perms.is_scrim_manager:
            items.append(("Review Results", "🧾", self.review_list, SUCCESS))
        await show(i, base(brand, "MATCH RESULTS", "Teams submit results with evidence; referees verify them. Only **approved** results count toward standings."),
                   MenuView(self.bot, i.user.id, items))

    async def recent(self, i: discord.Interaction) -> None:
        rows = await self.bot.db.read(lambda c: recent_results(c, i.guild_id, 60))
        brand = await self.bot.brand(i.guild_id)
        lines = []
        for r in rows:
            res = f"#{r['placement']} • {r['kills']} kills" if r["placement"] is not None else f"{r['score_for']}-{r['score_against']}"
            lines.append(f"**{trunc(r['event_title'], 36)}** M{r['number']} • **{r['team_name']}** • {res}")
        await Pager(self.bot, i.user.id, list_pages(brand, "RECENT RESULTS", lines, empty="No approved results yet."), back=self.hub, refresh=self.recent).start(i)

    # ------------------------------------------------------------------ submit
    async def submit_start(self, i: discord.Interaction) -> None:
        team = await self.bot.db.read(lambda c: team_of_user(c, i.guild_id, i.user.id))
        if team is None:
            await say(i, "You need to be in a team to submit results.", "warn")
            return
        if team["my_role"] not in MANAGERS:
            await say(i, "Only your captain or co-captain can submit results.", "error")
            return
        ms = await self.bot.db.read(lambda c: submittable_matches(c, team["id"]))
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"{m['title']} • {match_label(m)}", str(m["id"]), (f"Current: {m['existing'].title()}" if m["existing"] else "No result yet")[:100],
                 f"{'🏆' if m['ekind'] == 'tournament' else '🎯'} **{trunc(m['title'], 36)}** • {match_label(m)}"
                 f"{' • ' + RESULT_ICON.get(m['existing'], m['existing']) if m['existing'] else ''}") for m in ms]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.submit_modal(x, team["id"], int(value))
        await PickList(self.bot, i.user.id, brand, f"SUBMIT RESULT • {team['name']}", rows, picked, placeholder="Which match?", back=self.hub,
                       empty="No matches are waiting for your result. You need an **approved** registration and a match created by the organizer.").start(i)

    async def submit_modal(self, i: discord.Interaction, team_id: int, match_id: int) -> None:
        def q(c):
            m = get_match(c, match_id)
            rule = rule_for_match(c, m)
            ex = c.execute("SELECT * FROM match_results WHERE match_id=? AND team_id=?", (match_id, team_id)).fetchone()
            return m, rule, ex
        m, rule, ex = await self.bot.db.read(q)
        if ex and ex["status"] in ("APPROVED", "DISPUTED"):
            raise ServiceError(f"This result is already {ex['status'].lower()}. Open it under My Submissions to dispute it.")
        old_ev = " ".join(json.loads(ex["evidence"] or "[]")) if ex else None

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            tm = await self.bot.db.read(lambda c: team_of_user(c, x.guild_id, x.user.id))
            if not tm or tm["id"] != team_id or tm["my_role"] not in MANAGERS:
                raise ServiceError("Only your captain or co-captain can submit results.")
            kw: dict = dict(match_id=match_id, team_id=team_id, user_id=x.user.id, evidence=parse_evidence(f["evidence"]),
                            notes=optional(f["notes"], field="Notes", max_len=400, multiline=True))
            if rule.kind == "br":
                kw.update(placement=parse_int(f["a"], field="Placement", lo=1, hi=100), kills=parse_int(f["b"], field="Kills", lo=0, hi=200, default=0))
            else:
                kw.update(score_for=parse_int(f["a"], field="Your score", lo=0, hi=999), score_against=parse_int(f["b"], field="Opponent score", lo=0, hi=999))
            rid, created = await self.bot.db.run(lambda c: submit_result(c, **kw))
            await self.bot.staff_log(x.guild, f"🧾 Result **#{rid}** {'submitted' if created else 'updated'} for **{m['kind']} #{m['event_id']}** (team {tm['name']}). Review it in Match Results → Review.")
            await self.result_detail(x, rid, self.mine, notice="Submitted! A referee will review it. Attach screenshots with `/evidence` to speed things up.")
        a_label, b_label = (("Placement (1-100)", "Kills") if rule.kind == "br" else ("Your score (rounds/points)", "Opponent score"))
        await i.response.send_modal(FormModal(f"Result • {match_label(m)}"[:45], [
            Field("a", a_label, max_length=3, default=str(ex["placement"] if rule.kind == "br" and ex and ex["placement"] is not None else (ex["score_for"] if ex and ex["score_for"] is not None else ""))),
            Field("b", b_label, max_length=3, default=str(ex["kills"] if rule.kind == "br" and ex else (ex["score_against"] if ex and ex["score_against"] is not None else ""))),
            Field("evidence", "Evidence links (up to 3, one per line)", "https://... (screenshots/video)", required=False, paragraph=True, max_length=600, default=old_ev),
            Field("notes", "Notes for the referee (optional)", required=False, paragraph=True, max_length=400, default=ex["notes"] if ex else None)], submit))

    async def mine(self, i: discord.Interaction) -> None:
        team = await self.bot.db.read(lambda c: team_of_user(c, i.guild_id, i.user.id))
        if team is None:
            await say(i, "You are not in a team.", "warn")
            return
        rows_db = await self.bot.db.read(lambda c: c.execute(
            "SELECT r.id, r.status, r.placement, r.kills, r.score_for, r.score_against, m.number, m.kind, m.event_id, "
            "CASE m.kind WHEN 'tournament' THEN (SELECT title FROM tournaments WHERE id=m.event_id) ELSE (SELECT title FROM scrims WHERE id=m.event_id) END AS title "
            "FROM match_results r JOIN matches m ON m.id=r.match_id WHERE r.team_id=? ORDER BY r.id DESC LIMIT 50", (team["id"],)).fetchall())
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"{r['title']} • M{r['number']}", str(r["id"]), RESULT_ICON.get(r["status"], r["status"]),
                 f"{RESULT_ICON.get(r['status'], r['status'])} **{trunc(r['title'], 36)}** M{r['number']} • "
                 f"{('#' + str(r['placement']) + ' / ' + str(r['kills']) + 'K') if r['placement'] is not None else (str(r['score_for']) + '-' + str(r['score_against']))}") for r in rows_db]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.result_detail(x, int(value), self.mine)
        await PickList(self.bot, i.user.id, brand, f"MY SUBMISSIONS • {team['name']}", rows, picked, placeholder="Open a submission…", back=self.hub,
                       empty="Your team has not submitted any results yet.", refresh=self.mine).start(i)

    async def result_detail(self, i: discord.Interaction, rid: int, back, *, notice: str | None = None) -> None:
        r = await self.bot.db.read(lambda c: get_result(c, rid))
        team = await self.bot.db.read(lambda c: team_of_user(c, i.guild_id, i.user.id))
        perms = await self.bot.perms(i)
        mine = team is not None and team["id"] == r["team_id"]
        reviewer = await self._may_review(i, perms, r)
        if r["guild_id"] != i.guild_id or not (mine or reviewer):
            raise ServiceError("You can't view that result.")
        brand = await self.bot.brand(i.guild_id)
        e = result_embed(brand, r)
        view = BaseView(self.bot, i.user.id)
        is_mgr = mine and team["my_role"] in MANAGERS
        if is_mgr and r["status"] in ("PENDING", "REJECTED"):
            view.add_item(ActionButton("Edit / Resubmit", lambda x: self.submit_modal(x, r["team_id"], r["match_id"]), emoji="✏️", style=PRIMARY, row=0))
        if is_mgr and r["status"] in ("PENDING", "REJECTED", "APPROVED"):
            view.add_item(ActionButton("Add evidence link", lambda x: self.evidence_modal(x, rid, back), emoji="📎", row=0))
        if is_mgr and r["status"] in ("APPROVED", "REJECTED"):
            kind = "dispute" if r["status"] == "APPROVED" else "appeal"
            view.add_item(ActionButton("Dispute result" if kind == "dispute" else "Appeal rejection", lambda x: self.dispute_modal(x, rid, kind), emoji="⚖️", style=DANGER, row=0))
        if reviewer and r["status"] in ("PENDING", "DISPUTED"):
            view.add_item(ActionButton("Approve", lambda x: self.approve(x, rid, back), emoji="✅", style=SUCCESS, row=1))
            view.add_item(ActionButton("Reject", lambda x: self.reject_modal(x, rid, back), emoji="❌", style=DANGER, row=1))
        if reviewer:
            view.add_item(ActionButton("Correct values", lambda x: self.correct_modal(x, rid, back), emoji="🛠️", row=1))
        add_nav(view, back)
        await show(i, e, view)
        if notice:
            await i.followup.send(embed=embed_notice(notice, "ok"), ephemeral=True)

    async def evidence_modal(self, i: discord.Interaction, rid: int, back) -> None:
        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            r = await self.bot.db.read(lambda c: get_result(c, rid))
            tm = await self.bot.db.read(lambda c: team_of_user(c, x.guild_id, x.user.id))
            perms = await self.bot.perms(x)
            if not ((tm and tm["id"] == r["team_id"] and tm["my_role"] in MANAGERS) or await self._may_review(x, perms, r)):
                raise ServiceError("You can't add evidence to that result.")
            urls = parse_evidence(f["urls"])
            if not urls:
                raise ValidationError("Paste at least one link.")
            await self.bot.db.run(lambda c: scoring_add_evidence(c, rid, urls))
            await self.result_detail(x, rid, back)
        await i.response.send_modal(FormModal("Add evidence", [Field("urls", "Links (one per line)", "https://...", paragraph=True, max_length=600)], submit))

    async def dispute_modal(self, i: discord.Interaction, rid: int, kind: str) -> None:
        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            r = await self.bot.db.read(lambda c: get_result(c, rid))
            tm = await self.bot.db.read(lambda c: team_of_user(c, x.guild_id, x.user.id))
            if not tm or tm["id"] != r["team_id"] or tm["my_role"] not in MANAGERS:
                raise ServiceError("Only your captain or co-captain can open a dispute.")
            desc = clean(f["why"], field="Explanation", min_len=10, max_len=900, multiline=True)
            cid = await self.bot.db.run(lambda c: create_report(c, x.guild_id, x.user.id, kind, f"result:{rid}", desc, parse_evidence(f["urls"]), rid))
            await self.bot.staff_log(x.guild, f"⚖️ New **{kind}** case **#{cid}** on result #{rid} by <@{x.user.id}>.")
            await say(x, f"Case **#{cid}** opened. Staff will review it privately." + (" The contested result no longer counts until it is resolved." if kind == "dispute" else ""), "ok")
        await i.response.send_modal(FormModal("Dispute" if kind == "dispute" else "Appeal", [
            Field("why", "What is wrong?", paragraph=True, max_length=900, min_length=10), Field("urls", "Evidence links (optional)", required=False, paragraph=True, max_length=600)], submit))

    # ------------------------------------------------------------------ review (referees)
    async def _may_review(self, i: discord.Interaction, perms: Perms, r: dict) -> bool:
        refs = await self.bot.db.read(lambda c: assigned_referees(c, r["kind"], r["event_id"]))
        return can_review_result(perms, i.user.id, r.get("organizer_id"), refs)

    async def _auth_review(self, i: discord.Interaction, rid: int) -> dict:
        r = await self.bot.db.read(lambda c: get_result(c, rid))
        perms = await self.bot.perms(i)
        if r["guild_id"] != i.guild_id or not await self._may_review(i, perms, r):
            raise ServiceError("You are not allowed to review this result.")
        return r

    async def review_list(self, i: discord.Interaction, kind: str | None = None, event_id: int | None = None, back=None) -> None:
        perms = await self.bot.perms(i)
        if not (perms.is_referee or perms.is_organizer or perms.is_scrim_manager):
            await say(i, "Only referees and organizers can review results.", "error")
            return
        back = back or self.hub
        pending = await self.bot.db.read(lambda c: pending_results(c, i.guild_id))
        if kind and event_id:
            pending = [r for r in pending if r["kind"] == kind and r["event_id"] == event_id]
        allowed = []
        for r in pending:
            full = await self.bot.db.read(lambda c, rid=r["id"]: get_result(c, rid))
            if await self._may_review(i, perms, full):
                allowed.append(r)
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"{r['team_name']} • {r['event_title']} M{r['number']}"[:100], str(r["id"]), RESULT_ICON.get(r["status"], r["status"]),
                 f"{RESULT_ICON.get(r['status'], r['status'])} **{r['team_name']}** • {trunc(r['event_title'], 30)} M{r['number']}") for r in allowed]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.result_detail(x, int(value), lambda y: self.review_list(y, kind, event_id, back))
        await PickList(self.bot, i.user.id, brand, "REVIEW RESULTS", rows, picked, placeholder="Review a result…", back=back,
                       empty="Nothing is waiting for review. 🎉", refresh=lambda x: self.review_list(x, kind, event_id, back)).start(i)

    async def approve(self, i: discord.Interaction, rid: int, back) -> None:
        await self._auth_review(i, rid)
        row = await self.bot.db.run(lambda c: review_result(c, result_id=rid, reviewer_id=i.user.id, approve=True, reason=None))
        await self._after_review(i, rid, True, None)
        await self.result_detail(i, rid, back)

    async def reject_modal(self, i: discord.Interaction, rid: int, back) -> None:
        await self._auth_review(i, rid)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            await self._auth_review(x, rid)
            reason = clean(f["reason"], field="Reason", min_len=3, max_len=400, multiline=True)
            await self.bot.db.run(lambda c: review_result(c, result_id=rid, reviewer_id=x.user.id, approve=False, reason=reason))
            await self._after_review(x, rid, False, reason)
            await self.result_detail(x, rid, back)
        await i.response.send_modal(FormModal("Reject result", [Field("reason", "Reason (shown to the team)", paragraph=True, max_length=400, min_length=3)], submit))

    async def correct_modal(self, i: discord.Interaction, rid: int, back) -> None:
        r = await self._auth_review(i, rid)
        rule = await self.bot.db.read(lambda c: rule_for_match(c, get_match(c, r["match_id"])))

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            await self._auth_review(x, rid)
            reason = clean(f["reason"], field="Reason", min_len=3, max_len=300)
            bonus = parse_float(f["bonus"], field="Bonus", lo=-1000, hi=1000, default=0)
            kw: dict = dict(result_id=rid, reviewer_id=x.user.id, reason=reason, bonus=bonus, placement=None, kills=None, score_for=None, score_against=None)
            if rule.kind == "br":
                kw.update(placement=parse_int(f["a"], field="Placement", lo=1, hi=100), kills=parse_int(f["b"], field="Kills", lo=0, hi=200))
            else:
                kw.update(score_for=parse_int(f["a"], field="Score for", lo=0, hi=999), score_against=parse_int(f["b"], field="Score against", lo=0, hi=999))

            def go(c):
                correct_result(c, **kw)
                cur = c.execute("SELECT status FROM match_results WHERE id=?", (rid,)).fetchone()["status"]
                if cur in ("PENDING", "DISPUTED"):
                    review_result(c, result_id=rid, reviewer_id=x.user.id, approve=True, reason=f"Corrected: {reason}")
            await self.bot.db.run(go)
            await self._after_review(x, rid, True, f"Corrected by referee: {reason}")
            await self.result_detail(x, rid, back)
        a_label, b_label = (("Placement", "Kills") if rule.kind == "br" else ("Score for", "Score against"))
        await i.response.send_modal(FormModal("Correct result", [
            Field("a", a_label, max_length=3, default=str(r["placement"] if rule.kind == "br" and r["placement"] is not None else (r["score_for"] if r["score_for"] is not None else ""))),
            Field("b", b_label, max_length=3, default=str(r["kills"] if rule.kind == "br" else (r["score_against"] if r["score_against"] is not None else ""))),
            Field("bonus", "Bonus / penalty points", required=False, max_length=6, default=f"{r['bonus']:g}" if r["bonus"] else "0"),
            Field("reason", "Reason for the correction", max_length=300, min_length=3)], submit))

    async def _after_review(self, i: discord.Interaction, rid: int, approved: bool, reason: str | None) -> None:
        r = await self.bot.db.read(lambda c: get_result(c, rid))
        team = await self.bot.db.read(lambda c: get_team(c, r["team_id"]))
        await self.bot.dm(r["submitted_by"], content=f"Your result for **{r['event_title']}** M{r['number']} was **{'approved ✅' if approved else 'rejected ❌'}**." + (f"\nReferee note: {reason}" if reason else ""))
        if approved and team:
            cid = await self.bot.db.read(lambda c: int(get_setting(c, i.guild_id, "ch_results") or 0))
            brand = await self.bot.brand(i.guild_id)
            if cid:
                await self.bot.post(i.guild, cid, embed=result_embed(brand, r))

    # ------------------------------------------------------------------ /evidence (real attachment workflow)
    @app_commands.command(name="evidence", description="Attach a screenshot as evidence to a result or a case")
    @app_commands.describe(target="Where the evidence goes", item_id="The result # or case # shown in its details", file="Screenshot / image")
    @app_commands.choices(target=[app_commands.Choice(name="Match result", value="result"), app_commands.Choice(name="Report case", value="case")])
    @app_commands.guild_only()
    async def evidence(self, i: discord.Interaction, target: app_commands.Choice[str], item_id: int, file: discord.Attachment) -> None:
        if (file.content_type or "").split(";")[0] not in ALLOWED_EVIDENCE_TYPES:
            raise ServiceError("Evidence must be an image (png, jpg, webp or gif). For video, paste a link in the result form.")
        if file.size > MAX_EVIDENCE_BYTES:
            raise ServiceError("That image is larger than 8 MB.")
        perms = await self.bot.perms(i)
        if target.value == "result":
            r = await self.bot.db.read(lambda c: get_result(c, item_id))
            tm = await self.bot.db.read(lambda c: team_of_user(c, i.guild_id, i.user.id))
            if r["guild_id"] != i.guild_id or not ((tm and tm["id"] == r["team_id"] and tm["my_role"] in MANAGERS) or await self._may_review(i, perms, r)):
                raise ServiceError("You can only add evidence to your own team's results.")
        else:
            c_ = await self.bot.db.read(lambda c: report_get(c, item_id))
            if c_["guild_id"] != i.guild_id or not (c_["reporter_id"] == i.user.id or can_manage_reports(perms)):
                raise ServiceError("You can only add evidence to your own cases.")
        await i.response.defer(ephemeral=True)
        link = file.url
        cid = await self.bot.db.read(lambda c: int(get_setting(c, i.guild_id, "ch_staff_log") or 0) or int(get_setting(c, i.guild_id, "ch_results") or 0))
        stored_permanently = False
        if cid:
            brand = await self.bot.brand(i.guild_id)
            e = base(brand, "EVIDENCE", f"{target.name} **#{item_id}** • from <@{i.user.id}>")
            fobj = await file.to_file()
            e.set_image(url=f"attachment://{fobj.filename}")
            ch = i.guild.get_channel(cid) if i.guild else None
            if ch is not None and hasattr(ch, "send"):
                try:
                    msg = await ch.send(embed=e, file=fobj)  # type: ignore[union-attr]
                    link = msg.jump_url  # message links outlive Discord's expiring CDN URLs
                    stored_permanently = True
                except discord.HTTPException:
                    pass
        if target.value == "result":
            await self.bot.db.run(lambda c: scoring_add_evidence(c, item_id, [link]))
        else:
            await self.bot.db.run(lambda c: report_add_evidence(c, item_id, i.user.id, [link], staff=can_manage_reports(perms)))
        await i.followup.send(embed=notice("Evidence attached." + ("" if stored_permanently else " ⚠️ No evidence channel is configured, so this link may expire - ask an admin to set a staff log channel."), "ok" if stored_permanently else "warn"), ephemeral=True)

# --------------------------------------------------------------------------------------------------
# cogs/leaderboards.py
# Standings, scrim leaderboards, season rankings and the hall of fame (all read from approved results).
# --------------------------------------------------------------------------------------------------

class Leaderboards(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for name, fn in {"leaderboards": self.hub, "lb.tournaments": self.tournaments, "lb.scrims": self.scrims,
                         "lb.season": self.season, "lb.hall": self.hall}.items():
            self.bot.register_entry(name, fn)

    async def hub(self, i: discord.Interaction) -> None:
        brand = await self.bot.brand(i.guild_id)
        items = [("Tournament Standings", "🏆", self.tournaments, PRIMARY), ("Scrim Leaderboards", "🎯", self.scrims, PRIMARY),
                 ("Season Rankings", "📈", self.season, SECONDARY), ("Hall of Fame", "👑", self.hall, SECONDARY),
                 ("Recent Results", "📰", lambda x: self.bot.open(x, "result.recent"), SECONDARY)]
        await show(i, base(brand, "RANKINGS & STATISTICS", "Every number here is computed from **referee-approved** results in the database."),
                   MenuView(self.bot, i.user.id, items))

    # ------------------------------------------------------------------ per-event
    async def _event_list(self, i: discord.Interaction, kind: str) -> None:
        def q(c):
            evs = (tournament_list_for_guild(c, i.guild_id, ("PUBLISHED", "LIVE", "COMPLETED"), limit=60) if kind == "tournament"
                   else scrim_list_for_guild(c, i.guild_id, ("PUBLISHED", "PAUSED", "COMPLETED"), limit=60))
            out = []
            for e in evs:
                n = c.execute("SELECT COUNT(*) AS n FROM standings WHERE kind=? AND event_id=?", (kind, e["id"])).fetchone()["n"]
                out.append((e, n))
            return out
        data = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"#{e['id']} {e['title']}", str(e["id"]), f"{n} team(s) ranked", f"{event_line(kind, e)} • **{n}** ranked") for e, n in data]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.show_event(x, kind, int(value), lambda y: self._event_list(y, kind))
        label = "TOURNAMENT STANDINGS" if kind == "tournament" else "SCRIM LEADERBOARDS"
        await PickList(self.bot, i.user.id, brand, label, rows, picked, placeholder="Open standings…", back=self.hub, per_page=6,
                       empty="Nothing to show yet.", refresh=lambda x: self._event_list(x, kind)).start(i)

    async def tournaments(self, i: discord.Interaction) -> None:
        await self._event_list(i, "tournament")

    async def scrims(self, i: discord.Interaction) -> None:
        await self._event_list(i, "scrim")

    async def show_event(self, i: discord.Interaction, kind: str, event_id: int, back) -> None:
        def q(c):
            ev = (tournament_get(c, event_id) if kind == "tournament" else scrim_get(c, event_id))
            rule = get_rule(c, ev["guild_id"], ev["score_rule_id"])
            return ev, rule, get_standings(c, kind, event_id)
        ev, rule, rows = await self.bot.db.read(q)
        if ev["guild_id"] != i.guild_id:
            return
        brand = await self.bot.brand(i.guild_id)
        pages = standings_pages(brand, ev["title"], rows, rule.describe())
        extra = [ActionButton("Per-match results", lambda x: self.per_match(x, kind, event_id, lambda y: self.show_event(y, kind, event_id, back)), emoji="📋", row=1)]
        await Pager(self.bot, i.user.id, pages, extra=extra, back=back, refresh=lambda x: self.show_event(x, kind, event_id, back)).start(i)

    async def per_match(self, i: discord.Interaction, kind: str, event_id: int, back) -> None:
        rows = await self.bot.db.read(lambda c: c.execute(
            "SELECT m.number, m.lobby_id, l.number AS lobby, t.name, r.placement, r.kills, r.score_for, r.score_against, r.bonus FROM match_results r "
            "JOIN matches m ON m.id=r.match_id JOIN teams t ON t.id=r.team_id LEFT JOIN lobbies l ON l.id=m.lobby_id "
            "WHERE m.kind=? AND m.event_id=? AND r.status='APPROVED' ORDER BY m.number, l.number, r.placement, t.name", (kind, event_id)).fetchall())
        brand = await self.bot.brand(i.guild_id)
        lines = []
        for r in rows:
            res = f"#{r['placement']} • {r['kills']}K" if r["placement"] is not None else f"{r['score_for']}-{r['score_against']}"
            lines.append(f"M{r['number']}{' L' + str(r['lobby']) if r['lobby'] else ''} • **{trunc(r['name'], 24)}** • {res}" + (f" • bonus {r['bonus']:g}" if r["bonus"] else ""))
        await Pager(self.bot, i.user.id, list_pages(brand, "PER-MATCH RESULTS", lines, per_page=15, empty="No approved results yet."), back=back).start(i)

    # ------------------------------------------------------------------ season
    async def season(self, i: discord.Interaction, season: str | None = "__current__") -> None:
        def q(c):
            seasons = [r["season"] for r in c.execute(
                "SELECT DISTINCT season FROM (SELECT season FROM tournaments WHERE guild_id=? UNION SELECT season FROM scrims WHERE guild_id=?) WHERE season IS NOT NULL ORDER BY season",
                (i.guild_id, i.guild_id))]
            cur = c.execute("SELECT value FROM guild_settings WHERE guild_id=? AND key='season'", (i.guild_id,)).fetchone()
            current = cur["value"] if cur else "Season 1"
            chosen = current if season == "__current__" else season
            return seasons, current, chosen, season_rankings(c, i.guild_id, chosen, 100)
        seasons, current, chosen, rows = await self.bot.db.read(q)
        brand = await self.bot.brand(i.guild_id)
        ranked = []
        last = None
        for n, r in enumerate(rows, 1):
            key = (r["points"], r["kills"], r["wins"])
            rank = n if key != last else ranked[-1]["rank"]
            last = key
            ranked.append({**r, "rank": rank})
        title = f"{chosen or 'All seasons'} rankings"
        pages = standings_pages(brand, title, ranked)
        extra: list[discord.ui.Item] = []
        options = [("All seasons", "__all__", None)] + [(s, s, "current" if s == current else None) for s in seasons[:23]]

        async def pick(x: discord.Interaction, vals: list[str]) -> None:
            await self.season(x, None if vals[0] == "__all__" else vals[0])
        extra.append(ChoiceSelect(options, pick, placeholder=f"Season: {chosen or 'all'}", row=1))
        await Pager(self.bot, i.user.id, pages, extra=extra, back=self.hub).start(i)

    # ------------------------------------------------------------------ hall of fame
    async def hall(self, i: discord.Interaction) -> None:
        rows = await self.bot.db.read(lambda c: c.execute("SELECT * FROM hall_of_fame WHERE guild_id=? ORDER BY id DESC LIMIT 100", (i.guild_id,)).fetchall())
        brand = await self.bot.brand(i.guild_id)
        lines = [f"👑 **{trunc(r['title'], 50)}**{' • ' + r['season'] if r['season'] else ''}\n┗ {trunc(r['winner'], 80)}" + (f" - {trunc(r['note'], 80)}" if r["note"] else "") for r in rows]
        await Pager(self.bot, i.user.id, list_pages(brand, "HALL OF FAME", lines, per_page=8, empty="No champions have been entered yet."), back=self.hub).start(i)

# --------------------------------------------------------------------------------------------------
# cogs/staff.py
# Staff panel: applications, role grants, duty, referee assignments, activity log.
# --------------------------------------------------------------------------------------------------

class Staff(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for name, fn in {"staff": self.hub, "staff.apply": self.apply, "staff.applications": self.applications, "staff.members": self.members,
                         "staff.activity": self.activity, "staff.referees": self.referees}.items():
            self.bot.register_entry(name, fn)

    async def hub(self, i: discord.Interaction) -> None:
        perms = await self.bot.perms(i)
        brand = await self.bot.brand(i.guild_id)
        on_duty = await self.bot.db.read(lambda c: is_on_duty(c, i.guild_id, i.user.id))
        o = self.bot.open
        items: list = []
        if perms.is_staff:
            items += [("Case Queue", "🧰", lambda x: o(x, "report.cases"), PRIMARY), ("Verify Teams", "✅", lambda x: o(x, "team.directory"), SECONDARY),
                      ("Review Results", "🧾", lambda x: o(x, "result.review"), SECONDARY),
                      (f"Duty: {'ON' if on_duty else 'OFF'}", "🟢" if on_duty else "⚪", self.toggle_duty, SUCCESS if on_duty else SECONDARY),
                      ("Announcements", "📣", lambda x: o(x, "community.announce"), SECONDARY), ("Staff Activity", "🕵️", self.activity, SECONDARY)]
        if perms.is_organizer or perms.is_scrim_manager:
            items.append(("Assign Referees", "🧑‍⚖️", self.referees, SECONDARY))
        if perms.is_admin:
            items += [("Applications", "📨", self.applications, PRIMARY), ("Staff Roster", "👥", self.members, SECONDARY)]
        if not perms.is_staff:
            items.append(("Apply for Staff", "📝", self.apply, PRIMARY))
        else:
            items.append(("Apply for another role", "📝", self.apply, SECONDARY))
        text = "Tools for referees, organizers and moderators." if perms.any_staff else "You are not on the staff team yet. You can apply below."
        await show(i, base(brand, "STAFF PANEL", text), MenuView(self.bot, i.user.id, items))

    async def toggle_duty(self, i: discord.Interaction) -> None:
        if not await self.bot.require(i, lambda p: p.is_staff, "Only staff have a duty status."):
            return
        cur = await self.bot.db.read(lambda c: is_on_duty(c, i.guild_id, i.user.id))
        await self.bot.db.run(lambda c: set_duty(c, i.guild_id, i.user.id, not cur))
        await self.hub(i)

    # ------------------------------------------------------------------ applications
    async def apply(self, i: discord.Interaction) -> None:
        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            role = parse_choice(f["role"], field="Role", choices={"staff": "staff", "referee": "referee", "organizer": "organizer",
                                                                   "scrim manager": "scrim_manager", "scrim_manager": "scrim_manager"})
            reason = clean(f["why"], field="Reason", min_len=10, max_len=800, multiline=True)
            aid = await self.bot.db.run(lambda c: apply(c, x.guild_id, x.user.id, role, reason))
            await self.bot.staff_log(x.guild, f"📨 New staff application **#{aid}** from <@{x.user.id}> for **{ROLES[role]}**.")
            await say(x, f"Application **#{aid}** submitted. An administrator will review it.", "ok")
        await i.response.send_modal(FormModal("Staff application", [
            Field("role", "Role (staff / referee / organizer / scrim manager)", max_length=14, default="referee"),
            Field("why", "Why you? Experience, availability…", paragraph=True, max_length=800, min_length=10)], submit))

    async def applications(self, i: discord.Interaction) -> None:
        if not await self.bot.require(i, can_grant_roles, "Only administrators can review applications."):
            return
        apps = await self.bot.db.read(lambda c: pending_applications(c, i.guild_id))
        names = await labels_for(self.bot, i.guild, [a["user_id"] for a in apps])
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"#{a['id']} {names[a['user_id']]}", str(a["id"]), ROLES[a["role_wanted"]],
                 f"📨 **#{a['id']}** <@{a['user_id']}> → **{ROLES[a['role_wanted']]}**") for a in apps]

        async def picked(x: discord.Interaction, value: str) -> None:
            a = next((y for y in apps if str(y["id"]) == value), None)
            if a is None:
                await self.applications(x)
                return
            view = BaseView(self.bot, x.user.id)

            def decide(ok: bool):
                async def go(y: discord.Interaction) -> None:
                    if not await self.bot.require(y, can_grant_roles, "Only administrators can review applications."):
                        return
                    if ok:
                        perms = await self.bot.perms(y)
                        await self.bot.db.run(lambda c: review_application(c, a["id"], perms, y.user.id, True, None))
                        await self.bot.dm(a["user_id"], content=f"🎉 Your application for **{ROLES[a['role_wanted']]}** was approved.")
                        await self.applications(y)
                    else:
                        async def submit(z: discord.Interaction, f: dict[str, str]) -> None:
                            perms2 = await self.bot.perms(z)
                            note = optional(f["note"], field="Note", max_len=300)
                            await self.bot.db.run(lambda c: review_application(c, a["id"], perms2, z.user.id, False, note))
                            await self.bot.dm(a["user_id"], content=f"Your staff application was not approved." + (f"\nNote: {note}" if note else ""))
                            await self.applications(z)
                        await y.response.send_modal(FormModal("Reject application", [Field("note", "Note to applicant (optional)", required=False, max_length=300)], submit))
                return go
            view.add_item(ActionButton("Approve", decide(True), style=SUCCESS, emoji="✅"))
            view.add_item(ActionButton("Reject", decide(False), style=DANGER, emoji="❌"))
            add_nav(view, self.applications)
            await show(x, base(brand, f"APPLICATION #{a['id']}", f"<@{a['user_id']}> wants **{ROLES[a['role_wanted']]}**\n\n{trunc(a['reason'], 900)}"), view)
        await PickList(self.bot, i.user.id, brand, "STAFF APPLICATIONS", rows, picked, placeholder="Review an application…", back=self.hub, empty="No pending applications.").start(i)

    # ------------------------------------------------------------------ roster / grant / revoke
    async def members(self, i: discord.Interaction) -> None:
        if not await self.bot.require(i, can_grant_roles, "Only administrators can manage staff."):
            return
        ms = await self.bot.db.read(lambda c: staff_members(c, i.guild_id))
        by_user: dict[int, list[str]] = {}
        for m in ms:
            by_user.setdefault(m["user_id"], []).append(m["role"])
        names = await labels_for(self.bot, i.guild, list(by_user))
        brand = await self.bot.brand(i.guild_id)
        rows = [(names[u], str(u), ", ".join(ROLES[r] for r in rl)[:100], f"👤 <@{u}> ({names[u]}) • {', '.join(ROLES[r] for r in rl)}") for u, rl in by_user.items()]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.member_menu(x, int(value))

        async def grant_picker(x: discord.Interaction) -> None:
            await self.grant_flow(x)
        extra = [ActionButton("Grant role", grant_picker, emoji="➕", style=SUCCESS, row=2)]
        await PickList(self.bot, i.user.id, brand, "STAFF ROSTER", rows, picked, placeholder="Manage a member…", back=self.hub,
                       header="Roles granted here are in addition to the Discord roles set in Admin → Roles.", empty="No staff roles granted yet.", extra=extra).start(i)

    async def grant_flow(self, i: discord.Interaction) -> None:
        brand = await self.bot.brand(i.guild_id)
        state: dict = {}

        async def user_picked(x: discord.Interaction, vals: list) -> None:
            state["user"] = vals[0]
            await x.response.defer()

        async def role_picked(x: discord.Interaction, vals: list[str]) -> None:
            user = state.get("user")
            if user is None:
                raise ServiceError("Pick the member first.")
            if getattr(user, "bot", False):
                raise ServiceError("Bots cannot be staff.")
            perms = await self.bot.perms(x)
            await self.bot.db.run(lambda c: grant(c, x.guild_id, perms, x.user.id, user.id, vals[0]))
            await self.bot.dm(user.id, content=f"You were granted **{ROLES[vals[0]]}** in **{x.guild.name}**.")
            await self.members(x)
        view = BaseView(self.bot, i.user.id)
        view.add_item(UserPicker(user_picked, placeholder="1. Pick the member", row=0))
        view.add_item(ChoiceSelect([(label, key, None) for key, label in ROLES.items()], role_picked, placeholder="2. Pick the role to grant", row=1))
        add_nav(view, self.members)
        await show(i, base(brand, "GRANT ROLE", "Pick the member, then choose the role. Selecting the role applies it."), view)

    async def member_menu(self, i: discord.Interaction, user_id: int) -> None:
        if not await self.bot.require(i, can_grant_roles, "Only administrators can manage staff."):
            return
        roles = await self.bot.db.read(lambda c: roles_of(c, i.guild_id, user_id))
        brand = await self.bot.brand(i.guild_id)
        view = BaseView(self.bot, i.user.id)

        def revoker(role: str | None):
            async def go(x: discord.Interaction) -> None:
                async def yes(y: discord.Interaction) -> None:
                    perms = await self.bot.perms(y)
                    await self.bot.db.run(lambda c: revoke(c, y.guild_id, perms, y.user.id, user_id, role))
                    await self.members(y)
                await confirm(x, self.bot, f"Remove {'**' + ROLES[role] + '**' if role else '**all staff roles**'} from <@{user_id}>?", yes, yes_label="Remove",
                              on_no=lambda y: self.member_menu(y, user_id))
            return go
        for idx, r in enumerate(roles[:4]):
            view.add_item(ActionButton(f"Remove {ROLES[r]}", revoker(r), emoji="➖", style=DANGER, row=idx // 2))
        if len(roles) > 1:
            view.add_item(ActionButton("Remove ALL roles", revoker(None), emoji="🚫", style=DANGER, row=2))
        add_nav(view, self.members)
        await show(i, base(brand, "STAFF MEMBER", f"<@{user_id}>\nRoles: {', '.join(ROLES[r] for r in roles) or 'none'}"), view)

    # ------------------------------------------------------------------ activity
    async def activity(self, i: discord.Interaction) -> None:
        if not await self.bot.require(i, lambda p: p.is_staff, "Only staff can view activity."):
            return
        rows = await self.bot.db.read(lambda c: activity(c, i.guild_id, 150))
        brand = await self.bot.brand(i.guild_id)
        lines = [f"{ts(r['created_at'], 'R')} <@{r['actor_id']}> `{r['action']}`" + (f" → {trunc(r['target'], 30)}" if r["target"] else "") for r in rows]
        await Pager(self.bot, i.user.id, list_pages(brand, "STAFF ACTIVITY", lines, per_page=12, empty="No staff activity recorded yet."), back=self.hub, refresh=self.activity).start(i)

    # ------------------------------------------------------------------ referee assignment
    async def referees(self, i: discord.Interaction) -> None:
        perms = await self.bot.perms(i)
        if not (perms.is_organizer or perms.is_scrim_manager):
            await say(i, "Only organizers and scrim managers can assign referees.", "error")
            return
        brand = await self.bot.brand(i.guild_id)

        def q(c):
            own = None if perms.is_admin else i.user.id
            ts_ = tournament_list_for_guild(c, i.guild_id, ("DRAFT", "SCHEDULED", "PUBLISHED", "LIVE"), own, 40) if perms.is_organizer else []
            ss = scrim_list_for_guild(c, i.guild_id, ("DRAFT", "PUBLISHED", "PAUSED"), own, 40) if perms.is_scrim_manager else []
            return ts_, ss
        t_list, s_list = await self.bot.db.read(q)
        rows = ([(f"T#{t['id']} {t['title']}", f"tournament:{t['id']}", "Tournament", f"🏆 **#{t['id']}** {trunc(t['title'], 50)}") for t in t_list] +
                [(f"S#{s['id']} {s['title']}", f"scrim:{s['id']}", "Scrim", f"🎯 **#{s['id']}** {trunc(s['title'], 50)}") for s in s_list])

        async def picked(x: discord.Interaction, value: str) -> None:
            kind, eid = value.split(":")
            await self.referee_menu(x, kind, int(eid))
        await PickList(self.bot, i.user.id, brand, "ASSIGN REFEREES", rows, picked, placeholder="Pick an event…", back=self.hub,
                       header="If no referee is assigned to an event, **any** referee can review its results. Assigning restricts review to those referees.",
                       empty="You have no events to assign referees to.").start(i)

    async def referee_menu(self, i: discord.Interaction, kind: str, event_id: int) -> None:
        await self.bot.managed_event(i, kind, event_id)
        assigned = await self.bot.db.read(lambda c: referees_for(c, kind, event_id))
        names = await labels_for(self.bot, i.guild, assigned)
        brand = await self.bot.brand(i.guild_id)

        async def add(x: discord.Interaction, vals: list) -> None:
            perms = await self.bot.perms(x)
            await self.bot.db.run(lambda c: assign_referee(c, kind, event_id, vals[0].id, perms, x.user.id))
            await self.referee_menu(x, kind, event_id)

        async def remove(x: discord.Interaction, vals: list[str]) -> None:
            perms = await self.bot.perms(x)
            await self.bot.db.run(lambda c: unassign_referee(c, kind, event_id, int(vals[0]), perms, x.user.id))
            await self.referee_menu(x, kind, event_id)
        view = BaseView(self.bot, i.user.id)
        view.add_item(UserPicker(add, placeholder="Assign a referee…", row=0))
        if assigned:
            view.add_item(ChoiceSelect([(names[u], str(u), None) for u in assigned[:25]], remove, placeholder="Remove an assigned referee…", row=1))
        add_nav(view, self.referees)
        await show(i, base(brand, "REFEREES", f"{kind.title()} **#{event_id}**\nAssigned: " + (", ".join(f"<@{u}>" for u in assigned) or "none (any referee may review)")), view)

# --------------------------------------------------------------------------------------------------
# cogs/reports.py
# Reports, disputes, appeals and incident cases. Private to the reporter and staff.
# --------------------------------------------------------------------------------------------------

STAFF_ONLY = "Only staff can handle cases."


class Reports(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for name, fn in {"report.start": self.start, "report.mine": self.mine, "report.cases": self.cases}.items():
            self.bot.register_entry(name, fn)

    # ------------------------------------------------------------------ reporter side
    async def start(self, i: discord.Interaction) -> None:
        brand = await self.bot.brand(i.guild_id)
        perms = await self.bot.perms(i)

        async def chosen(x: discord.Interaction, vals: list[str]) -> None:
            await self.report_modal(x, vals[0])
        view = BaseView(self.bot, i.user.id)
        view.add_item(ChoiceSelect([(label, key, None) for key, label in REPORT_KINDS.items()], chosen, placeholder="What do you want to report?", row=0))
        view.add_item(ActionButton("My Cases", self.mine, emoji="🗂️", row=1))
        if can_manage_reports(perms):
            view.add_item(ActionButton("Review Cases", self.cases, emoji="🧰", style=PRIMARY, row=1))
        add_nav(view)
        await show(i, base(brand, "REPORT ISSUE", "Reports are **private** - only you and staff can see them. Pick a type to begin.\n"
                                                         "To dispute a match result, open it under **Match Results → My Submissions**."), view)

    async def report_modal(self, i: discord.Interaction, kind: str) -> None:
        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            target = optional(f["target"], field="Who/what", max_len=120)
            desc = clean(f["desc"], field="Description", min_len=10, max_len=1000, multiline=True)
            rid = await self.bot.db.run(lambda c: create_report(c, x.guild_id, x.user.id, kind, target, desc, parse_evidence(f["urls"])))
            await self.bot.staff_log(x.guild, f"🚩 New **{REPORT_KINDS[kind]}** case **#{rid}** from <@{x.user.id}>.")
            await self.detail(x, rid, self.mine, notice=f"Case **#{rid}** filed. Attach screenshots with `/evidence`.")
        await i.response.send_modal(FormModal(REPORT_KINDS[kind][:45], [
            Field("target", "Who or what is this about?", "Player/team name, event, ID…", required=False, max_length=120),
            Field("desc", "What happened?", "Be specific: what, when, where.", paragraph=True, max_length=1000, min_length=10),
            Field("urls", "Evidence links (up to 3, one per line)", "https://...", required=False, paragraph=True, max_length=600)], submit))

    async def mine(self, i: discord.Interaction) -> None:
        rows_db = await self.bot.db.read(lambda c: list_for_user(c, i.guild_id, i.user.id))
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"#{r['id']} {REPORT_KINDS[r['kind']]}", str(r["id"]), CASE_ICON.get(r["status"], r["status"]),
                 f"{CASE_ICON.get(r['status'], r['status'])} **#{r['id']}** {REPORT_KINDS[r['kind']]} • {trunc(r['description'], 60)}") for r in rows_db]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.detail(x, int(value), self.mine)
        await PickList(self.bot, i.user.id, brand, "MY CASES", rows, picked, placeholder="Open a case…", back=self.start, empty="You have not filed any reports.",
                       refresh=self.mine).start(i)

    # ------------------------------------------------------------------ staff side
    async def cases(self, i: discord.Interaction, status_filter: str = "open") -> None:
        if not await self.bot.require(i, can_manage_reports, STAFF_ONLY):
            return
        statuses = OPEN_STATES if status_filter == "open" else None
        rows_db = await self.bot.db.read(lambda c: list_cases(c, i.guild_id, statuses, 100))
        counts = await self.bot.db.read(lambda c: report_counts(c, i.guild_id))
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"#{r['id']} {REPORT_KINDS[r['kind']]}", str(r["id"]), CASE_ICON.get(r["status"], r["status"]),
                 f"{CASE_ICON.get(r['status'], r['status'])} **#{r['id']}** {REPORT_KINDS[r['kind']]} • <@{r['reporter_id']}>"
                 f"{' → <@' + str(r['assigned_to']) + '>' if r['assigned_to'] else ''}") for r in rows_db]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.detail(x, int(value), lambda y: self.cases(y, status_filter))

        async def toggle(x: discord.Interaction) -> None:
            await self.cases(x, "all" if status_filter == "open" else "open")
        header = " • ".join(f"{CASE_ICON[k]}: {n}" for k, n in counts.items() if k in CASE_ICON) or "No cases yet"
        extra = [ActionButton("Showing open only" if status_filter == "open" else "Showing all", toggle, emoji="🔀", row=2)]
        await PickList(self.bot, i.user.id, brand, "CASE QUEUE", rows, picked, placeholder="Review a case…", back=self.start, header=header,
                       empty="No cases match.", extra=extra, refresh=lambda x: self.cases(x, status_filter)).start(i)

    async def detail(self, i: discord.Interaction, rid: int, back, *, notice: str | None = None) -> None:
        r, notes = await self.bot.db.read(lambda c: (report_get(c, rid), report_notes(c, rid)))
        perms = await self.bot.perms(i)
        staff = can_manage_reports(perms)
        if r["guild_id"] != i.guild_id or not (staff or r["reporter_id"] == i.user.id):
            raise ServiceError("That case is private.")
        brand = await self.bot.brand(i.guild_id)
        view = BaseView(self.bot, i.user.id)
        if staff:
            if r["status"] != "RESOLVED":
                view.add_item(ActionButton("Assign to me", lambda x: self._assign(x, rid, back), emoji="🙋", row=0, disabled=r["assigned_to"] == i.user.id))
                view.add_item(ActionButton("Assign…", lambda x: self._assign_picker(x, rid, back), emoji="👤", row=0))
                view.add_item(ActionButton("Request evidence", lambda x: self._request_evidence(x, rid, back), emoji="📎", row=0, disabled=r["status"] == "NEED_EVIDENCE"))
                view.add_item(ActionButton("Add note", lambda x: self._note_modal(x, rid, back), emoji="📝", row=1))
                view.add_item(ActionButton("Resolve", lambda x: self._resolve_modal(x, rid, back), emoji="✅", style=SUCCESS, row=1))
            else:
                view.add_item(ActionButton("Reopen", lambda x: self._reopen(x, rid, back), emoji="🔁", style=PRIMARY, row=1))
        elif r["status"] != "RESOLVED":
            view.add_item(ActionButton("Add evidence link", lambda x: self._evidence_modal(x, rid, back), emoji="📎", row=0))
        view.add_item(ActionButton("Refresh", lambda x: self.detail(x, rid, back), emoji="🔄", row=1))
        add_nav(view, back)
        await show(i, case_embed(brand, r, notes, staff_view=staff), view)
        if notice:
            await i.followup.send(embed=embed_notice(notice, "ok"), ephemeral=True)

    async def _require_staff(self, i: discord.Interaction):
        return await self.bot.require(i, can_manage_reports, STAFF_ONLY)

    async def _assign(self, i: discord.Interaction, rid: int, back) -> None:
        if not await self._require_staff(i):
            return
        await self.bot.db.run(lambda c: assign(c, rid, i.user.id, i.user.id))
        await self.detail(i, rid, back)

    async def _assign_picker(self, i: discord.Interaction, rid: int, back) -> None:
        if not await self._require_staff(i):
            return

        async def picked(x: discord.Interaction, vals: list) -> None:
            if not await self._require_staff(x):
                return
            target = vals[0]
            tp = await self.bot.db.read(lambda c: get_perms_sync(c, guild_id=x.guild_id, user_id=target.id, owner_id=self.bot.config.owner_id,
                                                                    discord_admin=isinstance(target, discord.Member) and target.guild_permissions.administrator,
                                                                    role_ids=[r.id for r in getattr(target, "roles", [])]))
            if not can_manage_reports(tp):
                raise ServiceError("That member is not staff.")
            await self.bot.db.run(lambda c: assign(c, rid, target.id, x.user.id))
            await self.bot.dm(target.id, content=f"📌 Case **#{rid}** was assigned to you.")
            await self.detail(x, rid, back)
        view = BaseView(self.bot, i.user.id)
        view.add_item(UserPicker(picked, placeholder="Pick a staff member", row=0))
        add_nav(view, lambda x: self.detail(x, rid, back))
        await show(i, notice("Choose who should handle this case."), view)

    async def _request_evidence(self, i: discord.Interaction, rid: int, back) -> None:
        if not await self._require_staff(i):
            return
        r = await self.bot.db.run(lambda c: (request_evidence(c, rid, i.user.id), report_get(c, rid))[1])
        await self.bot.dm(r["reporter_id"], content=f"📎 Staff need more evidence for your case **#{rid}**. Use `/evidence` (screenshot) or open **Report Issue → My Cases → Add evidence link**.")
        await self.detail(i, rid, back)

    async def _note_modal(self, i: discord.Interaction, rid: int, back) -> None:
        if not await self._require_staff(i):
            return

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not await self._require_staff(x):
                return
            note = clean(f["note"], field="Note", min_len=2, max_len=900, multiline=True)
            internal = parse_bool(f["internal"], field="Internal", default=True)
            await self.bot.db.run(lambda c: add_note(c, rid, x.user.id, note, internal))
            if not internal:
                r = await self.bot.db.read(lambda c: report_get(c, rid))
                await self.bot.dm(r["reporter_id"], content=f"💬 Staff replied on case **#{rid}**: {trunc(note, 500)}")
            await self.detail(x, rid, back)
        await i.response.send_modal(FormModal("Add note", [Field("note", "Note", paragraph=True, max_length=900), Field("internal", "Internal only? (yes/no)", max_length=3, default="yes")], submit))

    async def _resolve_modal(self, i: discord.Interaction, rid: int, back) -> None:
        if not await self._require_staff(i):
            return
        r = await self.bot.db.read(lambda c: report_get(c, rid))
        frozen = False
        if r["result_id"]:
            frozen = await self.bot.db.read(lambda c: (c.execute("SELECT status FROM match_results WHERE id=?", (r["result_id"],)).fetchone() or {}).get("status") == "DISPUTED")

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not await self._require_staff(x):
                return
            reason = clean(f["reason"], field="Resolution", min_len=3, max_len=800, multiline=True)
            action = None
            if frozen:
                action = parse_choice(f["action"], field="Result action", choices={"uphold": "uphold", "keep": "uphold", "void": "reject", "reject": "reject"})
            done = await self.bot.db.run(lambda c: resolve(c, rid, x.user.id, reason, result_action=action))
            await self.bot.dm(done["reporter_id"], content=f"✅ Your case **#{rid}** was resolved: {trunc(reason, 600)}")
            await self.detail(x, rid, back)
        fields = [Field("reason", "Resolution (shown to the reporter)", paragraph=True, max_length=800, min_length=3)]
        if frozen:
            fields.append(Field("action", "Disputed result: uphold or void?", "uphold / void", max_length=6))
        await i.response.send_modal(FormModal("Resolve case", fields, submit))

    async def _reopen(self, i: discord.Interaction, rid: int, back) -> None:
        if not await self._require_staff(i):
            return
        await self.bot.db.run(lambda c: reopen(c, rid, i.user.id))
        await self.detail(i, rid, back)

    async def _evidence_modal(self, i: discord.Interaction, rid: int, back) -> None:
        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            urls = parse_evidence(f["urls"])
            if not urls:
                raise ValidationError("Paste at least one link.")
            await self.bot.db.run(lambda c: report_add_evidence(c, rid, x.user.id, urls, staff=False))
            await self.detail(x, rid, back)
        await i.response.send_modal(FormModal("Add evidence", [Field("urls", "Links (one per line)", paragraph=True, max_length=600)], submit))

# --------------------------------------------------------------------------------------------------
# cogs/community.py
# Community features: announcements, recruitment, polls, giveaways, calendar, badges, hall of fame.
# --------------------------------------------------------------------------------------------------

BAR = 12


def poll_embed(brand: Branding, t: dict) -> discord.Embed:
    total = t["total"]
    lines = []
    for n, (opt, c) in enumerate(zip(t["options"], t["counts"]), 1):
        filled = round(BAR * c / total) if total else 0
        lines.append(f"**{n}. {opt}**\n`{'█' * filled}{'░' * (BAR - filled)}` {c} ({(100 * c // total) if total else 0}%)")
    e = base(brand, "POLL", f"## {t['poll']['question']}\n\n" + "\n".join(lines))
    e.set_footer(text=f"{brand.footer[:150]} • {total} vote(s)" + (" • CLOSED" if t["poll"]["closed"] else " • one vote each, change any time"))
    return e


def giveaway_embed(brand: Branding, g: dict, entries: int, winners: list[int] | None = None) -> discord.Embed:
    e = base(brand, "GIVEAWAY", f"## 🎁 {g['prize']}")
    e.add_field(name="Ends", value=ts(g["ends_at"], "R"))
    e.add_field(name="Winners", value=str(g["winners"]))
    e.add_field(name="Entries", value=str(entries))
    req = [x for x, on in (("be in a team", g["require_team"]), ("have a player profile", g["require_profile"])) if on]
    e.add_field(name="Requirements", value=("You must " + " and ".join(req)) if req else "Everyone can enter", inline=False)
    e.add_field(name="Host", value=f"<@{g['host_id']}>")
    if winners is not None:
        e.add_field(name="🏆 Winners", value=", ".join(f"<@{u}>" for u in winners) or "No eligible entries", inline=False)
        e.color = discord.Color(0x2ECC71)
    return e


class PollButton(discord.ui.DynamicItem[discord.ui.Button], template=r"aegis:poll:(?P<id>[0-9]+):(?P<idx>[0-9])"):
    def __init__(self, poll_id: int, idx: int, label: str = "Vote") -> None:
        super().__init__(discord.ui.Button(label=trunc(f"{idx + 1}. {label}", 80), style=discord.ButtonStyle.primary, custom_id=f"aegis:poll:{poll_id}:{idx}"))
        self.poll_id, self.idx = poll_id, idx

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):  # type: ignore[override]
        return cls(int(match["id"]), int(match["idx"]))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        problem = interaction.client.gate(interaction)  # type: ignore[attr-defined]
        if problem:
            await interaction.response.send_message(embed=notice(problem, "warn"), ephemeral=True)
        return problem is None

    async def callback(self, interaction: discord.Interaction) -> None:
        bot: AegisBot = interaction.client  # type: ignore[assignment]
        t = await bot.db.run(lambda c: vote(c, self.poll_id, interaction.user.id, self.idx))
        brand = await bot.brand(interaction.guild_id)
        await interaction.response.edit_message(embed=poll_embed(brand, t))

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        await interaction.client.report_error(interaction, error, "poll-button")  # type: ignore[attr-defined]


class GiveawayButton(discord.ui.DynamicItem[discord.ui.Button], template=r"aegis:gw:(?P<id>[0-9]+)"):
    def __init__(self, giveaway_id: int) -> None:
        super().__init__(discord.ui.Button(label="Enter giveaway", style=discord.ButtonStyle.success, emoji="🎁", custom_id=f"aegis:gw:{giveaway_id}"))
        self.giveaway_id = giveaway_id

    @classmethod
    async def from_custom_id(cls, interaction, item, match, /):  # type: ignore[override]
        return cls(int(match["id"]))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        problem = interaction.client.gate(interaction)  # type: ignore[attr-defined]
        if problem:
            await interaction.response.send_message(embed=notice(problem, "warn"), ephemeral=True)
        return problem is None

    async def callback(self, interaction: discord.Interaction) -> None:
        bot: AegisBot = interaction.client  # type: ignore[assignment]
        n = await bot.db.run(lambda c: enter_giveaway(c, self.giveaway_id, interaction.user.id))
        g = await bot.db.read(lambda c: c.execute("SELECT * FROM giveaways WHERE id=?", (self.giveaway_id,)).fetchone())
        brand = await bot.brand(interaction.guild_id)
        await interaction.response.edit_message(embed=giveaway_embed(brand, g, n))
        await interaction.followup.send(embed=notice("You're in! Good luck 🍀", "ok"), ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        await interaction.client.report_error(interaction, error, "giveaway-button")  # type: ignore[attr-defined]


class Community(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        self.bot.add_dynamic_items(PollButton, GiveawayButton)
        for name, fn in {"community": self.hub, "community.announce": self.announce, "community.recruit": self.recruit, "community.polls": self.polls,
                         "community.giveaways": self.giveaways, "community.calendar": self.calendar, "community.badges": self.badges,
                         "community.hof": self.hof_add}.items():
            self.bot.register_entry(name, fn)

    async def _channel_for(self, guild_id: int, key: str) -> int:
        s = await self.bot.settings(guild_id)
        return int(s.get(key) or 0) or int(s.get("ch_esports") or 0)

    async def _send_public(self, i: discord.Interaction, key: str, embed: discord.Embed, view: discord.ui.View | None = None) -> discord.Message | None:
        cid = await self._channel_for(i.guild_id, key)
        msg = await self.bot.post(i.guild, cid, embed=embed, view=view)
        if msg is None and i.channel is not None and hasattr(i.channel, "send"):
            try:  # fall back to the channel where the staff member is working
                kw = {"embed": embed}
                if view is not None:
                    kw["view"] = view
                msg = await i.channel.send(**kw)  # type: ignore[union-attr]
            except discord.HTTPException:
                msg = None
        return msg

    async def hub(self, i: discord.Interaction) -> None:
        perms = await self.bot.perms(i)
        brand = await self.bot.brand(i.guild_id)
        items = [("Event Calendar", "🗓️", self.calendar, PRIMARY), ("Recruitment Board", "📣", self.recruit, SECONDARY), ("Polls", "📊", self.polls, SECONDARY),
                 ("Giveaways", "🎁", self.giveaways, SECONDARY), ("Hall of Fame", "👑", lambda x: self.bot.open(x, "lb.hall"), SECONDARY),
                 ("Season Rankings", "📈", lambda x: self.bot.open(x, "lb.season"), SECONDARY)]
        if can_post_community(perms):
            items += [("Post Announcement", "📢", self.announce, SUCCESS), ("Award Badge", "🏅", self.badges, SECONDARY), ("Add Hall of Fame", "🏆", self.hof_add, SECONDARY)]
        await show(i, base(brand, "COMMUNITY", "News, recruitment, polls, giveaways and the event calendar."), MenuView(self.bot, i.user.id, items, back=None))

    # ------------------------------------------------------------------ calendar
    async def calendar(self, i: discord.Interaction) -> None:
        rows = await self.bot.db.read(lambda c: calendar(c, i.guild_id, now_iso()))
        brand = await self.bot.brand(i.guild_id)
        lines = [f"{'🏆' if r['kind'] == 'Tournament' else '🎯'} {ts(r['start_time'], 'f')} • **{trunc(r['title'], 40)}** • {r['game']} (#{r['id']})" for r in rows]
        await Pager(self.bot, i.user.id, list_pages(brand, "EVENT CALENDAR", lines, per_page=10, empty="No upcoming events are scheduled."), back=self.hub, refresh=self.calendar).start(i)

    # ------------------------------------------------------------------ announcements
    async def announce(self, i: discord.Interaction) -> None:
        if not await self.bot.require(i, can_post_community, "Only staff and organizers can post announcements."):
            return
        brand = await self.bot.brand(i.guild_id)
        kinds = {"general": ("General news", "ch_esports"), "tournament": ("Tournament news", "ch_tournaments"), "scrim": ("Scrim news", "ch_scrims")}

        async def chosen(x: discord.Interaction, vals: list[str]) -> None:
            label, key = kinds[vals[0]]

            async def submit(y: discord.Interaction, f: dict[str, str]) -> None:
                if not can_post_community(await self.bot.perms(y)):
                    raise ServiceError("Only staff and organizers can post announcements.")
                title = clean(f["title"], field="Title", min_len=3, max_len=100)
                body = clean(f["body"], field="Message", min_len=3, max_len=1800, multiline=True)
                e = base(brand, label.upper(), f"## {title}\n\n{body}")
                e.set_footer(text=f"{brand.footer[:150]} • posted by {y.user.display_name}")
                msg = await self._send_public(y, key, e)
                if msg is None:
                    raise ServiceError("I couldn't post anywhere. Set an announcements channel in Admin → Channels and check my permissions.")
                await self.bot.db.run(lambda c: record_announcement(c, y.guild_id, y.user.id, vals[0], title, body, msg.channel.id, msg.id))
                await say(y, f"Posted in {msg.channel.mention}.", "ok")
            await x.response.send_modal(FormModal(f"{label}", [Field("title", "Title", max_length=100), Field("body", "Message", paragraph=True, max_length=1800)], submit))
        view = BaseView(self.bot, i.user.id)
        view.add_item(ChoiceSelect([(l, k, None) for k, (l, _) in kinds.items()], chosen, placeholder="What kind of announcement?", row=0))
        add_nav(view, self.hub)
        await show(i, base(brand, "ANNOUNCEMENT", "Announcements go to the channel configured for that type. Mentions in the text do **not** ping anyone."), view)

    # ------------------------------------------------------------------ recruitment
    async def recruit(self, i: discord.Interaction, kind: str | None = None) -> None:
        brand = await self.bot.brand(i.guild_id)
        if kind is None:
            view = MenuView(self.bot, i.user.id, [("Teams Recruiting", "🛡️", lambda x: self.recruit(x, "team"), PRIMARY), ("Players Looking", "🧑", lambda x: self.recruit(x, "player"), PRIMARY),
                                                  ("Post Team Recruitment", "➕", self.post_team, SECONDARY), ("Post: I'm looking for a team", "➕", self.post_player, SECONDARY),
                                                  ("My Posts", "🗂️", self.my_posts, SECONDARY)], back=self.hub)
            await show(i, base(brand, "RECRUITMENT BOARD", "Find teammates or a team. Posts are public to server members."), view)
            return
        posts = await self.bot.db.read(lambda c: list_posts(c, i.guild_id, kind))
        rows = []
        for p in posts:
            who = p["team_name"] if kind == "team" else None
            rows.append(((who or f"Player {p['user_id']}")[:100], str(p["id"]), f"{p['game'] or 'Any'} • {p['role'] or '—'}"[:100],
                         f"{'🛡️ **' + who + '**' if who else '🧑 <@' + str(p['user_id']) + '>'} • {p['game'] or 'any game'} • {p['role'] or '—'} • {p['region'] or '—'}\n┗ {trunc(p['body'], 100)}"))

        async def picked(x: discord.Interaction, value: str) -> None:
            p = next((y for y in posts if str(y["id"]) == value), None)
            if p is None:
                await self.recruit(x, kind)
                return
            view2 = BaseView(self.bot, x.user.id)
            mine = await self.bot.db.read(lambda c: team_of_user(c, x.guild_id, x.user.id))
            if kind == "team" and p["team_id"]:
                view2.add_item(ActionButton("View Team", lambda y: self.bot.get_cog("Teams").team_detail(y, p["team_id"], back_handler=lambda z: self.recruit(z, kind)), emoji="🛡️", style=PRIMARY))
            if kind == "player":
                view2.add_item(ActionButton("View Profile", lambda y: self.bot.get_cog("Players").player_detail(y, p["user_id"], mine, lambda z: self.recruit(z, kind)), emoji="🧑", style=PRIMARY))
            add_nav(view2, lambda y: self.recruit(y, kind))
            await show(x, base(brand, "RECRUITMENT", f"<@{p['user_id']}> • {p['game'] or 'any game'} • {p['role'] or '—'} • {p['region'] or '—'}\n\n{p['body']}"), view2)
        await PickList(self.bot, i.user.id, brand, "TEAMS RECRUITING" if kind == "team" else "PLAYERS LOOKING", rows, picked, placeholder="Open a post…", back=self.recruit,
                       per_page=6, empty="No active posts yet.", refresh=lambda x: self.recruit(x, kind)).start(i)

    async def _post_modal(self, i: discord.Interaction, kind: str) -> None:
        games = await self.bot.db.read(lambda c: get_games(c, i.guild_id))
        brand = await self.bot.brand(i.guild_id)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            game = resolve_game(games, f["game"]) if f["game"].strip() else None
            role = optional(f["role"], field="Role", max_len=30)
            region = optional(f["region"], field="Region", max_len=30)
            body = clean(f["body"], field="Message", min_len=5, max_len=500, multiline=True)
            fn = post_team_recruitment if kind == "team" else post_player_recruitment
            pid = await self.bot.db.run(lambda c: fn(c, x.guild_id, x.user.id, game, role, region, body))
            cid = await self.bot.db.read(lambda c: int(get_setting(c, x.guild_id, "ch_recruit") or 0))
            if cid:
                team = await self.bot.db.read(lambda c: team_of_user(c, x.guild_id, x.user.id))
                head = f"🛡️ **{team['name']}** is recruiting" if kind == "team" and team else f"🧑 <@{x.user.id}> is looking for a team"
                await self.bot.post(x.guild, cid, embed=base(brand, "RECRUITMENT", f"{head}\n{game or 'Any game'} • {role or '—'} • {region or '—'}\n\n{body}"))
            await say(x, f"Posted (#{pid}). Manage it under **My Posts**.", "ok")
        await i.response.send_modal(FormModal("Recruitment post", [
            Field("game", "Game", games[0], required=False, max_length=30), Field("role", "Role needed / your role", "IGL, Sniper…", required=False, max_length=30),
            Field("region", "Region", required=False, max_length=30), Field("body", "Message", paragraph=True, max_length=500, min_length=5)], submit))

    async def post_team(self, i: discord.Interaction) -> None:
        team = await self.bot.db.read(lambda c: team_of_user(c, i.guild_id, i.user.id))
        if not team or team["my_role"] not in ("captain", "cocaptain"):
            await say(i, "Only a team captain or co-captain can post team recruitment.", "error")
            return
        await self._post_modal(i, "team")

    async def post_player(self, i: discord.Interaction) -> None:
        await self._post_modal(i, "player")

    async def my_posts(self, i: discord.Interaction) -> None:
        posts = [p for p in await self.bot.db.read(lambda c: list_posts(c, i.guild_id, None, 200)) if p["user_id"] == i.user.id]
        perms = await self.bot.perms(i)
        brand = await self.bot.brand(i.guild_id)
        rows = [(f"#{p['id']} {p['kind']} post", str(p["id"]), trunc(p["body"], 90), f"#{p['id']} **{p['kind']}** • {trunc(p['body'], 80)}") for p in posts]

        async def picked(x: discord.Interaction, value: str) -> None:
            async def yes(y: discord.Interaction) -> None:
                await self.bot.db.run(lambda c: close_post(c, int(value), y.user.id, staff=perms.is_staff))
                await self.my_posts(y)
            await confirm(x, self.bot, f"Close post #{value}?", yes, yes_label="Close post", on_no=self.my_posts)
        await PickList(self.bot, i.user.id, brand, "MY POSTS", rows, picked, placeholder="Close a post…", back=self.recruit, empty="You have no active posts.").start(i)

    # ------------------------------------------------------------------ polls
    async def polls(self, i: discord.Interaction) -> None:
        perms = await self.bot.perms(i)
        brand = await self.bot.brand(i.guild_id)
        rows_db = await self.bot.db.read(lambda c: c.execute("SELECT * FROM polls WHERE guild_id=? ORDER BY id DESC LIMIT 30", (i.guild_id,)).fetchall())
        rows = [(f"#{p['id']} {p['question']}", str(p["id"]), "Closed" if p["closed"] else "Open", f"{'🔒' if p['closed'] else '🟢'} **#{p['id']}** {trunc(p['question'], 70)}") for p in rows_db]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.poll_detail(x, int(value))
        extra = []
        if can_post_community(perms):
            extra.append(ActionButton("Create Poll", self.poll_create, emoji="➕", style=SUCCESS, row=2))
        await PickList(self.bot, i.user.id, brand, "POLLS", rows, picked, placeholder="Open a poll…", back=self.hub, empty="No polls yet.", extra=extra).start(i)

    async def poll_detail(self, i: discord.Interaction, pid: int) -> None:
        t = await self.bot.db.read(lambda c: tally(c, pid))
        perms = await self.bot.perms(i)
        brand = await self.bot.brand(i.guild_id)
        view = BaseView(self.bot, i.user.id)
        p = t["poll"]

        def vote_for(idx: int):
            async def go(x: discord.Interaction) -> None:
                await self.bot.db.run(lambda c: vote(c, pid, x.user.id, idx))
                await self.poll_detail(x, pid)
            return go
        if not p["closed"]:
            for idx, opt in enumerate(t["options"]):
                view.add_item(ActionButton(trunc(f"{idx + 1}. {opt}", 80), vote_for(idx), style=PRIMARY, row=idx // 3))
            if p["creator_id"] == i.user.id or perms.is_staff:
                async def close(x: discord.Interaction) -> None:
                    await self.bot.db.run(lambda c: close_poll(c, pid, x.user.id, staff=perms.is_staff))
                    await self._close_message(x, pid)
                    await self.poll_detail(x, pid)
                view.add_item(ActionButton("Close poll", close, emoji="🔒", style=DANGER, row=2))
        add_nav(view, self.polls)
        await show(i, poll_embed(brand, t), view)

    async def _close_message(self, i: discord.Interaction, pid: int) -> None:
        p = await self.bot.db.read(lambda c: c.execute("SELECT * FROM polls WHERE id=?", (pid,)).fetchone())
        if p and p["channel_id"] and p["message_id"] and i.guild:
            ch = i.guild.get_channel(p["channel_id"])
            if ch is not None and hasattr(ch, "fetch_message"):
                try:
                    msg = await ch.fetch_message(p["message_id"])  # type: ignore[union-attr]
                    t = await self.bot.db.read(lambda c: tally(c, pid))
                    await msg.edit(embed=poll_embed(await self.bot.brand(i.guild_id), t), view=None)
                except discord.HTTPException:
                    pass

    async def poll_create(self, i: discord.Interaction) -> None:
        if not await self.bot.require(i, can_post_community, "Only staff and organizers can create polls."):
            return

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not can_post_community(await self.bot.perms(x)):
                raise ServiceError("Only staff and organizers can create polls.")
            q = clean(f["q"], field="Question", min_len=5, max_len=250)
            opts = [clean(o, field="Option", max_len=80) for o in f["opts"].splitlines() if o.strip()]
            pid = await self.bot.db.run(lambda c: create_poll(c, x.guild_id, x.user.id, q, opts))
            t = await self.bot.db.read(lambda c: tally(c, pid))
            view = discord.ui.View(timeout=None)
            for idx, opt in enumerate(t["options"]):
                view.add_item(PollButton(pid, idx, opt))
            msg = await self._send_public(x, "ch_esports", poll_embed(await self.bot.brand(x.guild_id), t), view)
            if msg is None:
                await self.bot.db.run(lambda c: c.execute("DELETE FROM polls WHERE id=?", (pid,)))
                raise ServiceError("I couldn't post the poll. Set a community channel in Admin → Channels and check my permissions.")
            await self.bot.db.run(lambda c: c.execute("UPDATE polls SET channel_id=?, message_id=? WHERE id=?", (msg.channel.id, msg.id, pid)))
            await say(x, f"Poll posted in {msg.channel.mention}.", "ok")
        await i.response.send_modal(FormModal("Create poll", [Field("q", "Question", max_length=250), Field("opts", "Options (one per line, 2-5)", paragraph=True, max_length=400)], submit))

    # ------------------------------------------------------------------ giveaways
    async def giveaways(self, i: discord.Interaction) -> None:
        perms = await self.bot.perms(i)
        brand = await self.bot.brand(i.guild_id)
        rows_db = await self.bot.db.read(lambda c: c.execute("SELECT * FROM giveaways WHERE guild_id=? ORDER BY id DESC LIMIT 20", (i.guild_id,)).fetchall())
        lines = [f"{'✅' if g['ended'] else '🟢'} **#{g['id']}** {trunc(g['prize'], 50)} • {'ended' if g['ended'] else 'ends ' + ts(g['ends_at'], 'R')}" for g in rows_db]
        view = BaseView(self.bot, i.user.id)
        if can_post_community(perms):
            view.add_item(ActionButton("Create Giveaway", self.giveaway_create, emoji="➕", style=SUCCESS))
            live = [g for g in rows_db if not g["ended"]]
            if live:
                async def end_pick(x: discord.Interaction, vals: list[str]) -> None:
                    await self.end_giveaway_now(x, int(vals[0]))
                view.add_item(ChoiceSelect([(f"#{g['id']} {g['prize']}", str(g["id"]), "End now & draw") for g in live[:25]], end_pick, placeholder="End a giveaway now…", row=1))
        add_nav(view, self.hub)
        await show(i, base(brand, "GIVEAWAYS", "\n".join(lines) or "No giveaways yet. Enter by pressing the button on a giveaway message."), view)

    async def giveaway_create(self, i: discord.Interaction) -> None:
        if not await self.bot.require(i, can_post_community, "Only staff and organizers can host giveaways."):
            return

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not can_post_community(await self.bot.perms(x)):
                raise ServiceError("Only staff and organizers can host giveaways.")
            prize = clean(f["prize"], field="Prize", min_len=2, max_len=200)
            ends = to_iso(utcnow() + parse_duration(f["dur"]))
            winners = parse_int(f["winners"], field="Winners", lo=1, hi=20, default=1)
            req_team = parse_bool(f["req"].split(",")[0] if f["req"] else "", field="Require team", default=False)
            req_profile = parse_bool(f["req"].split(",")[1] if "," in f["req"] else "", field="Require profile", default=False)
            gid = await self.bot.db.run(lambda c: create_giveaway(c, x.guild_id, x.user.id, prize, ends, winners, req_team, req_profile))
            g = await self.bot.db.read(lambda c: c.execute("SELECT * FROM giveaways WHERE id=?", (gid,)).fetchone())
            view = discord.ui.View(timeout=None)
            view.add_item(GiveawayButton(gid))
            msg = await self._send_public(x, "ch_esports", giveaway_embed(await self.bot.brand(x.guild_id), g, 0), view)
            if msg is None:
                await self.bot.db.run(lambda c: c.execute("DELETE FROM giveaways WHERE id=?", (gid,)))
                raise ServiceError("I couldn't post the giveaway. Set a community channel in Admin → Channels and check my permissions.")
            await self.bot.db.run(lambda c: c.execute("UPDATE giveaways SET channel_id=?, message_id=? WHERE id=?", (msg.channel.id, msg.id, gid)))
            await say(x, f"Giveaway posted in {msg.channel.mention}.", "ok")
        await i.response.send_modal(FormModal("Create giveaway", [
            Field("prize", "Prize", max_length=200), Field("dur", "Duration", "e.g. 2h, 1d, 1h30m", max_length=10),
            Field("winners", "Winners", "1", max_length=2, default="1"),
            Field("req", "Require: team?, profile? (yes/no, yes/no)", "no,no", required=False, max_length=10, default="no,no")], submit))

    async def end_giveaway_now(self, i: discord.Interaction, gid: int) -> None:
        if not await self.bot.require(i, can_post_community, "Only staff and organizers can end giveaways."):
            return
        await finish_giveaway(self.bot, gid)
        await self.giveaways(i)

    # ------------------------------------------------------------------ badges / hall of fame
    async def badges(self, i: discord.Interaction) -> None:
        if not await self.bot.require(i, can_post_community, "Only staff can award badges."):
            return
        brand = await self.bot.brand(i.guild_id)

        async def player_picked(x: discord.Interaction, vals: list) -> None:
            user = vals[0]

            async def submit(y: discord.Interaction, f: dict[str, str]) -> None:
                if not can_post_community(await self.bot.perms(y)):
                    raise ServiceError("Only staff can award badges.")
                badge = clean(f["badge"], field="Badge", min_len=2, max_len=40)
                await self.bot.db.run(lambda c: award_badge(c, y.guild_id, y.user.id, badge, user_id=user.id, note=optional(f["note"], field="Note", max_len=200)))
                await self.bot.dm(user.id, content=f"🏅 You were awarded the **{badge}** badge in **{y.guild.name}**!")
                await say(y, f"Awarded **{badge}** to <@{user.id}>.", "ok")
            await x.response.send_modal(FormModal("Award badge", [Field("badge", "Badge name", "e.g. Season 1 MVP", max_length=40), Field("note", "Note (optional)", required=False, max_length=200)], submit))

        async def team_modal(x: discord.Interaction) -> None:
            async def submit(y: discord.Interaction, f: dict[str, str]) -> None:
                if not can_post_community(await self.bot.perms(y)):
                    raise ServiceError("Only staff can award badges.")
                team = await self.bot.db.read(lambda c: find_team_by_name(c, y.guild_id, f["team"]))
                if not team:
                    raise ServiceError("No team with that exact name.")
                badge = clean(f["badge"], field="Badge", min_len=2, max_len=40)
                await self.bot.db.run(lambda c: award_badge(c, y.guild_id, y.user.id, badge, team_id=team["id"]))
                await say(y, f"Awarded **{badge}** to **{team['name']}**.", "ok")
            await x.response.send_modal(FormModal("Award team badge", [Field("team", "Team name (exact)", max_length=32), Field("badge", "Badge name", max_length=40)], submit))
        view = BaseView(self.bot, i.user.id)
        view.add_item(UserPicker(player_picked, placeholder="Award a badge to a player…", row=0))
        view.add_item(ActionButton("Award to a team", team_modal, emoji="🛡️", row=1))
        add_nav(view, self.hub)
        await show(i, base(brand, "AWARD BADGE", "Badges are manual awards by staff and appear on profiles and team pages."), view)

    async def hof_add(self, i: discord.Interaction) -> None:
        if not await self.bot.require(i, can_post_community, "Only staff can add Hall of Fame entries."):
            return
        season = (await self.bot.settings(i.guild_id)).get("season")

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not can_post_community(await self.bot.perms(x)):
                raise ServiceError("Only staff can add Hall of Fame entries.")
            title = clean(f["title"], field="Title", min_len=2, max_len=100)
            winner = clean(f["winner"], field="Winner", min_len=1, max_len=150)
            season_v = optional(f["season"], field="Season", max_len=30)
            note = optional(f["note"], field="Note", max_len=200)
            await self.bot.db.run(lambda c: add_hall_of_fame(c, x.guild_id, x.user.id, title, winner, season_v, note))
            await self.bot.open(x, "lb.hall")
        await i.response.send_modal(FormModal("Hall of Fame entry", [Field("title", "Title", "e.g. Winter Cup champions", max_length=100), Field("winner", "Winner(s)", max_length=150),
                                                                    Field("season", "Season", required=False, max_length=30, default=season), Field("note", "Note", required=False, max_length=200)], submit))


async def finish_giveaway(bot: AegisBot, gid: int) -> list[int]:
    """Draw winners (idempotent), update the message and announce. Used by the button flow and the background loop."""
    winners = await bot.db.run(lambda c: end_giveaway(c, gid))
    g = await bot.db.read(lambda c: c.execute("SELECT * FROM giveaways WHERE id=?", (gid,)).fetchone())
    if not g:
        return winners
    guild = bot.get_guild(g["guild_id"])
    brand = await bot.brand(g["guild_id"])
    n = await bot.db.read(lambda c: giveaway_entries(c, gid))
    if guild and g["channel_id"] and g["message_id"]:
        ch = guild.get_channel(g["channel_id"])
        if ch is not None and hasattr(ch, "fetch_message"):
            try:
                msg = await ch.fetch_message(g["message_id"])  # type: ignore[union-attr]
                await msg.edit(embed=giveaway_embed(brand, g, n, winners), view=None)
                await ch.send(f"🎉 Giveaway **{g['prize']}** ended! " + (("Winners: " + ", ".join(f"<@{u}>" for u in winners)) if winners else "No eligible entries."))  # type: ignore[union-attr]
            except discord.HTTPException:
                pass
    for u in winners:
        await bot.dm(u, content=f"🎉 You won **{g['prize']}**! Contact the staff to claim it.")
    return winners

# --------------------------------------------------------------------------------------------------
# cogs/admin.py
# Administrator panel: configuration (all persisted in SQLite), scoring rules, modules, audit log, export, backups.
# --------------------------------------------------------------------------------------------------

ADMIN_ONLY = "Only administrators can use the admin panel."
EXPORTS = {
    "teams": ("Teams", "SELECT id,name,tag,game,verified,recruiting,captain_id,disbanded,created_at FROM teams WHERE guild_id=?"),
    "players": ("Players (no game UIDs)", "SELECT user_id,ign,role,region,language,games,status,created_at FROM player_profiles WHERE guild_id=?"),
    "tournaments": ("Tournaments", "SELECT id,title,game,mode,team_size,map,region,start_time,entry_fee,prize,max_teams,status,season,organizer_id FROM tournaments WHERE guild_id=?"),
    "scrims": ("Scrims", "SELECT id,title,game,mode,tier,map,fee,start_time,lobby_count,lobby_capacity,status,season,organizer_id FROM scrims WHERE guild_id=?"),
    "results": ("Approved results", "SELECT m.kind,m.event_id,m.number,t.name AS team,r.placement,r.kills,r.bonus,r.score_for,r.score_against,r.reviewed_at FROM match_results r "
                                    "JOIN matches m ON m.id=r.match_id JOIN teams t ON t.id=r.team_id WHERE m.guild_id=? AND r.status='APPROVED'"),
    "standings": ("Standings", "SELECT s.kind,s.event_id,t.name AS team,s.rank,s.played,s.points,s.kills,s.wins FROM standings s JOIN teams t ON t.id=s.team_id WHERE t.guild_id=?"),
    "audit": ("Audit log (last 2000)", "SELECT id,actor_id,action,target,detail,created_at FROM audit_logs WHERE guild_id=? ORDER BY id DESC LIMIT 2000"),
}


def bytes_h(n: int) -> str:
    return f"{n / 1024:.1f} KB" if n < 1024 * 1024 else f"{n / 1024 / 1024:.2f} MB"


class Admin(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for name, fn in {"admin": self.hub, "admin.backups": self.backups}.items():
            self.bot.register_entry(name, fn)

    async def _need_admin(self, i: discord.Interaction) -> bool:
        return await self.bot.require(i, lambda p: p.is_admin, ADMIN_ONLY) is not None

    # ------------------------------------------------------------------ hub
    async def hub(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        brand = await self.bot.brand(i.guild_id)
        s = await self.bot.settings(i.guild_id)
        unset = [CHANNEL_KEYS[k] for k in CHANNEL_KEYS if not int(s.get(k) or 0)]
        text = "Server configuration is stored in the database and survives restarts."
        if unset:
            text += f"\n\n⚠️ Not configured: {', '.join(unset[:4])}{'…' if len(unset) > 4 else ''}"
        items = [("Publish Main Panel", "📌", self.publish_panel, SUCCESS), ("Channels", "#️⃣", self.channels, PRIMARY), ("Roles", "🎭", self.roles, PRIMARY),
                 ("Games", "🎮", self.games, SECONDARY), ("Limits & Check-in", "📏", self.limits, SECONDARY), ("Registration", "📝", self.registration, SECONDARY),
                 ("Scoring Rules", "🧮", self.scoring, SECONDARY), ("Branding", "🎨", self.branding, SECONDARY), ("Season & Timezone", "🕒", self.season, SECONDARY),
                 ("Modules", "🧩", self.modules, SECONDARY), ("Staff Roster", "👥", lambda x: self.bot.open(x, "staff.members"), SECONDARY),
                 ("Applications", "📨", lambda x: self.bot.open(x, "staff.applications"), SECONDARY), ("Database", "🗄️", self.database, SECONDARY),
                 ("Backups", "💾", self.backups, SECONDARY), ("Audit Log", "📜", self.audit_log, SECONDARY), ("Export Data", "📤", self.export, SECONDARY)]
        await show(i, base(brand, "ADMINISTRATOR PANEL", text), MenuView(self.bot, i.user.id, items))

    async def publish_panel(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        await self.bot.get_cog("Home").publish_flow(i)  # type: ignore[union-attr]

    # ------------------------------------------------------------------ channels
    async def channels(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        s = await self.bot.settings(i.guild_id)
        brand = await self.bot.brand(i.guild_id)
        lines = [f"**{label}**: " + (f"<#{s[k]}>" if int(s.get(k) or 0) else "—") for k, label in CHANNEL_KEYS.items()]
        view = BaseView(self.bot, i.user.id)

        async def chosen(x: discord.Interaction, vals: list[str]) -> None:
            await self.channel_pick(x, vals[0])
        view.add_item(ChoiceSelect([(label, k, None) for k, label in CHANNEL_KEYS.items()], chosen, placeholder="Which channel setting?", row=0))
        add_nav(view, self.hub)
        await show(i, base(brand, "CHANNELS", "\n".join(lines)), view)

    async def channel_pick(self, i: discord.Interaction, key: str) -> None:
        if not await self._need_admin(i):
            return
        brand = await self.bot.brand(i.guild_id)

        async def picked(x: discord.Interaction, vals: list) -> None:
            if not await self._need_admin(x):
                return
            ch = x.guild.get_channel(vals[0].id) if x.guild else None
            if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                raise ServiceError("Pick a text channel.")
            need = ch.permissions_for(x.guild.me)
            missing = [n for n, ok in (("View Channel", need.view_channel), ("Send Messages", need.send_messages), ("Embed Links", need.embed_links)) if not ok]
            if missing:
                raise ServiceError(f"I'm missing **{', '.join(missing)}** in {ch.mention}. Fix the channel permissions and pick it again.")
            await self.bot.db.run(lambda c: (set_setting(c, x.guild_id, key, str(ch.id)), audit(c, x.guild_id, x.user.id, "admin.channel", key, str(ch.id))))
            await self.channels(x)

        async def clear(x: discord.Interaction) -> None:
            if not await self._need_admin(x):
                return
            await self.bot.db.run(lambda c: (clear_setting(c, x.guild_id, key), audit(c, x.guild_id, x.user.id, "admin.channel_clear", key)))
            await self.channels(x)
        view = BaseView(self.bot, i.user.id)
        view.add_item(ChannelPicker(picked, placeholder=f"Pick: {CHANNEL_KEYS[key]}", row=0))
        view.add_item(ActionButton("Clear this setting", clear, emoji="🧹", style=DANGER, row=1))
        add_nav(view, self.channels)
        await show(i, base(brand, "CHANNEL", f"Choose the channel for **{CHANNEL_KEYS[key]}**.\nI check that I can post there before saving."), view)

    # ------------------------------------------------------------------ roles
    async def roles(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        s = await self.bot.settings(i.guild_id)
        brand = await self.bot.brand(i.guild_id)
        lines = [f"**{label}**: " + (f"<@&{s[k]}>" if int(s.get(k) or 0) else "—") for k, label in ROLE_KEYS.items()]
        view = BaseView(self.bot, i.user.id)

        async def chosen(x: discord.Interaction, vals: list[str]) -> None:
            await self.role_pick(x, vals[0])
        view.add_item(ChoiceSelect([(label, k, None) for k, label in ROLE_KEYS.items()], chosen, placeholder="Which role setting?", row=0))
        add_nav(view, self.hub)
        await show(i, base(brand, "ROLES", "\n".join(lines) + "\n\nMembers with a configured Discord role get the matching permissions. Individual grants are under **Staff Roster**."), view)

    async def role_pick(self, i: discord.Interaction, key: str) -> None:
        if not await self._need_admin(i):
            return
        brand = await self.bot.brand(i.guild_id)

        async def picked(x: discord.Interaction, vals: list) -> None:
            if not await self._need_admin(x):
                return
            role = x.guild.get_role(vals[0].id)
            if role is None or role.is_default():
                raise ServiceError("Pick a real role - @everyone cannot be used.")
            if role.managed:
                raise ServiceError("That role is managed by an integration and cannot be assigned to people.")
            await self.bot.db.run(lambda c: (set_setting(c, x.guild_id, key, str(role.id)), audit(c, x.guild_id, x.user.id, "admin.role", key, str(role.id))))
            await self.roles(x)

        async def clear(x: discord.Interaction) -> None:
            if not await self._need_admin(x):
                return
            await self.bot.db.run(lambda c: (clear_setting(c, x.guild_id, key), audit(c, x.guild_id, x.user.id, "admin.role_clear", key)))
            await self.roles(x)
        view = BaseView(self.bot, i.user.id)
        view.add_item(RolePicker(picked, placeholder=f"Pick: {ROLE_KEYS[key]}", row=0))
        view.add_item(ActionButton("Clear this setting", clear, emoji="🧹", style=DANGER, row=1))
        add_nav(view, self.roles)
        await show(i, base(brand, "ROLE", f"Choose the Discord role for **{ROLE_KEYS[key]}**."), view)

    # ------------------------------------------------------------------ simple modals
    async def games(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        cur = await self.bot.db.read(lambda c: get_games(c, i.guild_id))

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not await self._need_admin(x):
                return
            games = [clean(g, field="Game", min_len=2, max_len=30) for g in re.split(r"[,\n]", f["games"]) if g.strip()]
            if not 1 <= len(games) <= 20:
                raise ValidationError("Provide between 1 and 20 games.")
            if len({g.casefold() for g in games}) != len(games):
                raise ValidationError("Each game must be listed once.")
            await self.bot.db.run(lambda c: (set_setting(c, x.guild_id, "games", json.dumps(games)), audit(c, x.guild_id, x.user.id, "admin.games", None, ",".join(games))))
            await self.hub(x)
        await i.response.send_modal(FormModal("Supported games", [Field("games", "Games (comma separated, primary first)", paragraph=True, max_length=600, default=", ".join(cur))], submit))

    async def limits(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        s = await self.bot.settings(i.guild_id)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not await self._need_admin(x):
                return
            vals = {"max_roster": parse_int(f["roster"], field="Max roster", lo=1, hi=15), "max_subs": parse_int(f["subs"], field="Max subs", lo=0, hi=5),
                    "checkin_window": parse_int(f["checkin"], field="Check-in window", lo=5, hi=240), "reminder_minutes": parse_int(f["remind"], field="Reminder", lo=5, hi=240)}

            def go(c):
                for k, val in vals.items():
                    set_setting(c, x.guild_id, k, str(val))
                audit(c, x.guild_id, x.user.id, "admin.limits", None, json.dumps(vals))
            await self.bot.db.run(go)
            await self.hub(x)
        await i.response.send_modal(FormModal("Limits & timing", [
            Field("roster", "Max main roster size per team", max_length=2, default=s["max_roster"]), Field("subs", "Max substitutes per team", max_length=1, default=s["max_subs"]),
            Field("checkin", "Check-in opens (minutes before start)", max_length=3, default=s["checkin_window"]),
            Field("remind", "Match reminder (minutes before start)", max_length=3, default=s["reminder_minutes"])], submit))

    async def registration(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        brand = await self.bot.brand(i.guild_id)
        flags = {k: await self.bot.db.read(lambda c, k=k: get_bool(c, i.guild_id, k)) for k in BOOL_KEYS}
        view = BaseView(self.bot, i.user.id)

        def toggler(key: str):
            async def go(x: discord.Interaction) -> None:
                if not await self._need_admin(x):
                    return
                new = "0" if flags[key] else "1"
                await self.bot.db.run(lambda c: (set_setting(c, x.guild_id, key, new), audit(c, x.guild_id, x.user.id, "admin.toggle", key, new)))
                await self.registration(x)
            return go
        for idx, (k, label) in enumerate(BOOL_KEYS.items()):
            view.add_item(ActionButton(f"{'✅' if flags[k] else '⬜'} {label}"[:80], toggler(k), style=SUCCESS if flags[k] else SECONDARY, row=idx))
        add_nav(view, self.hub)
        await show(i, base(brand, "REGISTRATION SETTINGS", "Press a setting to switch it on/off."), view)

    async def branding(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        s = await self.bot.settings(i.guild_id)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not await self._need_admin(x):
                return
            vals = {"brand_name": clean(f["name"], field="Name", min_len=2, max_len=40), "brand_color": f"{parse_hex_color(f['color']):06X}",
                    "brand_footer": clean(f["footer"], field="Footer", min_len=2, max_len=100), "brand_image": validate_url(f["image"], field="Banner URL") or ""}

            def go(c):
                for k, val in vals.items():
                    set_setting(c, x.guild_id, k, val)
                audit(c, x.guild_id, x.user.id, "admin.branding")
            await self.bot.db.run(go)
            await self.hub(x)
        await i.response.send_modal(FormModal("Server branding", [
            Field("name", "Brand name", max_length=40, default=s["brand_name"]), Field("color", "Accent color (hex)", "00B4FF", max_length=7, default=s["brand_color"]),
            Field("footer", "Footer text", max_length=100, default=s["brand_footer"]), Field("image", "Banner image URL (optional)", required=False, max_length=500, default=s["brand_image"])], submit))

    async def season(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        s = await self.bot.settings(i.guild_id)

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not await self._need_admin(x):
                return
            season = clean(f["season"], field="Season", min_len=1, max_len=30)
            offset = format_offset(parse_offset(f["offset"]))

            def go(c):
                set_setting(c, x.guild_id, "season", season)
                set_setting(c, x.guild_id, "utc_offset", offset)
                audit(c, x.guild_id, x.user.id, "admin.season", None, f"{season} {offset}")
            await self.bot.db.run(go)
            await self.hub(x)
        await i.response.send_modal(FormModal("Season & timezone", [
            Field("season", "Current season name", "Season 1", max_length=30, default=s["season"]),
            Field("offset", "Server time zone (UTC offset)", "+05:30", max_length=6, default=s["utc_offset"])], submit))

    async def modules(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        off = await self.bot.db.read(lambda c: disabled_modules(c, i.guild_id))
        brand = await self.bot.brand(i.guild_id)
        view = BaseView(self.bot, i.user.id)

        def toggler(m: str):
            async def go(x: discord.Interaction) -> None:
                if not await self._need_admin(x):
                    return

                def work(c):
                    cur = disabled_modules(c, x.guild_id)
                    cur.symmetric_difference_update({m})
                    set_setting(c, x.guild_id, "modules_disabled", json.dumps(sorted(cur)))
                    audit(c, x.guild_id, x.user.id, "admin.module", m, "off" if m in cur else "on")
                await self.bot.db.run(work)
                await self.modules(x)
            return go
        for idx, m in enumerate(MODULES):
            view.add_item(ActionButton(f"{'⬜ OFF' if m in off else '✅ ON'} • {m}", toggler(m), style=SECONDARY if m in off else SUCCESS, row=idx // 3))
        add_nav(view, self.hub)
        await show(i, base(brand, "MODULES", "Switch whole feature areas on or off. Hidden modules refuse to open even from old buttons."), view)

    # ------------------------------------------------------------------ scoring rules
    async def scoring(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        rules = await self.bot.db.read(lambda c: list_rules(c, i.guild_id))
        brand = await self.bot.brand(i.guild_id)
        rows = [(r["name"], str(r["id"]), f"{'★ default • ' if r['is_default'] else ''}{r['kind']}", f"{'★ ' if r['is_default'] else ''}**{r['name']}** • {r['kind']}") for r in rules]

        async def picked(x: discord.Interaction, value: str) -> None:
            await self.rule_detail(x, int(value))
        extra = [ActionButton("New rule", self.rule_new, emoji="➕", style=SUCCESS, row=2)]
        await PickList(self.bot, i.user.id, brand, "SCORING RULES", rows, picked, placeholder="Open a rule…", back=self.hub, extra=extra).start(i)

    async def rule_new(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not await self._need_admin(x):
                return
            name = clean(f["name"], field="Name", min_len=2, max_len=40)
            kind = parse_choice(f["kind"], field="Type", choices={"br": "br", "battle royale": "br", "versus": "versus", "vs": "versus"})
            rule = Rule(name=name, kind=kind, tiebreak=["points", "kills", "best_placement"] if kind == "br" else ["points", "round_diff", "wins"])
            rid = await self.bot.db.run(lambda c: save_rule(c, x.guild_id, x.user.id, rule))
            await self.rule_detail(x, rid)
        await i.response.send_modal(FormModal("New scoring rule", [Field("name", "Rule name", max_length=40), Field("kind", "Type: br (battle royale) or versus", "br", max_length=14, default="br")], submit))

    async def rule_detail(self, i: discord.Interaction, rid: int) -> None:
        if not await self._need_admin(i):
            return
        row = await self.bot.db.read(lambda c: c.execute("SELECT * FROM score_rules WHERE id=? AND guild_id=?", (rid, i.guild_id)).fetchone())
        if not row:
            raise ServiceError("Rule not found.")
        rule = Rule.from_row(row)
        brand = await self.bot.brand(i.guild_id)
        view = BaseView(self.bot, i.user.id)
        view.add_item(ActionButton("Edit values", lambda x: self.rule_edit(x, rid), emoji="✏️", style=PRIMARY))
        view.add_item(ActionButton("Make default", lambda x: self._default(x, rid), emoji="⭐", disabled=bool(row["is_default"])))
        add_nav(view, self.scoring)
        await show(i, base(brand, f"RULE • {rule.name}", f"Type: **{'Battle royale' if rule.kind == 'br' else 'Versus'}**{' • ★ default' if row['is_default'] else ''}\n\n{rule.describe()}\n\n"
                                                              "Saving recalculates standings for every event that uses this rule."), view)

    async def _default(self, i: discord.Interaction, rid: int) -> None:
        if not await self._need_admin(i):
            return
        await self.bot.db.run(lambda c: set_default_rule(c, i.guild_id, i.user.id, rid))
        await self.rule_detail(i, rid)

    async def rule_edit(self, i: discord.Interaction, rid: int) -> None:
        if not await self._need_admin(i):
            return
        row = await self.bot.db.read(lambda c: c.execute("SELECT * FROM score_rules WHERE id=? AND guild_id=?", (rid, i.guild_id)).fetchone())
        if not row:
            raise ServiceError("Rule not found.")
        rule = Rule.from_row(row)
        keys = TIEBREAK_KEYS

        def parse_tiebreak(text: str) -> list[str]:
            tb = [t.strip().lower() for t in re.split(r"[,>\s]+", text) if t.strip()]
            bad = [t for t in tb if t not in keys]
            if bad or not tb:
                raise ValidationError(f"Tie-break must use: {', '.join(keys)}.")
            return tb

        async def submit(x: discord.Interaction, f: dict[str, str]) -> None:
            if not await self._need_admin(x):
                return
            new = Rule(id=rid, name=clean(f["name"], field="Name", min_len=2, max_len=40), kind=rule.kind, tiebreak=parse_tiebreak(f["tb"]))
            if rule.kind == "br":
                new.placement_points = parse_number_list(f["a"], field="Placement points", max_items=100, lo=-1000, hi=1000)
                new.kill_points = parse_float(f["b"], field="Kill points", lo=-100, hi=100)
            else:
                w, d, l = (parse_number_list(f["a"], field="Win, draw, loss points", min_items=3, max_items=3, lo=-1000, hi=1000) + [0, 0, 0])[:3]
                new.win_points, new.draw_points, new.loss_points = w, d, l
                new.round_points = parse_float(f["b"], field="Points per round won", lo=-100, hi=100)
            await self.bot.db.run(lambda c: save_rule(c, x.guild_id, x.user.id, new, rid))
            await self.rule_detail(x, rid)
        if rule.kind == "br":
            fields = [Field("name", "Rule name", max_length=40, default=rule.name),
                      Field("a", "Placement points, 1st place first", "10,6,5,4,3,2,1,1", max_length=300, default=",".join(f"{p:g}" for p in rule.placement_points)),
                      Field("b", "Points per kill", max_length=6, default=f"{rule.kill_points:g}"),
                      Field("tb", "Tie-break order", "points,kills,best_placement", max_length=80, default=",".join(rule.tiebreak))]
        else:
            fields = [Field("name", "Rule name", max_length=40, default=rule.name),
                      Field("a", "Win, draw, loss points", "3,1,0", max_length=30, default=f"{rule.win_points:g},{rule.draw_points:g},{rule.loss_points:g}"),
                      Field("b", "Points per round won", max_length=6, default=f"{rule.round_points:g}"),
                      Field("tb", "Tie-break order", "points,round_diff,wins", max_length=80, default=",".join(rule.tiebreak))]
        await i.response.send_modal(FormModal("Edit scoring rule", fields, submit))

    # ------------------------------------------------------------------ database / audit / export
    async def database(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        st = await self.bot.db.status()
        brand = await self.bot.brand(i.guild_id)
        e = base(brand, "DATABASE", None)
        e.add_field(name="Integrity", value="✅ ok" if st["integrity"] == "ok" else f"❌ {st['integrity']}")
        e.add_field(name="Schema", value=f"v{st['schema_version']} (latest v{st['latest_version']})")
        e.add_field(name="Size", value=bytes_h(st["size_bytes"]))
        e.add_field(name="Rows (all servers)", value="\n".join(f"{k}: {n}" for k, n in st["counts"].items()), inline=False)
        view = BaseView(self.bot, i.user.id)
        view.add_item(ActionButton("Refresh", self.database, emoji="🔄"))
        view.add_item(ActionButton("Backups", self.backups, emoji="💾"))
        add_nav(view, self.hub)
        await show(i, e, view)

    async def audit_log(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        rows = await self.bot.db.read(lambda c: c.execute("SELECT * FROM audit_logs WHERE guild_id=? ORDER BY id DESC LIMIT 200", (i.guild_id,)).fetchall())
        brand = await self.bot.brand(i.guild_id)
        lines = [f"{ts(r['created_at'], 'R')} <@{r['actor_id']}> `{r['action']}`" + (f" → {trunc(r['target'], 28)}" if r["target"] else "") for r in rows]
        await Pager(self.bot, i.user.id, list_pages(brand, "AUDIT LOG", lines, per_page=12, empty="Nothing recorded yet."), back=self.hub, refresh=self.audit_log).start(i)

    async def export(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        brand = await self.bot.brand(i.guild_id)

        async def chosen(x: discord.Interaction, vals: list[str]) -> None:
            if not await self._need_admin(x):
                return
            label, sql = EXPORTS[vals[0]]
            await x.response.defer()
            rows = await self.bot.db.read(lambda c: c.execute(sql, (x.guild_id,)).fetchall())
            buf = io.StringIO()
            if rows:
                w = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
                w.writeheader()
                for r in rows:
                    w.writerow({k: ("'" + str(val) if isinstance(val, str) and val[:1] in "=+-@" else val) for k, val in r.items()})  # neutralise spreadsheet formulas
            else:
                buf.write("no rows\n")
            await self.bot.db.run(lambda c: audit(c, x.guild_id, x.user.id, "admin.export", vals[0]))
            await x.followup.send(embed=notice(f"{label}: {len(rows)} row(s).", "ok"), file=discord.File(io.BytesIO(buf.getvalue().encode("utf-8-sig")), filename=f"aegis-{vals[0]}.csv"), ephemeral=True)
        view = BaseView(self.bot, i.user.id)
        view.add_item(ChoiceSelect([(label, k, None) for k, (label, _) in EXPORTS.items()], chosen, placeholder="What do you want to export?", row=0))
        add_nav(view, self.hub)
        await show(i, base(brand, "EXPORT DATA", "CSV files for this server. Room passwords and game UIDs are **never** exported."), view)

    # ------------------------------------------------------------------ backups (restore = bot owner only)
    def _backup_files(self) -> list[Path]:
        d = self.bot.config.backup_dir
        return sorted(d.glob("aegis-*.sqlite3"), reverse=True) if d.exists() else []

    async def backups(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        perms = await self.bot.perms(i)
        files = self._backup_files()
        brand = await self.bot.brand(i.guild_id)
        lines = [f"`{f.name}` • {bytes_h(f.stat().st_size)}" for f in files[:15]]
        view = BaseView(self.bot, i.user.id)
        view.add_item(ActionButton("Create backup now", self.backup_now, emoji="💾", style=SUCCESS, row=0))
        if perms.is_owner and files:
            async def restore_pick(x: discord.Interaction, vals: list[str]) -> None:
                await self.restore_confirm(x, vals[0])
            view.add_item(ChoiceSelect([(f.name, f.name, bytes_h(f.stat().st_size)) for f in files[:25]], restore_pick, placeholder="Restore from… (bot owner only)", row=1))
        add_nav(view, self.hub)
        note = "" if perms.is_owner else "\n\nRestoring is limited to the bot owner."
        await show(i, base(brand, "BACKUPS", ("\n".join(lines) or "No backups yet.") + f"\n\nStored on the bot host in `{self.bot.config.backup_dir.name}/` (last 14 kept)." + note), view)

    async def backup_now(self, i: discord.Interaction) -> None:
        if not await self._need_admin(i):
            return
        await i.response.defer()
        dest = self.bot.config.backup_dir / f"aegis-{now_iso().replace(':', '').replace('-', '')}.sqlite3"
        await self.bot.db.backup(dest)  # validated before it is kept
        for old in self._backup_files()[14:]:
            old.unlink(missing_ok=True)
        await self.bot.db.run(lambda c: audit(c, i.guild_id, i.user.id, "admin.backup", dest.name))
        await self.backups(i)

    async def restore_confirm(self, i: discord.Interaction, name: str) -> None:
        perms = await self.bot.perms(i)
        if not perms.is_owner:
            raise ServiceError("Only the bot owner can restore the database.")
        path = self.bot.config.backup_dir / Path(name).name  # strip any path components
        if not path.exists():
            raise ServiceError("That backup no longer exists.")
        try:
            validate_backup(path)
        except BackupError as exc:
            raise ServiceError(f"That backup is not usable: {exc}") from exc

        async def yes(x: discord.Interaction) -> None:
            if not (await self.bot.perms(x)).is_owner:
                raise ServiceError("Only the bot owner can restore the database.")
            await x.response.defer()
            self.bot.maintenance = True
            try:
                safety = await self.bot.db.restore(path, self.bot.config.backup_dir / "safety")
            finally:
                self.bot.maintenance = False
            await self.backups(x)
            await x.followup.send(embed=notice(f"Restored from `{path.name}`. A safety copy of the previous data was saved as `{safety.name if safety else 'n/a'}`.", "ok"), ephemeral=True)
        await confirm(i, self.bot, f"**Restore `{path.name}`?** Everything changed since that backup is replaced. A safety copy of the current data is saved first.", yes,
                      yes_label="Restore", on_no=self.backups)

# --------------------------------------------------------------------------------------------------
# cogs/owner.py
# Bot-owner panel (OWNER_ID only): health, diagnostics, error log, maintenance mode, command sync, servers.
# --------------------------------------------------------------------------------------------------

OWNER_ONLY = "Only the bot owner can use this panel."


def uptime(seconds: float) -> str:
    s = int(seconds)
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, _ = divmod(s, 60)
    return f"{d}d {h}h {m}m" if d else f"{h}h {m}m"


class Owner(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        for name, fn in {"owner": self.hub, "owner.diagnostics": self.diagnostics, "owner.errors": self.errors}.items():
            self.bot.register_entry(name, fn)

    async def _need_owner(self, i: discord.Interaction) -> bool:
        return await self.bot.require(i, lambda p: p.is_owner, OWNER_ONLY) is not None

    async def hub(self, i: discord.Interaction) -> None:
        if not await self._need_owner(i):
            return
        brand = await self.bot.brand(i.guild_id)
        mode = "🔧 ON" if self.bot.maintenance else "off"
        e = base(brand, "OWNER PANEL", f"Latency **{round(self.bot.latency * 1000)} ms** • Uptime **{uptime(time.time() - self.bot.started_at)}** • "
                                               f"Servers **{len(self.bot.guilds)}** • Maintenance **{mode}**")
        if self.bot.failed_cogs:
            e.add_field(name="⚠️ Modules that failed to load", value="\n".join(f"`{k}`: {v[:120]}" for k, v in self.bot.failed_cogs.items())[:1000], inline=False)
        items = [("Diagnostics", "🩺", self.diagnostics, PRIMARY), ("Error Log", "🐞", self.errors, SECONDARY), ("Servers", "🌐", self.servers, SECONDARY),
                 (f"Maintenance: {'ON' if self.bot.maintenance else 'OFF'}", "🔧", self.toggle_maintenance, DANGER if self.bot.maintenance else SECONDARY),
                 ("Sync Commands", "🔄", self.sync, SECONDARY), ("Backups & Restore", "💾", lambda x: self.bot.open(x, "admin.backups"), SECONDARY),
                 ("Admin Panel", "⚙️", lambda x: self.bot.open(x, "admin"), SECONDARY)]
        await show(i, e, MenuView(self.bot, i.user.id, items))

    async def diagnostics(self, i: discord.Interaction) -> None:
        if not await self._need_owner(i):
            return
        brand = await self.bot.brand(i.guild_id)
        st = await self.bot.db.status()
        e = base(brand, "DIAGNOSTICS", None)
        e.add_field(name="Runtime", value=f"Python {platform.python_version()}\ndiscord.py {discord.__version__}\n{platform.system()} {platform.machine()}")
        e.add_field(name="Process", value=f"Uptime {uptime(time.time() - self.bot.started_at)}\nLatency {round(self.bot.latency * 1000)} ms\nErrors since start: {self.bot.errors.total}")
        e.add_field(name="Database", value=f"Integrity: {'✅ ok' if st['integrity'] == 'ok' else '❌ ' + str(st['integrity'])}\nSchema v{st['schema_version']}/{st['latest_version']}\n{st['size_bytes'] / 1024:.0f} KB")
        e.add_field(name="Loaded modules", value="\n".join(f"✅ `{c}`" for c in self.bot.loaded_cogs) or "none", inline=False)
        if self.bot.failed_cogs:
            e.add_field(name="Failed modules", value="\n".join(f"❌ `{k}`: {v[:100]}" for k, v in self.bot.failed_cogs.items())[:1000], inline=False)
        missing = self.bot.validate_entries()
        e.add_field(name="Panel buttons", value="✅ every button has a handler" if not missing else "❌ unregistered: " + ", ".join(sorted(missing)), inline=False)
        e.add_field(name="Commands", value=f"{len(self.bot.tree.get_commands())} slash command(s) in this build • {len(self.bot.entries)} screens registered", inline=False)
        view = BaseView(self.bot, i.user.id)
        view.add_item(ActionButton("Refresh", self.diagnostics, emoji="🔄"))
        add_nav(view, self.hub)
        await show(i, e, view)

    async def errors(self, i: discord.Interaction) -> None:
        if not await self._need_owner(i):
            return
        brand = await self.bot.brand(i.guild_id)
        recent = self.bot.errors.recent(40)
        lines = [f"`{r.ref}` <t:{int(r.at)}:R> `{r.where}`\n> {r.summary[:110]}" for r in recent]

        async def clear(x: discord.Interaction) -> None:
            if not await self._need_owner(x):
                return
            self.bot.errors.clear()
            await self.errors(x)
        extra = [ActionButton("Clear", clear, emoji="🧹", style=DANGER, row=0)]
        await Pager(self.bot, i.user.id, list_pages(brand, "ERROR LOG", lines, per_page=6, empty="No errors since the bot started. ✅"),
                    extra=extra, back=self.hub, refresh=self.errors).start(i)

    async def servers(self, i: discord.Interaction) -> None:
        if not await self._need_owner(i):
            return
        brand = await self.bot.brand(i.guild_id)
        lines = [f"**{g.name}** • {g.member_count or '?'} members • `{g.id}`" for g in sorted(self.bot.guilds, key=lambda g: -(g.member_count or 0))]
        await Pager(self.bot, i.user.id, list_pages(brand, "SERVERS", lines, per_page=10, empty="Not in any server."), back=self.hub).start(i)

    async def toggle_maintenance(self, i: discord.Interaction) -> None:
        if not await self._need_owner(i):
            return
        self.bot.maintenance = not self.bot.maintenance
        await self.bot.db.run(lambda c: audit(c, i.guild_id, i.user.id, "owner.maintenance", None, "on" if self.bot.maintenance else "off"))
        await self.hub(i)

    async def sync(self, i: discord.Interaction) -> None:
        if not await self._need_owner(i):
            return
        await i.response.defer()
        n = await self.bot.sync_commands()
        await say(i, f"Synced **{n}** slash command(s) globally. Discord can take a few minutes to show them.", "ok",
                  view=self._back_view(i))

    def _back_view(self, i: discord.Interaction) -> BaseView:
        view = BaseView(self.bot, i.user.id)
        add_nav(view, self.hub)
        return view


# ==================================================================================================
# SECTION 13 - BACKGROUND TASKS AND SCHEDULED JOBS
# ==================================================================================================

# --------------------------------------------------------------------------------------------------
# cogs/tasks.py
# Background loops: scheduled publishing, match reminders, giveaway endings, daily backup.
#
# Every loop catches its own errors so one bad row never stops the loop, and every action is idempotent
# (status/flag updated in the database before or together with the side effect).
# --------------------------------------------------------------------------------------------------

log_tasks = logging.getLogger("aegis.tasks")
MAX_REMINDER_MINUTES = 240
KEEP_BACKUPS = 14


class Tasks(commands.Cog):
    def __init__(self, bot: AegisBot) -> None:
        self.bot = bot

    async def cog_load(self) -> None:
        self.publish_loop.start()
        self.reminder_loop.start()
        self.giveaway_loop.start()
        self.backup_loop.start()

    async def cog_unload(self) -> None:
        for loop in (self.publish_loop, self.reminder_loop, self.giveaway_loop, self.backup_loop):
            loop.cancel()

    # ------------------------------------------------------------------ scheduled publish
    @tasks.loop(seconds=60)
    async def publish_loop(self) -> None:
        try:
            now = to_iso(utcnow())
            due = await self.bot.db.read(lambda c: due_to_publish(c, now))
            for t in due:
                await self.bot.db.run(lambda c, tid=t["id"]: mark_published(c, tid))
                guild = self.bot.get_guild(t["guild_id"])
                if guild is not None:
                    await publish_event_message(self.bot, guild, "tournament", t["id"])
                log_tasks.info("auto-published tournament %s", t["id"])
        except Exception:  # noqa: BLE001
            log_tasks.exception("publish loop failed")

    # ------------------------------------------------------------------ reminders
    @tasks.loop(seconds=60)
    async def reminder_loop(self) -> None:
        try:
            now_dt = utcnow()
            now, horizon = to_iso(now_dt), to_iso(now_dt + timedelta(minutes=MAX_REMINDER_MINUTES))
            for kind, table, fn, reg_table, players_table, fk in (
                    ("tournament", "tournaments", tournament_due_reminders, "tournament_registrations", "tournament_reg_players", "tournament_id"),
                    ("scrim", "scrims", scrim_due_reminders, "scrim_registrations", "scrim_reg_players", "scrim_id")):
                events = await self.bot.db.read(lambda c, fn=fn: fn(c, now, horizon))
                for ev in events:
                    minutes = await self.bot.db.read(lambda c, g=ev["guild_id"]: get_int(c, g, "reminder_minutes", 15))
                    if ev["start_time"] > to_iso(now_dt + timedelta(minutes=minutes)):
                        continue  # not within this server's reminder window yet
                    # claim first so a slow DM loop can never send twice
                    claimed = await self.bot.db.run(lambda c, e=ev: c.execute(f"UPDATE {table} SET reminded=1 WHERE id=? AND reminded=0", (e["id"],)).rowcount)  # noqa: S608
                    if not claimed:
                        continue
                    users = await self.bot.db.read(lambda c, e=ev: [r["user_id"] for r in c.execute(
                        f"SELECT DISTINCT p.user_id FROM {players_table} p JOIN {reg_table} r ON r.id=p.reg_id "  # noqa: S608
                        f"WHERE r.{fk}=? AND r.status='APPROVED'", (e["id"],)).fetchall()])
                    start_unix = int(from_iso(ev["start_time"]).timestamp())
                    text = f"⏰ **{ev['title']}** starts <t:{start_unix}:R>. Check-in and room details are in the bot menu (**Check-in**)."
                    for uid in users:
                        await self.bot.dm(uid, content=text)
                    log_tasks.info("reminded %s players for %s %s", len(users), kind, ev["id"])
        except Exception:  # noqa: BLE001
            log_tasks.exception("reminder loop failed")

    # ------------------------------------------------------------------ giveaways
    @tasks.loop(seconds=30)
    async def giveaway_loop(self) -> None:
        try:
            due = await self.bot.db.read(lambda c: due_giveaways(c, to_iso(utcnow())))
            for g in due:
                await finish_giveaway(self.bot, g["id"])
        except Exception:  # noqa: BLE001
            log_tasks.exception("giveaway loop failed")

    # ------------------------------------------------------------------ backups
    @tasks.loop(hours=24)
    async def backup_loop(self) -> None:
        try:
            d = self.bot.config.backup_dir
            dest = d / f"aegis-auto-{utcnow().strftime('%Y%m%dT%H%M%SZ')}.sqlite3"
            await self.bot.db.backup(dest)
            files = sorted(Path(d).glob("aegis-*.sqlite3"), reverse=True)
            for old in files[KEEP_BACKUPS:]:
                old.unlink(missing_ok=True)
            log_tasks.info("daily backup written: %s", dest.name)
        except Exception:  # noqa: BLE001
            log_tasks.exception("daily backup failed")

    @publish_loop.before_loop
    @reminder_loop.before_loop
    @giveaway_loop.before_loop
    async def _wait(self) -> None:
        await self.bot.wait_until_ready()

    @backup_loop.before_loop
    async def _wait_backup(self) -> None:
        await self.bot.wait_until_ready()
        # do not back up the instant the bot boots (restart loops would flood the folder): wait one hour
        import asyncio
        await asyncio.sleep(3600)


# --------------------------------------------------------------------------------------------------
# Cog registry. Replaces the former discord.py extension loading (load_extension("cogs.x")): every cog class
# is now defined in this file and is added by AegisBot.setup_hook in this order. Names keep the old module
# labels so the owner panel's "Loaded modules" list looks exactly as before.
# --------------------------------------------------------------------------------------------------
COGS: tuple[tuple[str, type[commands.Cog]], ...] = (
    ("cogs.home", Home), ("cogs.teams", Teams), ("cogs.players", Players), ("cogs.tournaments", Tournaments),
    ("cogs.scrims", Scrims), ("cogs.registrations", Registrations), ("cogs.matches", Matches),
    ("cogs.leaderboards", Leaderboards), ("cogs.staff", Staff), ("cogs.reports", Reports),
    ("cogs.community", Community), ("cogs.admin", Admin), ("cogs.owner", Owner), ("cogs.tasks", Tasks),
)


# ==================================================================================================
# SECTION 14 - LOGGING, ERROR HANDLING AND DIAGNOSTICS
# ==================================================================================================

# --------------------------------------------------------------------------------------------------
# utils/logging_setup.py
# Structured logging with token redaction.
# --------------------------------------------------------------------------------------------------

_TOKEN_RE = re.compile(r"[A-Za-z0-9_\-]{23,28}\.[A-Za-z0-9_\-]{6,7}\.[A-Za-z0-9_\-]{27,}")


class RedactFilter(logging.Filter):
    def __init__(self, secrets: list[str] | None = None):
        super().__init__()
        self._secrets = [s for s in (secrets or []) if s and len(s) > 8]

    def _scrub(self, text: str) -> str:
        for s in self._secrets:
            text = text.replace(s, "[REDACTED]")
        return _TOKEN_RE.sub("[REDACTED]", text)

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:  # noqa: BLE001
            return True
        record.msg = self._scrub(msg)
        record.args = ()
        if record.exc_info:
            record.exc_text = self._scrub(logging.Formatter().formatException(record.exc_info))
            record.exc_info = None
        return True


def setup_logging(level: str = "INFO", log_dir: Path | None = None, secrets: list[str] | None = None) -> None:
    fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s", "%Y-%m-%dT%H:%M:%S")
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    flt = RedactFilter(secrets)
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(RotatingFileHandler(log_dir / "aegis.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8"))
    for h in handlers:
        h.setFormatter(fmt)
        h.addFilter(flt)
        root.addHandler(h)
    logging.getLogger("discord").setLevel(logging.WARNING if level != "DEBUG" else logging.DEBUG)
    logging.getLogger("discord.http").setLevel(logging.WARNING)


# ==================================================================================================
# SECTION 15 - STARTUP AND BOT EXECUTION
# ==================================================================================================

# --------------------------------------------------------------------------------------------------
# main.py
# AEGIS ESPORTS entry point.
# --------------------------------------------------------------------------------------------------

async def run() -> int:
    try:
        config = Config.from_env()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2
    setup_logging(config.log_level, config.log_dir, secrets=[config.token])
    log = logging.getLogger("aegis")
    if not config.owner_id:
        log.warning("OWNER_ID is not set - the owner panel will be unavailable.")

    import discord

    bot = AegisBot(config, Database(config.database_path))
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _request_stop() -> None:
        log.info("shutdown requested")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):  # e.g. Windows
            pass

    runner = asyncio.create_task(bot.start(config.token))
    waiter = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait({runner, waiter}, return_when=asyncio.FIRST_COMPLETED)
        if runner in done:
            exc = runner.exception()
            if isinstance(exc, discord.LoginFailure):
                log.error("Discord rejected the bot token. Check DISCORD_BOT_TOKEN in .env.")
                return 3
            if isinstance(exc, discord.PrivilegedIntentsRequired):
                log.error("A privileged intent is required but not enabled in the Developer Portal.")
                return 4
            if exc:
                raise exc
    finally:
        await bot.close()
        for t in (runner, waiter):
            t.cancel()
        await asyncio.gather(runner, waiter, return_exceptions=True)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(run()))
    except KeyboardInterrupt:
        sys.exit(0)
