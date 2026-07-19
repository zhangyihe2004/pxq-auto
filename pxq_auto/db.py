from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from .config import profile_key


SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY,
    show_id TEXT NOT NULL,
    show_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    session_name TEXT NOT NULL,
    interval_sec INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    session_status TEXT NOT NULL DEFAULT '',
    sale_time_ms INTEGER,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(show_id, session_id)
);

CREATE TABLE IF NOT EXISTS task_plans (
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    seat_plan_id TEXT NOT NULL,
    plan_name TEXT NOT NULL,
    price REAL NOT NULL,
    priority INTEGER NOT NULL,
    can_buy_count INTEGER NOT NULL DEFAULT 0,
    sale_started INTEGER NOT NULL DEFAULT 0,
    updated_at REAL NOT NULL,
    PRIMARY KEY(task_id, seat_plan_id),
    UNIQUE(task_id, priority)
);

CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY,
    phone TEXT NOT NULL UNIQUE,
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    profile_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'RESERVED',
    order_id TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS audiences (
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    name TEXT NOT NULL,
    masked_id TEXT NOT NULL,
    PRIMARY KEY(account_id, position),
    UNIQUE(account_id, name, masked_id)
);

CREATE TABLE IF NOT EXISTS account_plans (
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    seat_plan_id TEXT NOT NULL,
    priority INTEGER NOT NULL,
    PRIMARY KEY(account_id, seat_plan_id),
    UNIQUE(account_id, priority)
);
"""


class Database:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=15, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA journal_mode = WAL")
        self.connection.execute("PRAGMA busy_timeout = 15000")
        self.connection.executescript(SCHEMA)
        self.connection.execute(
            """
            UPDATE accounts
            SET status = CASE
                WHEN NOT EXISTS (
                    SELECT 1 FROM audiences WHERE account_id = accounts.id
                ) THEN 'NEEDS_AUDIENCE'
                WHEN NOT EXISTS (
                    SELECT 1 FROM account_plans WHERE account_id = accounts.id
                ) THEN 'NEEDS_PLANS'
                ELSE 'READY'
            END
            WHERE status IN ('READY', 'NEEDS_AUDIENCE', 'NEEDS_PLANS')
            """
        )

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *_args) -> None:
        self.close()

    @staticmethod
    def _next_id(connection: sqlite3.Connection, table: str) -> int:
        rows = connection.execute(f"SELECT id FROM {table} ORDER BY id").fetchall()
        expected = 1
        for row in rows:
            if row["id"] != expected:
                break
            expected += 1
        return expected

    # ---- 抢票任务 ----

    def create_task(
        self,
        *,
        show_id: str,
        show_name: str,
        session_id: str,
        session_name: str,
        interval_sec: int,
        sale_time_ms: int | None,
        plans: list[dict],
    ) -> tuple[int, bool]:
        now = time.time()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            existing = self.connection.execute(
                "SELECT id FROM tasks WHERE show_id = ? AND session_id = ?",
                (show_id, session_id),
            ).fetchone()
            if existing:
                self.connection.rollback()
                return int(existing["id"]), False
            task_id = self._next_id(self.connection, "tasks")
            self.connection.execute(
                """
                INSERT INTO tasks (
                    id, show_id, show_name, session_id, session_name,
                    interval_sec, sale_time_ms, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    show_id,
                    show_name,
                    session_id,
                    session_name,
                    interval_sec,
                    sale_time_ms,
                    now,
                    now,
                ),
            )
            self.connection.executemany(
                """
                INSERT INTO task_plans (
                    task_id, seat_plan_id, plan_name, price, priority,
                    can_buy_count, sale_started, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        task_id,
                        plan["pid"],
                        plan["plan_name"],
                        plan["price"],
                        priority,
                        int(plan.get("can_buy_count") or 0),
                        int(bool(plan.get("sale_started"))),
                        now,
                    )
                    for priority, plan in enumerate(plans, 1)
                ],
            )
            self.connection.commit()
            return task_id, True
        except Exception:
            self.connection.rollback()
            raise

    def list_tasks(self) -> list[sqlite3.Row]:
        return self.connection.execute("SELECT * FROM tasks ORDER BY id").fetchall()

    def get_task(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()

    def get_task_plans(self, task_id: int) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM task_plans WHERE task_id = ? ORDER BY priority",
            (task_id,),
        ).fetchall()

    def update_task_snapshot(
        self,
        task_id: int,
        session_status: str,
        plans: list[tuple[str, int, bool]],
    ) -> bool:
        now = time.time()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            changed = self.connection.execute(
                """
                UPDATE tasks SET session_status = ?, updated_at = ?
                WHERE id = ? AND status = 'active'
                """,
                (session_status, now, task_id),
            ).rowcount
            if not changed:
                self.connection.rollback()
                return False
            self.connection.executemany(
                """
                UPDATE task_plans
                SET can_buy_count = ?, sale_started = ?, updated_at = ?
                WHERE task_id = ? AND seat_plan_id = ?
                """,
                [
                    (count, int(started), now, task_id, plan_id)
                    for plan_id, count, started in plans
                ],
            )
            self.connection.commit()
            return True
        except Exception:
            self.connection.rollback()
            raise

    def set_task_status(self, task_id: int, status: str) -> bool:
        return bool(
            self.connection.execute(
                "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?",
                (status, time.time(), task_id),
            ).rowcount
        )

    def set_task_interval(self, task_id: int, interval_sec: int) -> bool:
        return bool(
            self.connection.execute(
                "UPDATE tasks SET interval_sec = ?, updated_at = ? WHERE id = ?",
                (interval_sec, time.time(), task_id),
            ).rowcount
        )

    def delete_task(self, task_id: int) -> list[str]:
        profiles = [
            row["profile_key"]
            for row in self.connection.execute(
                "SELECT profile_key FROM accounts WHERE task_id = ?", (task_id,)
            )
        ]
        with self.connection:
            changed = self.connection.execute(
                "DELETE FROM tasks WHERE id = ?", (task_id,)
            ).rowcount
        return profiles if changed else []

    # ---- 账号与观演人 ----

    def reserve_account(self, task_id: int, phone: str) -> sqlite3.Row:
        now = time.time()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            if not self.get_task(task_id):
                raise ValueError(f"抢票任务 #{task_id} 不存在")
            existing = self.connection.execute(
                "SELECT * FROM accounts WHERE phone = ?", (phone,)
            ).fetchone()
            if existing:
                raise ValueError(
                    f"该手机号已绑定抢票任务 #{existing['task_id']}，一个账号只能绑定一个场次"
                )
            account_id = self._next_id(self.connection, "accounts")
            self.connection.execute(
                """
                INSERT INTO accounts (
                    id, phone, task_id, profile_key, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'RESERVED', ?, ?)
                """,
                (account_id, phone, task_id, profile_key(phone), now, now),
            )
            self.connection.commit()
            return self.get_account(account_id)  # type: ignore[return-value]
        except Exception:
            self.connection.rollback()
            raise

    def get_account(self, account_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM accounts WHERE id = ?", (account_id,)
        ).fetchone()

    def get_account_by_phone(self, phone: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM accounts WHERE phone = ?", (phone,)
        ).fetchone()

    def list_accounts(self, task_id: int | None = None) -> list[sqlite3.Row]:
        if task_id is None:
            return self.connection.execute(
                "SELECT * FROM accounts ORDER BY id"
            ).fetchall()
        return self.connection.execute(
            "SELECT * FROM accounts WHERE task_id = ? ORDER BY id", (task_id,)
        ).fetchall()

    def set_account_status(
        self,
        account_id: int,
        status: str,
        *,
        order_id: str | None = None,
        error: str = "",
    ) -> bool:
        return bool(
            self.connection.execute(
                """
                UPDATE accounts
                SET status = ?, order_id = ?, last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (status, order_id, error, time.time(), account_id),
            ).rowcount
        )

    def replace_audiences(self, account_id: int, people: list[tuple[str, str]]) -> None:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute(
                "DELETE FROM audiences WHERE account_id = ?", (account_id,)
            )
            self.connection.executemany(
                "INSERT INTO audiences (account_id, position, name, masked_id) VALUES (?, ?, ?, ?)",
                [
                    (account_id, position, name, masked_id)
                    for position, (name, masked_id) in enumerate(people, 1)
                ],
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def get_audiences(self, account_id: int) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM audiences WHERE account_id = ? ORDER BY position",
            (account_id,),
        ).fetchall()

    def replace_account_plans(self, account_id: int, plan_ids: list[str]) -> None:
        account = self.get_account(account_id)
        if not account:
            raise ValueError(f"账号 #{account_id} 不存在")
        known = {row["seat_plan_id"] for row in self.get_task_plans(account["task_id"])}
        if (
            not plan_ids
            or len(plan_ids) != len(set(plan_ids))
            or not set(plan_ids) <= known
        ):
            raise ValueError("票档配置为空、重复或包含其他场次的票档")
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            self.connection.execute(
                "DELETE FROM account_plans WHERE account_id = ?", (account_id,)
            )
            self.connection.executemany(
                "INSERT INTO account_plans (account_id, seat_plan_id, priority) VALUES (?, ?, ?)",
                [
                    (account_id, plan_id, priority)
                    for priority, plan_id in enumerate(plan_ids, 1)
                ],
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def get_account_plans(self, account_id: int) -> list[sqlite3.Row]:
        return self.connection.execute(
            """
            SELECT p.*, ap.priority AS account_priority
            FROM account_plans ap
            JOIN accounts a ON a.id = ap.account_id
            JOIN task_plans p
              ON p.task_id = a.task_id AND p.seat_plan_id = ap.seat_plan_id
            WHERE ap.account_id = ?
            ORDER BY ap.priority
            """,
            (account_id,),
        ).fetchall()

    def configuration_status(self, account_id: int) -> str:
        if not self.get_audiences(account_id):
            return "NEEDS_AUDIENCE"
        if not self.get_account_plans(account_id):
            return "NEEDS_PLANS"
        return "READY"

    def delete_account(self, account_id: int) -> str | None:
        account = self.get_account(account_id)
        if not account:
            return None
        with self.connection:
            self.connection.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        return str(account["profile_key"])
