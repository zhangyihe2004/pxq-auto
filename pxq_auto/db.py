from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from .config import profile_key


_UNCHANGED = object()


SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY,
    show_id TEXT NOT NULL,
    show_name TEXT NOT NULL,
    session_id TEXT NOT NULL,
    session_name TEXT NOT NULL,
    support_seat_picking INTEGER NOT NULL,
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
    limitation INTEGER NOT NULL,
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
    enabled INTEGER NOT NULL DEFAULT 0,
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

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> Database:
        return self

    def __exit__(self, *_args) -> None:
        self.close()

    @staticmethod
    def _next_id(connection: sqlite3.Connection, table: str) -> int:
        if table not in {"tasks", "accounts"}:
            raise ValueError(f"不支持为表 {table} 分配序号")
        row = connection.execute(
            f"""
            SELECT CASE
                WHEN EXISTS (SELECT 1 FROM {table} WHERE id = 1)
                THEN (
                    SELECT MIN(current.id + 1)
                    FROM {table} AS current
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {table} AS following
                        WHERE following.id = current.id + 1
                    )
                )
                ELSE 1
            END AS id
            """
        ).fetchone()
        return int(row["id"])

    # ---- 抢票任务 ----

    def create_task(
        self,
        *,
        show_id: str,
        show_name: str,
        session_id: str,
        session_name: str,
        support_seat_picking: bool,
        interval_sec: int,
        session_status: str,
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
                    support_seat_picking, interval_sec, session_status,
                    sale_time_ms, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    show_id,
                    show_name,
                    session_id,
                    session_name,
                    int(support_seat_picking),
                    interval_sec,
                    session_status,
                    sale_time_ms,
                    now,
                    now,
                ),
            )
            self.connection.executemany(
                """
                INSERT INTO task_plans (
                    task_id, seat_plan_id, plan_name, price, limitation, priority,
                    can_buy_count, sale_started, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        task_id,
                        plan["pid"],
                        plan["plan_name"],
                        plan["price"],
                        int(plan.get("limitation") or 0),
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
        sale_time_ms: int | None,
        plans: list[tuple[str, int, bool]],
    ) -> bool:
        now = time.time()
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            changed = self.connection.execute(
                """
                UPDATE tasks
                SET session_status = ?,
                    sale_time_ms = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'active'
                """,
                (session_status, sale_time_ms, now, task_id),
            ).rowcount
            if not changed:
                self.connection.rollback()
                return False
            self.connection.execute(
                """
                UPDATE task_plans
                SET can_buy_count = 0, sale_started = 0, updated_at = ?
                WHERE task_id = ?
                """,
                (now, task_id),
            )
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

    def set_task_status(
        self, task_id: int, status: str, *, current_status: str | None = None
    ) -> bool:
        condition = " AND status = ?" if current_status is not None else ""
        parameters = [status, time.time(), task_id]
        if current_status is not None:
            parameters.append(current_status)
        return bool(
            self.connection.execute(
                "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ?" + condition,
                parameters,
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
            account = self.get_account(account_id)
            if account is None:
                raise RuntimeError("账号创建后无法读取")
            self.connection.commit()
            return account
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
        order_id: str | None | object = _UNCHANGED,
        error: str = "",
    ) -> bool:
        keep_order_id = order_id is _UNCHANGED
        return bool(
            self.connection.execute(
                """
                UPDATE accounts
                SET status = ?,
                    enabled = CASE
                        WHEN ? IN (
                            'STOPPED', 'NEEDS_LOGIN', 'CREATED', 'UNKNOWN', 'COMPLETE'
                        )
                        THEN 0 ELSE enabled
                    END,
                    order_id = CASE WHEN ? THEN order_id ELSE ? END,
                    last_error = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    status,
                    status,
                    int(keep_order_id),
                    None if keep_order_id else order_id,
                    error,
                    time.time(),
                    account_id,
                ),
            ).rowcount
        )

    def activate_account(self, account_id: int) -> None:
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            account = self.get_account(account_id)
            if not account:
                raise ValueError(f"账号 #{account_id} 不存在")
            if account["status"] in {"RESERVED", "NEEDS_LOGIN"}:
                raise ValueError("账号尚未登录")
            if account["status"] in {"CREATED", "UNKNOWN"}:
                raise ValueError("账号存在订单保护状态")
            if account["status"] == "COMPLETE":
                raise ValueError("原观演人均已购买")
            if not self.get_account_plans(account_id) or not self.get_audiences(
                account_id
            ):
                raise ValueError("账号配置不完整")
            self.connection.execute(
                """
                UPDATE accounts
                SET enabled = 1, status = 'READY', last_error = '', updated_at = ?
                WHERE id = ?
                """,
                (time.time(), account_id),
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def claim_account(self, account_id: int) -> bool:
        """仅在账号仍启用且空闲时原子取得一次执行权。"""
        return bool(
            self.connection.execute(
                """
                UPDATE accounts SET status = 'RUNNING', updated_at = ?
                WHERE id = ? AND enabled = 1 AND status = 'READY'
                """,
                (time.time(), account_id),
            ).rowcount
        )

    def begin_account_configuration(self, account_id: int) -> bool:
        """停止空闲账号；已进入抢票流程时拒绝并发修改。"""
        return bool(
            self.connection.execute(
                """
                UPDATE accounts
                SET enabled = 0,
                    status = CASE WHEN status = 'READY' THEN 'STOPPED' ELSE status END,
                    updated_at = ?
                WHERE id = ? AND status != 'RUNNING'
                """,
                (time.time(), account_id),
            ).rowcount
        )

    def deactivate_account(self, account_id: int) -> bool:
        return bool(
            self.connection.execute(
                """
                UPDATE accounts
                SET enabled = 0,
                    status = CASE
                        WHEN status IN ('READY', 'RUNNING') THEN 'STOPPED'
                        ELSE status
                    END,
                    updated_at = ?
                WHERE id = ?
                """,
                (time.time(), account_id),
            ).rowcount
        )

    def remove_audiences(self, account_id: int, masked_ids: tuple[str, ...]) -> None:
        if not masked_ids:
            return
        placeholders = ",".join("?" for _ in masked_ids)
        with self.connection:
            self.connection.execute(
                f"DELETE FROM audiences WHERE account_id = ? "
                f"AND masked_id IN ({placeholders})",
                (account_id, *masked_ids),
            )

    def get_audiences(self, account_id: int) -> list[sqlite3.Row]:
        return self.connection.execute(
            "SELECT * FROM audiences WHERE account_id = ? ORDER BY position",
            (account_id,),
        ).fetchall()

    def save_account_config(
        self,
        account_id: int,
        plan_ids: list[str],
        people: list[tuple[str, str]],
    ) -> None:
        """校验并以一个事务同时替换票档、观演人和运行状态。"""
        if not plan_ids or len(plan_ids) != len(set(plan_ids)):
            raise ValueError("至少选择一个不重复的票档")
        if not people:
            raise ValueError("至少添加一位观演人")
        masked_ids = [masked_id for _, masked_id in people]
        if len(masked_ids) != len(set(masked_ids)):
            raise ValueError("同一证件不能重复添加")

        self.connection.execute("BEGIN IMMEDIATE")
        try:
            account = self.get_account(account_id)
            if not account:
                raise ValueError(f"账号 #{account_id} 不存在")
            if account["status"] in {"RUNNING", "CREATED", "UNKNOWN"}:
                raise ValueError("账号当前状态不允许修改配置")
            known = {
                row["seat_plan_id"] for row in self.get_task_plans(account["task_id"])
            }
            if not set(plan_ids) <= known:
                raise ValueError("票档包含其他场次或已失效的编号")
            placeholders = ",".join("?" for _ in masked_ids)
            conflict = self.connection.execute(
                f"""
                SELECT a.id, u.masked_id
                FROM audiences u
                JOIN accounts a ON a.id = u.account_id
                WHERE a.task_id = ? AND a.id != ?
                  AND u.masked_id IN ({placeholders})
                LIMIT 1
                """,
                (account["task_id"], account_id, *masked_ids),
            ).fetchone()
            if conflict:
                raise ValueError(
                    f"证件 {conflict['masked_id']} 已配置在同场次账号 #{conflict['id']}"
                )
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
            self.connection.execute(
                """
                UPDATE accounts
                SET enabled = 0, status = 'STOPPED', order_id = NULL,
                    last_error = '', updated_at = ?
                WHERE id = ?
                """,
                (time.time(), account_id),
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

    def delete_account(self, account_id: int) -> str | None:
        account = self.get_account(account_id)
        if not account:
            return None
        with self.connection:
            self.connection.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        return str(account["profile_key"])
