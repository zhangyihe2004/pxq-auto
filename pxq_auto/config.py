from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(os.environ.get("PXQ_AUTO_DIR", Path.home() / ".pxq-auto"))
CONFIG_PATH = BASE_DIR / "config.json"
DB_PATH = BASE_DIR / "pxq-auto.db"
ACCOUNTS_DIR = BASE_DIR / "accounts"

DEFAULT_CONFIG = {
    "feishu_app_id": "",
    "feishu_app_secret": "",
    "feishu_admin_open_ids": [],
    "feishu_default_chat_id": "",
    "browser_headless": True,
    "browser_timeout_seconds": 10,
    "max_concurrent_accounts": 3,
    "create_order_enabled": False,
}


@dataclass(frozen=True)
class SystemConfig:
    raw: dict
    browser_headless: bool
    browser_timeout_ms: int
    max_concurrent_accounts: int
    create_order_enabled: bool


@dataclass(frozen=True)
class ProjectConfig:
    name: str
    booking_url: str
    support_seat_picking: bool


@dataclass(frozen=True)
class AudienceConfig:
    name: str
    masked_id: str


@dataclass(frozen=True)
class PurchaseConfig:
    session: str
    plans: tuple[str, ...]
    plan_ids: tuple[str, ...]
    audiences: tuple[AudienceConfig, ...]


@dataclass(frozen=True)
class BrowserConfig:
    profile_dir: Path
    headless: bool
    timeout_ms: int


@dataclass(frozen=True)
class AppConfig:
    project: ProjectConfig
    purchase: PurchaseConfig
    browser: BrowserConfig
    create_order: bool

    @property
    def state_path(self) -> Path:
        return self.browser.profile_dir.parent / "order-state.json"

    @property
    def plan_key(self) -> str:
        raw = "\n".join(
            (
                self.project.name,
                self.project.booking_url,
                self.purchase.session,
                *self.purchase.plans,
                *self.purchase.plan_ids,
                *(f"{a.name}|{a.masked_id}" for a in self.purchase.audiences),
            )
        )
        return hashlib.sha256(raw.encode()).hexdigest()[:16]


def load_system_config() -> SystemConfig:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(
            json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    if os.name == "posix":
        CONFIG_PATH.chmod(0o600)
    raw = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("config.json 根节点必须是对象")
    expected = set(DEFAULT_CONFIG)
    missing = expected - set(raw)
    unknown = set(raw) - expected
    if missing:
        raise ValueError(f"config.json 缺少字段：{', '.join(sorted(missing))}")
    if unknown:
        raise ValueError(f"config.json 包含未知字段：{', '.join(sorted(unknown))}")
    for key in ("feishu_app_id", "feishu_app_secret", "feishu_default_chat_id"):
        if not isinstance(raw[key], str):
            raise ValueError(f"config.json 字段 {key} 必须是字符串")
    admins = raw["feishu_admin_open_ids"]
    if not isinstance(admins, list) or any(
        not isinstance(item, str) for item in admins
    ):
        raise ValueError("feishu_admin_open_ids 必须是字符串数组")
    for key in ("browser_headless", "create_order_enabled"):
        if not isinstance(raw[key], bool):
            raise ValueError(f"config.json 字段 {key} 必须是 true 或 false")
    timeout = raw["browser_timeout_seconds"]
    concurrent = raw["max_concurrent_accounts"]
    if type(timeout) is not int or not 5 <= timeout <= 120:
        raise ValueError("browser_timeout_seconds 必须在 5~120 之间")
    if type(concurrent) is not int or not 1 <= concurrent <= 20:
        raise ValueError("max_concurrent_accounts 必须在 1~20 之间")
    return SystemConfig(
        raw=raw,
        browser_headless=raw["browser_headless"],
        browser_timeout_ms=timeout * 1000,
        max_concurrent_accounts=concurrent,
        create_order_enabled=raw["create_order_enabled"],
    )


def account_home(profile_key: str) -> Path:
    return ACCOUNTS_DIR / profile_key


def remove_account_home(key: str) -> None:
    home = account_home(key).resolve()
    root = ACCOUNTS_DIR.resolve()
    if home.parent != root:
        raise RuntimeError("拒绝删除账号目录：路径越界")
    if home.exists():
        shutil.rmtree(home)


def profile_key(phone: str) -> str:
    return hashlib.sha256(phone.encode()).hexdigest()[:20]


def build_order_config(
    task, plans, audiences, account, system: SystemConfig
) -> AppConfig:
    if not plans:
        raise RuntimeError("账号尚未配置票档")
    people = tuple(
        AudienceConfig(person["name"], person["masked_id"]) for person in audiences
    )
    if not people:
        raise RuntimeError("账号尚未配置观演人")
    home = account_home(account["profile_key"])
    return AppConfig(
        project=_project_config(task),
        purchase=PurchaseConfig(
            task["session_name"],
            tuple(plan["plan_name"] for plan in plans),
            tuple(plan["seat_plan_id"] for plan in plans),
            people,
        ),
        browser=BrowserConfig(
            home / "browser-profile",
            system.browser_headless,
            system.browser_timeout_ms,
        ),
        create_order=system.create_order_enabled,
    )


def build_login_config(task, account, system: SystemConfig) -> AppConfig:
    home = account_home(account["profile_key"])
    return AppConfig(
        project=_project_config(task),
        purchase=PurchaseConfig(task["session_name"], (), (), ()),
        browser=BrowserConfig(
            home / "browser-profile", True, system.browser_timeout_ms
        ),
        create_order=False,
    )


def _project_config(task) -> ProjectConfig:
    show_id = task["show_id"]
    session_id = task["session_id"]
    support_seat_picking = bool(task["support_seat_picking"])
    seat_pick_type = "SUPPORT_SEAT" if support_seat_picking else "SUPPORT_NONE"
    return ProjectConfig(
        task["show_name"],
        f"https://m.piaoxingqiu.com/booking/{show_id}"
        f"?saleShowSessionId={session_id}&seatPickType={seat_pick_type}&showId={show_id}",
        support_seat_picking,
    )


def validate_phone(value: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"1\d{10}", value):
        raise ValueError("请输入 11 位大陆手机号")
    return value


def mask_id(value: str) -> str:
    parts = value.split()
    if (
        len(parts) != 2
        or not re.fullmatch(r"\d{3}", parts[0])
        or not re.fullmatch(r"[0-9Xx]{4}", parts[1])
    ):
        raise ValueError("身份证号只需输入前 3 位和后 4 位，如 210 2534")
    return f"{parts[0]}{'*' * 11}{parts[1].upper()}"


def mask_phone(phone: str) -> str:
    return f"{phone[:3]}****{phone[-4:]}"
