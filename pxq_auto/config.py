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
ARTIFACTS_DIR = BASE_DIR / "artifacts"

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

    @property
    def quantity(self) -> int:
        return len(self.audiences)


@dataclass(frozen=True)
class BrowserConfig:
    profile_dir: Path
    headless: bool
    timeout_ms: int


@dataclass(frozen=True)
class SafetyConfig:
    create_order: bool


@dataclass(frozen=True)
class AppConfig:
    project: ProjectConfig
    purchase: PurchaseConfig
    browser: BrowserConfig
    safety: SafetyConfig
    config_path: Path

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

    def render_plan(
        self,
        *,
        status: str,
        quantity: int | None = None,
        audiences: tuple[AudienceConfig, ...] | None = None,
    ) -> str:
        people = self.purchase.audiences if audiences is None else audiences
        count = len(people) if quantity is None else quantity
        lines = [
            f"演出：{self.project.name}",
            f"场次：{self.purchase.session}",
            f"状态：{status}",
            f"数量：{count}",
            f"观演人（{len(people[:count])}）：",
        ]
        lines.extend(
            f"✓ {index}. {person.name}｜{person.masked_id}"
            for index, person in enumerate(people[:count], 1)
        )
        lines.append(f"票档优先级（{len(self.purchase.plans)}）：")
        lines.extend(
            f"✓ {index}. {name}" for index, name in enumerate(self.purchase.plans, 1)
        )
        lines.append("操作：只创建订单，不支付")
        return "\n".join(lines)


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
    merged = {**DEFAULT_CONFIG, **raw}
    timeout = merged["browser_timeout_seconds"]
    concurrent = merged["max_concurrent_accounts"]
    if not isinstance(timeout, int) or not 5 <= timeout <= 120:
        raise ValueError("browser_timeout_seconds 必须在 5~120 之间")
    if not isinstance(concurrent, int) or not 1 <= concurrent <= 20:
        raise ValueError("max_concurrent_accounts 必须在 1~20 之间")
    return SystemConfig(
        raw=merged,
        browser_headless=bool(merged["browser_headless"]),
        browser_timeout_ms=timeout * 1000,
        max_concurrent_accounts=concurrent,
        create_order_enabled=bool(merged["create_order_enabled"]),
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
    show_id = task["show_id"]
    session_id = task["session_id"]
    booking_url = (
        f"https://m.piaoxingqiu.com/booking/{show_id}"
        f"?saleShowSessionId={session_id}&seatPickType=SUPPORT_SEAT&showId={show_id}"
    )
    home = account_home(account["profile_key"])
    return AppConfig(
        project=ProjectConfig(task["show_name"], booking_url),
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
        safety=SafetyConfig(system.create_order_enabled),
        config_path=CONFIG_PATH,
    )


def build_login_config(task, account, system: SystemConfig) -> AppConfig:
    show_id = task["show_id"]
    session_id = task["session_id"]
    home = account_home(account["profile_key"])
    return AppConfig(
        project=ProjectConfig(
            task["show_name"],
            f"https://m.piaoxingqiu.com/booking/{show_id}"
            f"?saleShowSessionId={session_id}&seatPickType=SUPPORT_SEAT&showId={show_id}",
        ),
        purchase=PurchaseConfig(task["session_name"], (), (), ()),
        browser=BrowserConfig(
            home / "browser-profile", True, system.browser_timeout_ms
        ),
        safety=SafetyConfig(False),
        config_path=CONFIG_PATH,
    )


def validate_phone(value: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"1\d{10}", value):
        raise ValueError("请输入 11 位大陆手机号")
    return value


def validate_masked_id(value: str) -> str:
    value = value.strip()
    if not re.fullmatch(r"\d{3}\*+[0-9Xx]{4}", value):
        raise ValueError("证件号格式应与票星球打码显示一致，如 110***********1234")
    return value


def mask_phone(phone: str) -> str:
    return f"{phone[:3]}****{phone[-4:]}"
