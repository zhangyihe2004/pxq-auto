from __future__ import annotations

import argparse
import asyncio
import logging
import sqlite3
import sys

import httpx

from . import __version__
from .api import PxqClient, PxqError
from .commands import CommandWorker
from .config import CONFIG_PATH, DB_PATH, load_system_config
from .db import Database
from .engine import AutoEngine
from .feishu import FeishuError, FeishuGateway, IncomingCommand
from .login import FeishuLoginManager
from .service import TaskService


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="pxq-auto", description="票星球监控、飞书控制与自动创建订单"
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("serve", help="启动监控、飞书和多账号抢票服务")
    sub.add_parser("doctor", help="检查配置、数据库和 Playwright")
    sub.add_parser("test-feishu", help="发送一条飞书测试消息")
    args = parser.parse_args()
    try:
        if args.command == "serve":
            asyncio.run(_serve())
        elif args.command == "doctor":
            _doctor()
        else:
            asyncio.run(_test_feishu())
    except KeyboardInterrupt:
        print("\n已停止。", file=sys.stderr)
        raise SystemExit(130) from None
    except (
        ValueError,
        OSError,
        sqlite3.Error,
        httpx.HTTPError,
        PxqError,
        FeishuError,
    ) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        raise SystemExit(1) from exc


async def _serve() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    system = load_system_config()
    db = Database(DB_PATH)
    client = PxqClient()
    queue: asyncio.Queue[IncomingCommand] = asyncio.Queue(maxsize=100)
    feishu = FeishuGateway(system.raw, queue)
    service = TaskService(db, client)
    engine = AutoEngine(db, service, feishu, system)
    login = FeishuLoginManager(db, feishu, system, engine.cancel_account)
    worker = CommandWorker(queue, db, service, engine, login, feishu)
    tasks = [
        asyncio.create_task(engine.run_forever()),
        asyncio.create_task(feishu.run_forever()),
        asyncio.create_task(login.run_forever()),
        asyncio.create_task(worker.run_forever()),
    ]
    if not system.create_order_enabled:
        logging.getLogger("pxq.auto").warning(
            "create_order_enabled=false：监控和登录正常，但不会创建订单"
        )
    try:
        await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await engine.close()
        await login.close()
        await feishu.aclose()
        await client.aclose()
        db.close()


def _doctor() -> None:
    system = load_system_config()
    with Database(DB_PATH):
        pass
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise ValueError(
            "Playwright 未安装，请执行 python -m pip install -e ."
        ) from exc
    with sync_playwright() as playwright:
        chromium = playwright.chromium.executable_path
    from pathlib import Path

    if not Path(chromium).is_file():
        raise ValueError(
            "Chromium 未安装，请执行 python -m playwright install chromium"
        )
    print("环境检查")
    print(f"配置：{CONFIG_PATH}")
    print(f"数据库：{DB_PATH}")
    print(f"Chromium：{chromium}")
    print(f"无界面浏览器：{'是' if system.browser_headless else '否'}")
    print(f"账号并发：{system.max_concurrent_accounts}")
    print(f"创建订单：{'已启用' if system.create_order_enabled else '已禁用'}")
    print("检查：通过")


async def _test_feishu() -> None:
    feishu = FeishuGateway(load_system_config().raw)
    try:
        if not await feishu.send_card(
            "pxq-auto 测试", "飞书应用机器人通信正常。", "green"
        ):
            raise FeishuError("发送失败，请检查应用配置和默认群 chat_id")
    finally:
        await feishu.aclose()
    print("发送成功")
