from __future__ import annotations

from .db import Database


STATUS_LABELS = {
    "RESERVED": "待完成登录",
    "STOPPED": "已停止",
    "READY": "等待抢票",
    "RUNNING": "正在抢票",
    "NEEDS_LOGIN": "登录已失效",
    "CREATED": "订单已创建",
    "UNKNOWN": "订单结果未知",
    "COMPLETE": "观演人均已购",
}


def status_label(status: str) -> str:
    return STATUS_LABELS[status]


def next_step(db: Database, account_id: int) -> str:
    account = db.get_account(account_id)
    if not account:
        return ""
    task = db.get_task(account["task_id"])
    status = account["status"]
    if status in {"RESERVED", "NEEDS_LOGIN"}:
        return f"下一步：登录 {account['task_id']}"
    if status == "CREATED":
        return "已创建待支付订单，程序不会支付。"
    if status == "UNKNOWN":
        return f"请先人工检查待支付订单；确认无订单后发送：重置 {account_id}"
    if not db.get_account_plans(account_id) or not db.get_audiences(account_id):
        return f"下一步：配置 {account_id}"
    if status == "STOPPED":
        return f"下一步：启动 {account_id}"
    if status == "READY" and task and task["status"] == "paused":
        return f"账号已启用；任务暂停中，发送：恢复 {task['id']}"
    return {
        "READY": "账号已启动，正在等待开售或回流。",
        "RUNNING": "账号正在执行抢票。",
        "COMPLETE": f"如需更换观演人，发送：配置 {account_id}",
    }.get(status, "")
