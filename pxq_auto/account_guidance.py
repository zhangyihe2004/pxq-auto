"""账号状态标签与下一步操作提示。"""

from __future__ import annotations

from .config import required_audience_count
from .db import Database


STATUS_LABELS = {
    "RESERVED": "待完成登录",
    "STOPPED": "已停止",
    "READY": "等待抢票",
    "RUNNING": "正在抢票",
    "NEEDS_LOGIN": "登录已失效",
    "CREATED": "订单已创建",
    "UNKNOWN": "订单结果未知",
    "COMPLETE": "目标数量已完成",
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
        return f"下一步：处理待支付订单；如已取消并需继续，发送：重置 {account_id}"
    if status == "UNKNOWN":
        return f"下一步：检查待支付订单；确认无订单后发送：重置 {account_id}"
    if status == "COMPLETE":
        return f"下一步：配置 {account_id}（设置新的目标数量）"
    people = db.get_audiences(account_id)
    required = (
        required_audience_count(task["real_name_mode"], int(account["quantity"]))
        if task
        else -1
    )
    if (
        not db.get_account_plans(account_id)
        or account["quantity"] < 1
        or len(people) != required
    ):
        return f"下一步：配置 {account_id}"
    if status == "STOPPED":
        return f"下一步：启动 {account_id}"
    if status == "READY" and task and task["status"] == "paused":
        return f"账号已启用；任务暂停中，发送：恢复 {task['id']}"
    return ""
