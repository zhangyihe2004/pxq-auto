from __future__ import annotations

from .db import Database


def plan_prompt(db: Database, account_id: int) -> str:
    account = db.get_account(account_id)
    if not account:
        return f"账号 #{account_id} 不存在。"
    plans = db.get_task_plans(account["task_id"])
    selected = {
        row["seat_plan_id"]: index
        for index, row in enumerate(db.get_account_plans(account_id), 1)
    }
    lines = [f"账号 #{account_id} 票档（{len(plans)}）："]
    for number, plan in enumerate(plans, 1):
        priority = selected.get(plan["seat_plan_id"])
        mark = "✓" if priority else "✗"
        state = (
            f"有票 {plan['can_buy_count']} 张"
            if plan["sale_started"] and plan["can_buy_count"] > 0
            else "可售但无票"
            if plan["sale_started"]
            else "未开售"
        )
        chosen = f"｜优先级 {priority}" if priority else ""
        lines.append(
            f"{mark} {number}. {plan['plan_name']}｜¥{plan['price']:g}｜{state}{chosen}"
        )
    current = db.get_account_plans(account_id)
    lines.append(
        "当前优先级："
        + (" → ".join(plan["plan_name"] for plan in current) if current else "未配置")
    )
    lines.extend(("", f"发送：票档 {account_id} <编号列表|全部>"))
    return "\n".join(lines)


def audience_prompt(db: Database, account_id: int) -> str:
    if not db.get_account(account_id):
        return f"账号 #{account_id} 不存在。"
    people = db.get_audiences(account_id)
    lines = [f"账号 #{account_id} 观演人（{len(people)}）："]
    lines.extend(
        f"✓ {index}. {person['name']}｜{person['masked_id']}"
        for index, person in enumerate(people, 1)
    )
    if not people:
        lines.append("尚未配置")
    lines.extend(
        (
            "",
            "发送：观演人 "
            f"{account_id} 姓名|打码证件号[,姓名|打码证件号]",
        )
    )
    return "\n".join(lines)


def next_step(db: Database, account_id: int) -> str:
    account = db.get_account(account_id)
    if not account:
        return ""
    status = account["status"]
    if status == "NEEDS_LOGIN":
        return f"下一步：发送“登录 {account['task_id']}”重新登录账号。"
    if not db.get_account_plans(account_id):
        return f"下一步：发送“票档 {account_id}”查看并选择票档。"
    if not db.get_audiences(account_id):
        return f"下一步：发送“观演人 {account_id}”查看配置格式。"
    if status == "READY":
        return "配置完成：账号已进入 READY，将按任务状态自动抢票。"
    if status == "RUNNING":
        return "账号正在执行抢票，请勿同时修改配置。"
    if status == "CREATED":
        return "已创建待支付订单；程序不会支付。"
    if status == "UNKNOWN":
        return (
            "订单结果无法确认，请先人工检查待支付订单；"
            f"确认无订单后发送“重置 {account_id}”。"
        )
    if status == "COMPLETE":
        return "配置的观演人均已购买，无需继续抢票。"
    return f"当前状态：{status}。"
