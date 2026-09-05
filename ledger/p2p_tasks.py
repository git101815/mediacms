from __future__ import annotations

from celery import shared_task


@shared_task(name="ledger.expire_p2p_agent_offer", queue="short_tasks")
def expire_p2p_agent_offer(assignment_id: int):
    from .p2p_services import expire_p2p_agent_assignment

    state, _order, remaining = expire_p2p_agent_assignment(assignment_id=assignment_id)
    if state == "not_yet" and remaining:
        expire_p2p_agent_offer.apply_async(args=[assignment_id], countdown=remaining)
    return state


@shared_task(name="ledger.expire_p2p_trade", queue="short_tasks")
def expire_p2p_trade(order_id: int):
    from .p2p_services import expire_p2p_trade_if_due

    state, _order, remaining = expire_p2p_trade_if_due(order_id=order_id)
    if state == "not_yet" and remaining:
        expire_p2p_trade.apply_async(args=[order_id], countdown=remaining)
    return state
