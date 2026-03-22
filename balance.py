from core import points_redis
from config import SHARED_POINTS_INIT


def _points_key(uid: int | str) -> str:
    return f"user_balance:{uid}"


async def get_or_init_balance(uid: int | str) -> float:
    key = _points_key(uid)
    await points_redis.setnx(key, SHARED_POINTS_INIT)
    raw = await points_redis.get(key)
    return round(float(raw or SHARED_POINTS_INIT), 2)


async def update_balance(uid: int | str, amount: float) -> float:
    if amount == 0:
        return await get_or_init_balance(uid)
    await get_or_init_balance(uid)
    val = await points_redis.incrbyfloat(_points_key(uid), round(amount, 2))
    return round(float(val), 2)
