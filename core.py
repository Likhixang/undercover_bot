from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.filters import BaseFilter
from redis.asyncio import Redis

from config import (
    TOKEN,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_PASSWORD,
    POINTS_REDIS_HOST,
    POINTS_REDIS_PORT,
    POINTS_REDIS_DB,
    POINTS_REDIS_PASSWORD,
)

bot = Bot(
    token=TOKEN,
    default=DefaultBotProperties(parse_mode="HTML"),
    session=AiohttpSession(timeout=12),
)

dp = Dispatcher()
redis = Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True,
    password=REDIS_PASSWORD,
)

if POINTS_REDIS_HOST:
    points_redis = Redis(
        host=POINTS_REDIS_HOST,
        port=POINTS_REDIS_PORT,
        db=POINTS_REDIS_DB,
        decode_responses=True,
        password=POINTS_REDIS_PASSWORD or None,
    )
else:
    points_redis = redis


class CleanTextFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        if not message.entities:
            return True
        for ent in message.entities:
            if ent.type not in ["bot_command", "mention", "phone_number"]:
                return False
        return True
