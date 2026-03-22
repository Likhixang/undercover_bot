import asyncio
import logging

from aiogram.types import BotCommand, BotCommandScopeAllGroupChats
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from config import (
    RUN_MODE,
    WEBHOOK_BASE_URL,
    WEBHOOK_PATH,
    WEBHOOK_HOST,
    WEBHOOK_PORT,
    WEBHOOK_SECRET_TOKEN,
)
from core import bot, dp, redis, points_redis
from handlers import router, speech_watchdog_loop

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

dp.include_router(router)

COMMANDS = [
    BotCommand(command="uc_help", description="谁是卧底帮助"),
    BotCommand(command="uc_new", description="创建房间"),
    BotCommand(command="uc_join", description="加入房间"),
    BotCommand(command="uc_leave", description="退出房间/中途出局"),
    BotCommand(command="uc_start", description="房主开局"),
    BotCommand(command="uc_say", description="本轮发言"),
    BotCommand(command="uc_status", description="查看房间状态"),
    BotCommand(command="uc_bal", description="查看我的积分"),
    BotCommand(command="uc_end", description="房主解散房间"),
    BotCommand(command="uc_force_stop", description="超管强制终止对局"),
    BotCommand(command="uc_maintain", description="超管停机维护"),
    BotCommand(command="uc_compensate", description="超管停机补偿 [更新内容]"),
]


async def main():
    await bot.set_my_commands(COMMANDS)
    await bot.set_my_commands(COMMANDS, scope=BotCommandScopeAllGroupChats())
    asyncio.create_task(speech_watchdog_loop())

    runner: web.AppRunner | None = None
    configured_mode = (RUN_MODE or "polling").strip().lower()
    if configured_mode not in {"polling", "webhook"}:
        logger.warning("Unknown RUN_MODE=%s, fallback to polling", RUN_MODE)
        configured_mode = "polling"

    effective_mode = configured_mode
    if configured_mode == "webhook" and not WEBHOOK_BASE_URL:
        logger.warning("WEBHOOK_BASE_URL missing, fallback to polling")
        effective_mode = "polling"

    webhook_path = WEBHOOK_PATH if WEBHOOK_PATH.startswith("/") else f"/{WEBHOOK_PATH}"

    try:
        if effective_mode == "webhook":
            try:
                webhook_url = f"{WEBHOOK_BASE_URL.rstrip('/')}{webhook_path}"
                await bot.set_webhook(
                    url=webhook_url,
                    secret_token=WEBHOOK_SECRET_TOKEN or None,
                    drop_pending_updates=True,
                )

                app = web.Application()
                request_handler = SimpleRequestHandler(
                    dispatcher=dp,
                    bot=bot,
                    secret_token=WEBHOOK_SECRET_TOKEN or None,
                )
                request_handler.register(app, path=webhook_path)
                setup_application(app, dp, bot=bot)

                runner = web.AppRunner(app)
                await runner.setup()
                site = web.TCPSite(runner, host=WEBHOOK_HOST, port=WEBHOOK_PORT)
                await site.start()
                logger.info("Webhook started at %s%s", WEBHOOK_BASE_URL.rstrip("/"), webhook_path)
                await asyncio.Event().wait()
            except Exception as e:
                logger.exception("Webhook startup failed, fallback to polling: %s", e)
                effective_mode = "polling"
                try:
                    await bot.delete_webhook(drop_pending_updates=False)
                except Exception:
                    pass
                logger.info("Bot running in polling mode")
                await dp.start_polling(bot)
        else:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("Bot running in polling mode")
            await dp.start_polling(bot)
    finally:
        if effective_mode == "webhook":
            try:
                await bot.delete_webhook(drop_pending_updates=False)
            except Exception:
                pass
            if runner is not None:
                try:
                    await runner.cleanup()
                except Exception:
                    pass
        await redis.aclose()
        if points_redis is not redis:
            await points_redis.aclose()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
