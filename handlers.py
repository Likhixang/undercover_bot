import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Dict

import httpx
from aiogram import BaseMiddleware, F, Router, types
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from config import (
    ALLOWED_CHAT_ID,
    ALLOWED_THREAD_ID,
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    AUTO_START_IDLE_SECONDS,
    BET_PER_PLAYER,
    MAX_PLAYERS,
    MIN_PLAYERS,
    SPEAK_REMIND_BEFORE_SECONDS,
    SPEAK_TIMEOUT_SECONDS,
    SUPER_ADMIN_IDS,
    VOTING_TIMEOUT_SECONDS,
    WATCHDOG_INTERVAL_SECONDS,
)
from config import COMPENSATION_AMOUNT
from balance import get_or_init_balance, update_balance
from core import bot, redis, points_redis
from game import UndercoverService
from utils import mention, safe_html, scope_id

_ai_headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"} if OPENAI_API_KEY else None

logger = logging.getLogger(__name__)
router = Router()
svc = UndercoverService()


# ───────────────────── 停机维护中间件 ─────────────────────

_ADMIN_COMMANDS = {"uc_maintain", "uc_compensate", "uc_force_stop"}

_KNOWN_UC_COMMANDS = {
    "uc_help", "uc_new", "uc_join", "uc_leave", "uc_start",
    "uc_say", "uc_status", "uc_bal", "uc_end", "uc_force_stop",
    "uc_maintain", "uc_compensate",
}


class MaintenanceMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any],
    ) -> Any:
        if isinstance(event, types.Message):
            text = (event.text or "").strip()
            # 只拦截 /uc_* 命令
            if text.startswith("/uc_"):
                cmd = text.split()[0].lstrip("/").split("@")[0]
                if cmd not in _ADMIN_COMMANDS:
                    chat_id = event.chat.id
                    if await redis.exists(f"uc:maintenance:{chat_id}"):
                        await event.reply("🔧 系统正在维护中，请稍后再试。")
                        return
        return await handler(event, data)


class ScopeGuardMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any],
    ) -> Any:
        if not ALLOWED_CHAT_ID:
            return await handler(event, data)

        if isinstance(event, types.Message):
            if event.chat.type not in ("group", "supergroup"):
                return await handler(event, data)
            text = (event.text or "").strip()
            if not text.startswith("/uc_") and text != "/start":
                return await handler(event, data)
            if event.chat.id != ALLOWED_CHAT_ID or event.message_thread_id != ALLOWED_THREAD_ID:
                await event.reply("❌ 本 bot 仅在指定话题提供服务。")
                return

        if isinstance(event, types.CallbackQuery):
            msg = event.message
            if not msg:
                return await handler(event, data)
            if msg.chat.type in ("group", "supergroup"):
                if msg.chat.id != ALLOWED_CHAT_ID or msg.message_thread_id != ALLOWED_THREAD_ID:
                    try:
                        await event.answer("❌ 本 bot 仅在指定话题提供服务。", show_alert=True)
                    except Exception:
                        pass
                    return

        return await handler(event, data)


class TelegramResilienceMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any],
    ) -> Any:
        try:
            return await handler(event, data)
        except TelegramNetworkError as e:
            logger.warning("[tg] network error: %s", e)
            return
        except TelegramBadRequest as e:
            msg = str(e).lower()
            if "query is too old" in msg or "query id is invalid" in msg or "message is not modified" in msg:
                logger.info("[tg] ignored bad request: %s", e)
                return
            raise


router.message.middleware(MaintenanceMiddleware())
router.message.middleware(ScopeGuardMiddleware())
router.callback_query.middleware(ScopeGuardMiddleware())
router.message.middleware(TelegramResilienceMiddleware())
router.callback_query.middleware(TelegramResilienceMiddleware())


async def _main_panel_kb(scope: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🆕 创建房间", callback_data="uc_panel:new"),
                InlineKeyboardButton(text="➕ 加入房间", callback_data="uc_panel:join"),
            ],
            [
                InlineKeyboardButton(text="➖ 退出房间", callback_data="uc_panel:leave"),
                InlineKeyboardButton(text="🎬 房主开局", callback_data="uc_panel:start"),
            ],
            [
                InlineKeyboardButton(text="🗣 发言引导", callback_data="uc_panel:say"),
                InlineKeyboardButton(text="📊 房间状态", callback_data="uc_panel:status"),
            ],
            [
                InlineKeyboardButton(text="💰 我的积分", callback_data="uc_panel:bal"),
                InlineKeyboardButton(text="🧹 房主解散", callback_data="uc_panel:end"),
            ],
            [InlineKeyboardButton(text="🔄 刷新面板", callback_data="uc_panel:refresh")],
        ]
    )


def _panel_key(scope: str) -> str:
    return f"uc:panel:{scope}"


def _trigger_key(scope: str) -> str:
    return f"uc:trigger:{scope}"


def _dm_hint_key(uid: int | str) -> str:
    return f"uc:dm_hint:{uid}"


def _speaker_announce_guard_key(scope: str, token: str) -> str:
    return f"uc:announce:{scope}:{token}"


def _speaker_prompt_key(scope: str) -> str:
    return f"uc:speaker_prompt:{scope}"


def _speaker_prompt_set_key(scope: str) -> str:
    return f"uc:speaker_prompts:{scope}"


def _vote_message_key(scope: str) -> str:
    return f"uc:vote_message:{scope}"


def _vote_result_key(scope: str) -> str:
    return f"uc:vote_result:{scope}"


def _vote_finalize_guard_key(scope: str) -> str:
    return f"uc:vote_finalize_guard:{scope}"


def _leave_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="确认退出", callback_data="uc_panel:leave_confirm"),
                InlineKeyboardButton(text="取消", callback_data="uc_panel:leave_cancel"),
            ]
        ]
    )


def _parse_scope(scope: str) -> tuple[int, int | None]:
    chat, thread = scope.split(":", 1)
    t = int(thread)
    return int(chat), (t if t != 0 else None)


async def _ctx_scope(message: types.Message) -> str:
    return scope_id(message.chat.id, message.message_thread_id)


async def _maybe_hint_private_start(chat_id: int, thread_id: int | None, uid: int, name: str) -> None:
    if await redis.setnx(_dm_hint_key(uid), "1") != 1:
        return
    await bot.send_message(
        chat_id,
        (
            f"📩 {mention(uid, name)} 首次参与前请先私聊机器人发送 <code>/start</code>，"
            "否则开局时可能收不到身份词。"
        ),
        message_thread_id=thread_id,
    )


async def _room_text(scope: str) -> str:
    room = await svc.get_room(scope)
    if not room:
        return "ℹ️ 当前没有房间。点击按钮可快速创建。"

    players = await svc.list_players(scope)
    state = room.get("state", "")
    spoken_uids = set(await redis.smembers(svc.spoken_key(scope))) if state in {"speaking", "voting"} else set()
    voted_uids = set(await redis.hkeys(svc.votes_key(scope))) if state == "voting" else set()

    player_lines: list[str] = []
    for p in players:
        alive_text = "在局🟢" if p.alive else "出局⚫"

        if not p.alive or state in {"lobby", "ended"}:
            speech_text = "-"
            vote_text = "-"
        else:
            speech_text = "已发言✅" if p.uid in spoken_uids else "未发言⬜"
            vote_text = "已投票✅" if p.uid in voted_uids else "未投票⬜"
            if state == "speaking":
                vote_text = "-"

        player_lines.append(
            f"• {mention(int(p.uid), p.name)} | 存活:{alive_text} | 发言:{speech_text} | 投票:{vote_text}"
        )
    players_text = "\n".join(player_lines)

    extra_line = ""
    if state == "lobby":
        deadline = int(float(room.get("auto_start_deadline", "0") or 0))
        remain = max(deadline - int(time.time()), 0) if deadline > 0 else 0
        extra_line = f"\n自动开局倒计时：<b>{remain}</b> 秒"
    elif state == "speaking" and room.get("current_speaker_uid"):
        sp = await svc.get_player(scope, room["current_speaker_uid"])
        if sp:
            extra_line = f"\n当前发言：{mention(int(sp['uid']), sp.get('name', sp['uid']))}"
    elif state == "voting":
        voted = await redis.hlen(svc.votes_key(scope))
        alive_count = len(await svc.alive_players(scope))
        deadline = int(float(room.get("voting_deadline", "0") or 0))
        remain = max(deadline - int(time.time()), 0) if deadline > 0 else 0
        extra_line = (
            f"\n投票进度：<b>{voted}/{alive_count}</b>\n"
            f"投票倒计时：<b>{remain}</b> 秒\n"
            "投票规则：在局玩家可投 1 人，局外不可投，且不可投自己。"
        )

    return (
        "🎭 <b>谁是卧底房间面板</b>\n"
        f"阶段：<b>{safe_html(room.get('state', 'unknown'))}</b>\n"
        f"轮次：<b>{safe_html(room.get('round', '0'))}</b>\n"
        f"房主：{mention(int(room.get('host_uid', '0')), room.get('host_name', '未知'))}\n"
        f"押注：每人 <b>{BET_PER_PLAYER}</b> 积分{extra_line}\n\n"
        f"玩家列表：\n{players_text}"
    )


def _role_plan_lines() -> str:
    lines: list[str] = []
    for player_count in range(MIN_PLAYERS, MAX_PLAYERS + 1):
        uc_count, wb_count = svc.role_plan(player_count)
        civ_count = player_count - uc_count - wb_count
        line = f"• {player_count} 人：平民 {civ_count} / 卧底 {uc_count}"
        if wb_count:
            line += f" / 白板 {wb_count}"
        lines.append(line)
    return "\n".join(lines)


def _help_text() -> str:
    return (
        "🎭 <b>谁是卧底帮助</b>\n\n"
        "<b>怎么开始</b>\n"
        f"• 先发送 <code>/start</code> 给机器人，避免开局时收不到身份词\n"
        f"• 群里用 <code>/uc_help</code> 呼出面板，创建、加入、开局、查看状态基本都可直接点按钮\n"
        f"• 房间人数达到 <b>{MIN_PLAYERS}</b> 到 <b>{MAX_PLAYERS}</b> 人后可开局；大厅 <b>{AUTO_START_IDLE_SECONDS}</b> 秒内无人继续加入或退出时，会自动尝试开局\n"
        "• 当前版本同一时间只允许存在一个房间，不能并行开多桌\n\n"
        "<b>流程规则</b>\n"
        "• 开局后机器人会私聊发身份词：平民拿平民词，卧底拿卧底词，白板没有词；若有多名卧底，卧底会在私聊中看到同阵营队友\n"
        "• 发言阶段按顺序轮流描述，不能直接说出词，也不要明显拆字、谐音或贴脸明示\n"
        f"• 当前发言人需在 <b>{SPEAK_TIMEOUT_SECONDS}</b> 秒内发言；可用 <code>/uc_say 你的描述</code>，也可直接回复机器人提示消息发言\n"
        f"• 剩余时间不多时会提醒一次，超时未发言会直接出局，并自动轮到下一位或进入投票\n"
        f"• 全员发言后进入投票阶段，限时 <b>{VOTING_TIMEOUT_SECONDS}</b> 秒；在局玩家各投 1 人，不能主动投自己，超时默认投自己\n"
        "• 进入投票时机器人会附上本轮全部发言摘要，便于统一回看后再投票\n"
        "• 白板被投出后，可在 30 秒内私聊机器人猜词；猜中平民词或卧底词任意一个，结算时可跟获胜阵营一起分奖池\n"
        "• 中途主动退出按出局处理；若退出者是卧底，可能直接触发终局结算\n\n"
        "<b>胜负与积分</b>\n"
        f"• 每局每人押注 <b>{BET_PER_PLAYER}</b> 积分，最终由胜利阵营平分奖池\n"
        "• 自动开局失败时（如人数不足、有人收不到私聊身份词、或积分不足），本局会取消，已扣押注会退回\n"
        "• 卧底全部出局且白板也全部出局：平民胜\n"
        "• 卧底人数大于等于平民人数且场上没有白板：卧底胜\n"
        "• 只剩白板存活：白板胜\n\n"
        "<b>人数与身份配置</b>\n"
        f"{_role_plan_lines()}\n\n"
        "<b>常用指令</b>\n"
        "<code>/uc_help</code> 打开帮助和主面板\n"
        "<code>/uc_new</code> 创建房间\n"
        "<code>/uc_join</code> 加入房间\n"
        "<code>/uc_leave</code> 退出房间或中途出局\n"
        "<code>/uc_start</code> 房主开局\n"
        "<code>/uc_say 你的描述</code> 提交本轮发言\n"
        "<code>/uc_status</code> 刷新房间状态\n"
        "<code>/uc_bal</code> 查看我的积分\n"
        "<code>/uc_end</code> 房主解散房间"
    )


async def _delete_message_later(chat_id: int, message_id: int, delay: int) -> None:
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass


async def _send_temp(chat_id: int, text: str, thread_id: int | None = None, delay: int = 15) -> None:
    sent = await bot.send_message(chat_id, text, message_thread_id=thread_id)
    asyncio.create_task(_delete_message_later(chat_id, sent.message_id, delay))


async def _render_panel(
    scope: str,
    chat_id: int,
    thread_id: int | None,
    tip: str = "",
    force_new: bool = False,
) -> None:
    room = await svc.get_room(scope)
    key = _panel_key(scope)
    current_id = await redis.get(key)
    if not room:
        if current_id:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=int(current_id))
            except Exception:
                pass
            await redis.delete(key)
        return

    if room.get("state") in ("speaking", "voting"):
        return

    body = await _room_text(scope)
    kb = await _main_panel_kb(scope)
    if tip:
        body = f"{tip}\n\n{body}"

    if current_id and not force_new:
        try:
            await bot.edit_message_text(
                body,
                chat_id=chat_id,
                message_id=int(current_id),
                reply_markup=kb,
            )
            return
        except TelegramBadRequest as e:
            # Refresh can hit "message is not modified"; that should not create a new panel.
            if "message is not modified" in str(e).lower():
                return
        except Exception:
            pass

    sent = await bot.send_message(
        chat_id,
        body,
        reply_markup=kb,
        message_thread_id=thread_id,
    )
    await redis.set(key, str(sent.message_id))


async def _render_from_message(message: types.Message, tip: str = "", force_new: bool = False) -> None:
    scope = await _ctx_scope(message)
    await _render_panel(scope, message.chat.id, message.message_thread_id, tip=tip, force_new=force_new)


async def _render_from_callback(cb: types.CallbackQuery, tip: str = "") -> None:
    msg = cb.message
    if not msg:
        return
    scope = scope_id(msg.chat.id, msg.message_thread_id)
    await _render_panel(scope, msg.chat.id, msg.message_thread_id, tip=tip)


async def _set_speaker_deadline(scope: str, seconds: int = SPEAK_TIMEOUT_SECONDS) -> None:
    deadline = int(time.time()) + seconds
    await redis.hset(
        svc.room_key(scope),
        mapping={
            "current_speaker_deadline": str(deadline),
            "current_speaker_reminded": "0",
        },
    )


async def _track_speaker_prompt(scope: str, message_id: int) -> None:
    await redis.sadd(_speaker_prompt_set_key(scope), str(message_id))
    await redis.set(_speaker_prompt_key(scope), str(message_id))


async def _clear_speaker_prompts(scope: str, chat_id: int) -> None:
    prompt_ids = set(await redis.smembers(_speaker_prompt_set_key(scope)))
    current_id = await redis.get(_speaker_prompt_key(scope))
    if current_id:
        prompt_ids.add(str(current_id))

    for prompt_id in prompt_ids:
        if not prompt_id or not str(prompt_id).isdigit():
            continue
        try:
            await bot.delete_message(chat_id=chat_id, message_id=int(prompt_id))
        except Exception:
            pass

    await redis.delete(_speaker_prompt_key(scope))
    await redis.delete(_speaker_prompt_set_key(scope))


async def _clear_vote_messages(scope: str, chat_id: int) -> None:
    for key in (_vote_message_key(scope), _vote_result_key(scope)):
        msg_id = await redis.get(key)
        if not msg_id or not str(msg_id).isdigit():
            await redis.delete(key)
            continue
        try:
            await bot.delete_message(chat_id=chat_id, message_id=int(msg_id))
        except Exception:
            pass
        await redis.delete(key)


async def _cleanup_scope_transient_messages(scope: str, chat_id: int) -> None:
    await _clear_speaker_prompts(scope, chat_id)
    await _clear_vote_messages(scope, chat_id)
    await redis.delete(_vote_finalize_guard_key(scope))


async def _snapshot_review_context(scope: str) -> tuple[list[tuple[str, str]], dict[str, str]]:
    logs = await svc.list_speech_logs(scope)
    players = await svc.list_players(scope)
    return logs, {p.uid: p.name for p in players}


async def _finalize_scope(scope: str, chat_id: int, thread_id: int | None) -> None:
    await _cleanup_scope_transient_messages(scope, chat_id)
    await svc.destroy_room(scope)
    await _render_panel(scope, chat_id, thread_id)


async def _announce_current_speaker(scope: str, tip: str = "") -> None:
    room = await svc.get_room(scope)
    uid = room.get("current_speaker_uid", "")
    if not uid:
        return
    p = await svc.get_player(scope, uid)
    if not p:
        return
    token = f"{room.get('round', '0')}:{room.get('speak_index', '0')}:{uid}"
    guard_key = _speaker_announce_guard_key(scope, token)
    if await redis.setnx(guard_key, "1") != 1:
        return
    await redis.expire(guard_key, max(SPEAK_TIMEOUT_SECONDS * 2, 180))
    chat_id, thread_id = _parse_scope(scope)
    logs = await svc.list_speech_logs(scope)
    history_lines: list[str] = []
    for idx, (speaker_uid, content) in enumerate(logs, start=1):
        sp = await svc.get_player(scope, speaker_uid)
        speaker_name = sp.get("name", speaker_uid) if sp else speaker_uid
        history_lines.append(f"{idx}. {safe_html(speaker_name)}：{safe_html(content)}")

    history_text = ""
    if history_lines:
        history_text = "🧾 <b>前面玩家发言</b>\n" + "\n".join(history_lines) + "\n\n"

    text = (
        f"{tip}\n" if tip else ""
    ) + history_text + (
        "🗣 <b>发言轮到你了</b>\n"
        f"当前发言人：{mention(int(uid), p.get('name', uid))}\n"
        f"请在 <b>{SPEAK_TIMEOUT_SECONDS}</b> 秒内发送 <code>/uc_say 你的描述</code>，"
        "或直接回复本条消息发言。"
    )
    try:
        sent = await bot.send_message(chat_id, text, message_thread_id=thread_id)
        await _track_speaker_prompt(scope, sent.message_id)
    except Exception:
        await redis.delete(guard_key)
        raise


async def _player_speech_content(scope: str, uid: int | str) -> str:
    logs = await svc.list_speech_logs(scope)
    target_uid = str(uid)
    for speaker_uid, content in logs:
        if speaker_uid == target_uid:
            return content
    return "本轮无有效发言记录。"


async def _find_pending_blank_scope(uid: int) -> tuple[str, dict[str, str]] | tuple[None, None]:
    for scope in await svc.open_rooms():
        p = await svc.get_player(scope, uid)
        if p and p.get("role") == "whiteboard" and p.get("blank_guess_state") == "pending":
            return scope, p
    return None, None


async def _speech_summary_text(scope: str) -> str:
    logs = await svc.list_speech_logs(scope)
    if not logs:
        return "🧾 <b>本轮发言</b>\n暂无有效发言记录。"

    lines: list[str] = []
    for idx, (uid, content) in enumerate(logs, start=1):
        p = await svc.get_player(scope, uid)
        name = p.get("name", uid) if p else uid
        lines.append(f"{idx}. {mention(int(uid), name)}：{safe_html(content)}")
    return "🧾 <b>本轮发言</b>\n" + "\n".join(lines)


async def _voting_message_text(scope: str, tip: str = "") -> str:
    summary_text = await _speech_summary_text(scope)
    room = await svc.get_room(scope)
    voted_uids = set(await redis.hkeys(svc.votes_key(scope)))
    alive_players = await svc.alive_players(scope)
    alive_count = len(alive_players)
    voted = len(voted_uids)
    candidates = "\n".join([
        f"• {mention(int(p.uid), p.name)} {'已投票✅' if p.uid in voted_uids else '未投票⬜'}"
        for p in alive_players
    ])
    deadline = int(float(room.get("voting_deadline", "0") or 0)) if room else 0
    remain = max(deadline - int(time.time()), 0) if deadline > 0 else VOTING_TIMEOUT_SECONDS
    vote_tip = (
        "🗳 <b>进入投票阶段</b>\n"
        f"投票进度：<b>{voted}/{alive_count}</b>\n"
        f"剩余时间：<b>{remain}</b> 秒\n"
        f"超时未投将默认投自己。\n"
        "请点击下方昵称按钮投票，不可主动投自己。\n\n"
        "候选列表：\n"
        f"{candidates}"
    )
    if tip:
        return f"{tip}\n\n{summary_text}\n\n{vote_tip}"
    return f"{summary_text}\n\n{vote_tip}"


async def _voting_kb(scope: str) -> InlineKeyboardMarkup:
    alive = await svc.alive_players(scope)
    rows = [[InlineKeyboardButton(text=p.name, callback_data=f"uc_vote:{p.uid}")] for p in alive]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _announce_voting_with_summary(scope: str, chat_id: int, thread_id: int | None, tip: str = "") -> None:
    await _clear_speaker_prompts(scope, chat_id)
    await redis.delete(_vote_finalize_guard_key(scope))
    text = await _voting_message_text(scope, tip=tip)
    kb = await _voting_kb(scope)
    current_id = await redis.get(_vote_message_key(scope))
    if current_id:
        try:
            await bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=int(current_id),
                reply_markup=kb,
            )
            return
        except Exception:
            pass
    sent = await bot.send_message(chat_id, text, reply_markup=kb, message_thread_id=thread_id)
    await redis.set(_vote_message_key(scope), str(sent.message_id))


async def _finalize_voting(scope: str, chat_id: int, thread_id: int | None, result_tip: str = "") -> None:
    guard_key = _vote_finalize_guard_key(scope)
    if await redis.setnx(guard_key, "1") != 1:
        return
    await redis.expire(guard_key, max(VOTING_TIMEOUT_SECONDS * 2, 120))

    try:
        room = await svc.get_room(scope)
        if not room or room.get("state") != "voting":
            return

        result = await svc.finish_voting(scope)
        vote_msg_id = await redis.get(_vote_message_key(scope))
        if vote_msg_id:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=int(vote_msg_id))
            except Exception:
                pass
        await redis.delete(_vote_message_key(scope))

        if result.get("tie"):
            tie_tip = f"⚖️ {safe_html(result.get('reason', '平票'))}，无人出局。"
            if result_tip:
                tie_tip += f"\n{safe_html(result_tip)}"
            winner = result.get("winner")
            if winner:
                civ_word, uc_word, uc_name, wb_info = await svc.reveal_words(scope)
                winner_cn = "平民" if winner == "civilian" else ("白板" if winner == "whiteboard" else "卧底")
                payout = result.get("payout", {})
                await _finish_game(
                    scope,
                    chat_id,
                    thread_id,
                    (
                        f"{tie_tip}\n"
                        f"🏁 <b>游戏结束</b>，胜利阵营：<b>{winner_cn}</b>\n"
                        f"平民词：<b>{safe_html(civ_word)}</b>\n"
                        f"卧底词：<b>{safe_html(uc_word)}</b>\n"
                        f"卧底有：{uc_name}"
                        + _whiteboard_text(wb_info)
                        + _payout_text(payout)
                    ),
                )
                return
            await _after_round_continue(scope, room_tip=f"{tie_tip}\n➡️ 下一轮开始")
            return

        eliminated_uid = int(result["eliminated"])
        player = await svc.get_player(scope, eliminated_uid)
        player_name = player.get("name", str(eliminated_uid)) if player else str(eliminated_uid)
        elim_name = mention(eliminated_uid, player_name)
        speech_content = result.get("eliminated_speech") or await _player_speech_content(scope, eliminated_uid)
        blank_guess_pending = result.get("blank_guess_pending", False)

        winner = result.get("winner")
        if winner:
            civ_word, uc_word, uc_name, wb_info = await svc.reveal_words(scope)
            winner_cn = "平民" if winner == "civilian" else ("白板" if winner == "whiteboard" else "卧底")
            payout = result.get("payout", {})
            await _finish_game(
                scope,
                chat_id,
                thread_id,
                (
                    "🏁 <b>游戏结束</b>\n"
                    f"本轮出局：{elim_name}\n"
                    f"该玩家发言：{safe_html(speech_content)}\n"
                    f"胜利阵营：<b>{winner_cn}</b>\n"
                    f"平民词：<b>{safe_html(civ_word)}</b>\n"
                    f"卧底词：<b>{safe_html(uc_word)}</b>\n"
                    f"卧底有：{uc_name}"
                    + _whiteboard_text(wb_info)
                    + (f"\n{safe_html(result_tip)}" if result_tip else "")
                    + f"{_payout_text(payout)}"
                ),
            )
            return

        result_text = (
            "☠️ <b>投票结果</b>\n"
            f"本轮出局：{elim_name}\n"
            f"该玩家发言：{safe_html(speech_content)}"
        )
        if blank_guess_pending:
            result_text += (
                "\n该玩家身份为 <b>白板</b>，请在 <b>30</b> 秒内私聊机器人发送你猜测的词。"
                "猜中平民词或卧底词任意一个即可在最终结算时与获胜方一起分奖池。"
            )
        if result_tip:
            result_text += f"\n{safe_html(result_tip)}"
        sent = await bot.send_message(chat_id, result_text, message_thread_id=thread_id)
        await redis.set(_vote_result_key(scope), str(sent.message_id))
        await _after_round_continue(scope, room_tip="➡️ 下一轮开始")
    finally:
        await redis.delete(guard_key)


def _payout_text(payout: dict) -> str:
    if not payout:
        return ""
    return (
        f"\n奖池：<b>{payout.get('total_pot', 0)}</b> | "
        f"胜方人数：<b>{payout.get('winner_count', 0)}</b> | "
        f"人均分得：<b>{payout.get('share', 0)}</b>"
    )


def _whiteboard_text(wb_info: list[dict]) -> str:
    if not wb_info:
        return ""
    lines = []
    for wb in wb_info:
        bonus = "✅ 获得积分资格" if wb["bonus"] else "❌ 未获积分资格"
        lines.append(f"{wb['name']}（{bonus}）")
    return "\n白板有：" + "、".join(lines)


async def _process_leave_result(scope: str, actor_uid: int, actor_name: str, result: dict) -> tuple[str, bool]:
    msg = result.get("message", "")
    leave_type = result.get("type", "")
    room = await svc.get_room(scope)
    chat_id, thread_id = _parse_scope(scope)

    if leave_type == "already_out":
        return msg, False

    if leave_type == "lobby_leave":
        notify = result.get("notify", "")
        if notify and room:
            await _send_temp(chat_id, f"ℹ️ {safe_html(notify)}", thread_id)
        return msg, room is None

    if leave_type == "ended_after_leave":
        winner = result.get("winner")
        winner_cn = "平民" if winner == "civilian" else ("白板" if winner == "whiteboard" else "卧底")
        civ_word, uc_word, uc_name, wb_info = await svc.reveal_words(scope)
        await _finish_game(
            scope,
            chat_id,
            thread_id,
            (
                f"🚪 {mention(actor_uid, actor_name)} 主动退出，本局按出局处理。\n"
                f"🏁 游戏结束，胜利阵营：<b>{winner_cn}</b>\n"
                f"平民词：<b>{safe_html(civ_word)}</b>\n"
                f"卧底词：<b>{safe_html(uc_word)}</b>\n"
                f"卧底有：{uc_name}"
                + _whiteboard_text(wb_info)
                + f"{_payout_text(result.get('payout', {}))}"
            ),
        )
        return msg, True

    if leave_type == "left_next_speaker":
        nxt = result.get("next_speaker", {})
        await _send_temp(
            chat_id,
            (
                f"🚪 {mention(actor_uid, actor_name)} 主动退出，本局按出局处理。\n"
                f"➡️ 下一位：{mention(int(nxt.get('uid')), nxt.get('name', nxt.get('uid', '')))}"
            ),
            thread_id,
        )
        await _set_speaker_deadline(scope)
        await _announce_current_speaker(scope)
        return msg, False

    if leave_type == "left_to_voting":
        await _announce_voting_with_summary(
            scope,
            chat_id,
            thread_id,
            tip=f"🚪 {mention(actor_uid, actor_name)} 主动退出，本局按出局处理。\n🗳 自动进入投票阶段。",
        )
        return msg, False

    if leave_type == "left_in_game":
        await _send_temp(
            chat_id,
            f"🚪 {mention(actor_uid, actor_name)} 主动退出，本局按出局处理。",
            thread_id,
        )
        return msg, False

    return msg, False


async def _after_round_continue(scope: str, room_tip: str = "") -> None:
    room = await svc.get_room(scope)
    if not room:
        return
    chat_id, thread_id = _parse_scope(scope)
    if room.get("state") == "speaking" and room.get("current_speaker_uid"):
        await _set_speaker_deadline(scope)
        await _announce_current_speaker(scope, tip=room_tip)
        return
    elif room.get("state") == "voting":
        await _announce_voting_with_summary(scope, chat_id, thread_id, tip=room_tip)
        return
    await _render_panel(scope, chat_id, thread_id)


def _extract_uc_say_content(raw_text: str) -> str:
    text = (raw_text or "").strip()
    if text.startswith("/uc_say"):
        parts = text.split(maxsplit=1)
        return parts[1].strip() if len(parts) > 1 else ""
    return text


async def _ai_speech_violates(words: list[str], content: str) -> bool:
    """Return True if the speech directly reveals any of the game words."""
    if not _ai_headers or not words:
        return False
    words_str = "、".join(words)
    prompt = (
        "你是卧底游戏的裁判。\n"
        "游戏规则：玩家需要用隐晦的方式描述自己的词语，不能直接说出。\n"
        f"本局禁止直接说出的词语有：{words_str}\n"
        f"玩家发言内容：{content}\n\n"
        "请判断该发言是否违规。违规的定义：直接说出了上述词语中的任意一个（包括同义词、谐音字、拆字等明显规避手段）。\n"
        "隐晦的类比、描述特征、举例等均不算违规。\n"
        "只回答 YES（违规）或 NO（不违规），不要解释。"
    )
    try:
        import asyncio
        loop = asyncio.get_event_loop()
        def _call():
            with httpx.Client(timeout=30) as client:
                resp = client.post(
                    f"{OPENAI_BASE_URL}/chat/completions",
                    headers=_ai_headers,
                    json={"model": "grok-4.1-fast", "max_tokens": 5, "stream": False, "messages": [{"role": "user", "content": prompt}]},
                )
                return resp.json()
        data = await loop.run_in_executor(None, _call)
        answer = (data["choices"][0]["message"]["content"] or "").strip().upper()
        return answer.startswith("YES")
    except Exception:
        return False


async def _ai_best_worst_speech(logs: list[tuple[str, str]], player_names: dict[str, str]) -> tuple[str, str]:
    """Return (best_uid, worst_uid) based on AI evaluation. Empty string if unavailable."""
    if not _ai_headers or len(logs) < 2:
        return "", ""
    lines = []
    for i, (uid, content) in enumerate(logs, start=1):
        name = player_names.get(uid, uid)
        lines.append(f"{i}. {name}：{content}")
    speeches_text = "\n".join(lines)
    prompt = (
        "你是卧底游戏的点评裁判。\n"
        "以下是本局所有玩家的发言（格式：序号. 玩家名：发言内容）：\n"
        f"{speeches_text}\n\n"
        "请从中选出：\n"
        "1. 最佳发言：最隐晦、最有迷惑性、最符合游戏精神的发言\n"
        "2. 最差发言：最直白、最没有迷惑性或最容易暴露身份的发言\n"
        "只回答两行，格式严格如下（用序号）：\n"
        "最佳:序号\n"
        "最差:序号"
    )
    try:
        loop = asyncio.get_event_loop()
        def _call():
            with httpx.Client(timeout=30) as client:
                resp = client.post(
                    f"{OPENAI_BASE_URL}/chat/completions",
                    headers=_ai_headers,
                    json={"model": "grok-4.1-fast", "max_tokens": 20, "stream": False, "messages": [{"role": "user", "content": prompt}]},
                )
                return resp.json()
        data = await loop.run_in_executor(None, _call)
        answer = (data["choices"][0]["message"]["content"] or "").strip()
        best_uid, worst_uid = "", ""
        for line in answer.splitlines():
            line = line.strip()
            if line.startswith("最佳:") or line.startswith("最佳："):
                idx = int(line.split(":")[-1].strip()) - 1
                if 0 <= idx < len(logs):
                    best_uid = logs[idx][0]
            elif line.startswith("最差:") or line.startswith("最差："):
                idx = int(line.split(":")[-1].strip()) - 1
                if 0 <= idx < len(logs):
                    worst_uid = logs[idx][0]
        return best_uid, worst_uid
    except Exception:
        return "", ""


async def _send_ai_review(
    scope: str,
    chat_id: int,
    thread_id: int | None,
    logs: list[tuple[str, str]] | None = None,
    player_names: dict[str, str] | None = None,
) -> None:
    """Evaluate speeches with AI and send a separate review message."""
    logs = logs if logs is not None else await svc.list_speech_logs(scope)
    if not logs:
        return
    if player_names is None:
        players = await svc.list_players(scope)
        player_names = {p.uid: p.name for p in players}
    best_uid, worst_uid = await _ai_best_worst_speech(logs, player_names)
    if not best_uid and not worst_uid:
        return
    lines = []
    if best_uid:
        best_name = player_names.get(best_uid, best_uid)
        best_content = next((c for u, c in logs if u == best_uid), "")
        lines.append(f"🏆 最佳发言：{mention(int(best_uid), best_name)}\n「{safe_html(best_content)}」")
    if worst_uid:
        worst_name = player_names.get(worst_uid, worst_uid)
        worst_content = next((c for u, c in logs if u == worst_uid), "")
        lines.append(f"💩 最差发言：{mention(int(worst_uid), worst_name)}\n「{safe_html(worst_content)}」")
    review_text = "\n".join(lines)
    await _send_temp(chat_id, f"🎙 <b>AI 点评</b>\n{review_text}", thread_id, delay=60)


async def _finish_game(
    scope: str,
    chat_id: int,
    thread_id: int | None,
    summary_text: str,
) -> None:
    review_logs, review_names = await _snapshot_review_context(scope)
    await _send_temp(chat_id, summary_text, thread_id, delay=60)
    asyncio.create_task(_send_ai_review(scope, chat_id, thread_id, review_logs, review_names))
    await _finalize_scope(scope, chat_id, thread_id)


async def _handle_speech_submit(scope: str, uid: int, name: str, content: str, reply_to: types.Message) -> None:
    if not content.strip():
        await reply_to.reply("❌ 发言内容不能为空")
        return

    # Step 1: validate it's this player's turn (no state change)
    try:
        await svc.validate_speech(scope, uid)
    except ValueError as e:
        await reply_to.reply(f"❌ {safe_html(str(e))}")
        return

    # Step 2: delete user message and prompt message immediately
    await _clear_speaker_prompts(scope, reply_to.chat.id)

    try:
        await reply_to.delete()
    except Exception:
        pass

    # Step 3: AI violation check
    players = await svc.list_players(scope)
    words = list({p.word for p in players if p.word})
    if await _ai_speech_violates(words, content):
        await _send_temp(
            reply_to.chat.id,
            f"⚠️ {mention(uid, name)} 发言疑似直接说出游戏词，已删除。\n请重新发言。",
            reply_to.message_thread_id,
        )
        # Re-send the speaker prompt so player can try again
        await _announce_current_speaker(scope)
        return

    # Step 4: advance game state and log
    try:
        to_voting, next_uid, next_name, alive_count = await svc.submit_speech(scope, uid)
    except ValueError as e:
        await _send_temp(reply_to.chat.id, f"❌ {safe_html(str(e))}", reply_to.message_thread_id)
        return

    await svc.append_speech_log(scope, uid, content)

    chat_id_del, _ = _parse_scope(scope)
    await _clear_vote_messages(scope, chat_id_del)

    if to_voting:
        await _after_round_continue(scope, room_tip=f"🗳 全员发言结束（存活 {alive_count} 人）")
        return

    await _after_round_continue(scope, room_tip=f"➡️ 下一位：{mention(int(next_uid), next_name)}")


async def _try_start(scope: str, actor_uid: int) -> tuple[bool, str]:
    room = await svc.get_room(scope)
    if not room:
        return False, "❌ 当前没有房间，请先创建"
    if room.get("host_uid") != str(actor_uid):
        return False, "❌ 只有房主可以开局"

    try:
        failed_dm, _, _, total_pot, civ_cnt, uc_cnt, wb_cnt = await svc.start_game(scope)
    except ValueError as e:
        return False, f"❌ {safe_html(str(e))}"

    if failed_dm:
        failed_list = "\n".join([f"• {uid}" for uid in failed_dm])
        return False, (
            "❌ 开局失败：以下玩家无法私聊收词。\n"
            "请先私聊机器人发送 /start 后重试。\n"
            f"{safe_html(failed_list)}"
        )

    await _set_speaker_deadline(scope)
    role_line = f"平民 {civ_cnt} 人 | 卧底 {uc_cnt} 人"
    if wb_cnt:
        role_line += f" | 白板 {wb_cnt} 人"
    return True, (
        "🎬 <b>游戏开始</b>\n"
        "身份与词语已私聊发放，请勿泄露原词。\n"
        f"{role_line}\n"
        f"本局总奖池：<b>{total_pot}</b>（每人 {BET_PER_PLAYER}）\n\n"
        "<b>第 1 轮 - 发言阶段</b>"
    )


@router.message(Command("start"))
async def start_private(message: types.Message):
    await message.reply(
        "✅ 已连接 Undercover Bot。\n"
        "在群里发送 /uc_help 呼出面板，后续可直接点按钮操作。"
    )


@router.message(F.chat.type == "private", F.text, ~F.text.startswith("/"))
async def blank_guess_private(message: types.Message):
    scope, player = await _find_pending_blank_scope(message.from_user.id)
    if not scope or not player:
        return

    try:
        result = await svc.submit_blank_guess(scope, message.from_user.id, message.text or "")
    except ValueError as e:
        await message.reply(f"❌ {safe_html(str(e))}")
        return

    chat_id, thread_id = _parse_scope(scope)
    actor = mention(message.from_user.id, message.from_user.full_name)
    if result.get("matched"):
        await message.reply("✅ 你猜对了，最终结算时会与你方获胜阵营一起分奖池。")
        await _send_temp(chat_id, f"🎯 {actor} 的白板猜词结果：<b>猜对了</b>。", thread_id)
        return

    await message.reply("❌ 你猜错了，本局白板奖励资格失效。")
    await _send_temp(chat_id, f"❌ {actor} 的白板猜词结果：<b>猜错了</b>。", thread_id)


@router.message(Command("uc_help"))
async def uc_help(message: types.Message):
    sent = await message.reply(_help_text())
    asyncio.create_task(_delete_message_later(message.chat.id, sent.message_id, 60))
    asyncio.create_task(_delete_message_later(message.chat.id, message.message_id, 60))


@router.message(Command("uc_new"))
async def uc_new(message: types.Message):
    scope = await _ctx_scope(message)
    await _maybe_hint_private_start(
        message.chat.id,
        message.message_thread_id,
        message.from_user.id,
        message.from_user.full_name,
    )
    try:
        await svc.create_room(scope, message.from_user.id, message.from_user.full_name)
        await redis.set(_trigger_key(scope), str(message.message_id))
        tip = f"✅ 房间已创建，房主：{mention(message.from_user.id, message.from_user.full_name)}"
    except ValueError as e:
        tip = f"❌ {safe_html(str(e))}"
    await _render_from_message(message, tip=tip, force_new=True)


@router.message(Command("uc_join"))
async def uc_join(message: types.Message):
    scope = await _ctx_scope(message)
    await _maybe_hint_private_start(
        message.chat.id,
        message.message_thread_id,
        message.from_user.id,
        message.from_user.full_name,
    )
    try:
        await svc.join_room(scope, message.from_user.id, message.from_user.full_name)
        ids = await svc.list_player_ids(scope)
        tip = f"✅ {mention(message.from_user.id, message.from_user.full_name)} 加入成功，当前人数：<b>{len(ids)}</b>"
    except ValueError as e:
        tip = f"❌ {safe_html(str(e))}"
    await _render_from_message(message, tip=tip, force_new=True)


@router.message(Command("uc_leave"))
async def uc_leave(message: types.Message):
    scope = await _ctx_scope(message)
    room = await svc.get_room(scope)
    if not room:
        await _render_from_message(message, tip="❌ 当前没有房间", force_new=True)
        return

    if room.get("state") == "lobby":
        try:
            result = await svc.leave_room(scope, message.from_user.id)
            tip, ended = await _process_leave_result(
                scope, message.from_user.id, message.from_user.full_name, result
            )
        except ValueError as e:
            tip = f"❌ {safe_html(str(e))}"
            ended = False
        if ended:
            await svc.destroy_room(scope)
            await redis.delete(_panel_key(scope))
            await redis.delete(_vote_result_key(scope))
        await _render_from_message(message, tip=f"ℹ️ {safe_html(tip)}", force_new=True)
        return

    await redis.setex(svc.leave_confirm_key(scope, message.from_user.id), 60, "1")
    await message.reply(
        "⚠️ 你正在对局中，确认退出后会按出局处理；若你是卧底，系统会直接结束本局并结算奖池。",
        reply_markup=_leave_confirm_kb(),
    )


@router.message(Command("uc_start"))
async def uc_start(message: types.Message):
    scope = await _ctx_scope(message)
    ok, tip = await _try_start(scope, message.from_user.id)
    if ok:
        panel_id = await redis.get(_panel_key(scope))
        await redis.delete(_panel_key(scope))
        await redis.delete(_vote_result_key(scope))
        if panel_id:
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=int(panel_id))
            except Exception:
                pass
        await _announce_current_speaker(scope, tip=tip)
    else:
        await message.reply(tip)


@router.message(Command("uc_say"))
async def uc_say(message: types.Message):
    scope = await _ctx_scope(message)
    content = _extract_uc_say_content(message.text or "")
    if not content:
        await message.reply("❌ 用法：/uc_say 你的描述")
        return
    await _handle_speech_submit(scope, message.from_user.id, message.from_user.full_name, content, message)


@router.message(F.text, ~F.text.startswith("/"), F.reply_to_message.as_("reply_msg"))
async def uc_reply_say(message: types.Message, reply_msg: types.Message):
    scope = await _ctx_scope(message)
    room = await svc.get_room(scope)
    if not room or room.get("state") != "speaking":
        return
    if room.get("current_speaker_uid") != str(message.from_user.id):
        return
    if not reply_msg.from_user or not reply_msg.from_user.is_bot:
        return

    hint_text = (reply_msg.text or reply_msg.caption or "")
    if ("发言轮到你了" not in hint_text) and ("请尽快发言" not in hint_text):
        return

    content = _extract_uc_say_content(message.text)
    if not content:
        await message.reply("❌ 回复发言内容不能为空")
        return
    await _handle_speech_submit(scope, message.from_user.id, message.from_user.full_name, content, message)


@router.message(Command("uc_status"))
async def uc_status(message: types.Message):
    await _render_from_message(message, force_new=True)


@router.message(Command("uc_bal"))
async def uc_bal(message: types.Message):
    bal = await get_or_init_balance(message.from_user.id)
    await message.reply(
        f"💰 {mention(message.from_user.id, message.from_user.full_name)} 当前积分：<b>{bal:.2f}</b>"
    )


@router.message(Command("uc_end"))
async def uc_end(message: types.Message):
    scope = await _ctx_scope(message)
    room = await svc.get_room(scope)
    if not room:
        await _render_from_message(message, tip="❌ 当前没有房间")
        return
    if room.get("host_uid") != str(message.from_user.id):
        await _render_from_message(message, tip="❌ 只有房主可解散")
        return

    await svc.refund_all_stakes(scope)
    await svc.refund_all_stakes(scope)
    await svc.destroy_room(scope)
    await redis.delete(_panel_key(scope))
    await redis.delete(_vote_result_key(scope))
    await _render_from_message(message, tip="🧹 房间已解散，押注已原路退回。", force_new=True)


@router.message(Command("uc_force_stop"))
async def uc_force_stop(message: types.Message):
    if message.from_user.id not in SUPER_ADMIN_IDS:
        await message.reply("❌ 仅超管可执行")
        return

    active_scope = await redis.get(svc.active_scope_key())
    scopes = set(await svc.open_rooms())
    if active_scope:
        scopes.add(active_scope)

    if not scopes:
        await redis.delete(svc.active_scope_key())
        await message.reply("ℹ️ 当前没有进行中的对局")
        return

    stopped_count = 0
    for scope in scopes:
        room = await svc.get_room(scope)
        if room:
            try:
                await svc.refund_all_stakes(scope)
            except Exception:
                logger.exception("uc_force_stop refund failed: %s", scope)
        try:
            await svc.destroy_room(scope)
            stopped_count += 1
        except Exception:
            logger.exception("uc_force_stop destroy failed: %s", scope)
            continue

        try:
            chat_id, thread_id = _parse_scope(scope)
            await bot.send_message(
                chat_id,
                f"🛑 对局已被超管强制终止：{mention(message.from_user.id, message.from_user.full_name)}",
                message_thread_id=thread_id,
            )
            await _render_panel(scope, chat_id, thread_id)
        except Exception:
            logger.exception("uc_force_stop notify failed: %s", scope)

    await redis.delete(svc.active_scope_key())
    await message.reply(f"✅ 已强制终止 {stopped_count} 个对局")


# ───────────────────── 停机维护 / 停机补偿 ─────────────────────

async def _compensation_cleanup(chat_id: int, msg_id: int, delay: float, redis_key: str):
    """延迟后清理停机补偿置顶：仅当 key 仍指向本消息时才解钉+删除+清 key"""
    await asyncio.sleep(delay)
    stored = await redis.get(redis_key)
    if stored and int(stored.split(":")[0]) == msg_id:
        try:
            await bot.unpin_chat_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass
        try:
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass
        await redis.delete(redis_key)


@router.message(Command("uc_maintain"))
async def uc_maintain(message: types.Message):
    if message.from_user.id not in SUPER_ADMIN_IDS:
        await message.reply("❌ 仅超管可执行")
        return

    chat_id = message.chat.id
    thread_id = message.message_thread_id

    # 1. 清理所有进行中的对局（退还押注）
    active_scope = await redis.get(svc.active_scope_key())
    scopes = set(await svc.open_rooms())
    if active_scope:
        scopes.add(active_scope)
    destroyed = 0
    for scope in scopes:
        room = await svc.get_room(scope)
        if room:
            try:
                await svc.refund_all_stakes(scope)
            except Exception:
                pass
        try:
            await svc.destroy_room(scope)
            destroyed += 1
        except Exception:
            pass
    await redis.delete(svc.active_scope_key())

    # 2. 解除旧的置顶公告（维护 / 补偿）
    for old_key in [f"uc:compensation_pin:{chat_id}", f"uc:maintenance_pin:{chat_id}"]:
        old_id = await redis.get(old_key)
        if old_id:
            old_msg = int(old_id.split(":")[0])
            try:
                await bot.unpin_chat_message(chat_id=chat_id, message_id=old_msg)
            except Exception:
                pass
            try:
                await bot.delete_message(chat_id=chat_id, message_id=old_msg)
            except Exception:
                pass
            await redis.delete(old_key)

    # 3. 设置维护标记
    await redis.set(f"uc:maintenance:{chat_id}", "1")

    # 4. 发送维护公告并置顶
    body = (
        f"🔧 <b>【停机维护公告】</b>\n\n"
        f"系统即将进行维护，暂时停止服务。\n"
        f"• 已清理 <b>{destroyed}</b> 个进行中的对局（押注已退还）\n\n"
        f"维护完成后将置顶「停机补偿」公告并发放补偿积分，感谢耐心等待！"
    )
    try:
        announce = await bot.send_message(chat_id, body, message_thread_id=thread_id)
        await bot.pin_chat_message(chat_id=chat_id, message_id=announce.message_id, disable_notification=False)
        await redis.set(f"uc:maintenance_pin:{chat_id}", str(announce.message_id))
    except Exception as e:
        logger.warning("[uc_maintain] 公告发送/置顶失败: %s", e)

    try:
        await message.delete()
    except Exception:
        pass


@router.message(Command("uc_compensate"))
async def uc_compensate(message: types.Message):
    if message.from_user.id not in SUPER_ADMIN_IDS:
        await message.reply("❌ 仅超管可执行")
        return

    chat_id = message.chat.id
    thread_id = message.message_thread_id

    # 解析自定义更新内容（命令后的文字）
    text = (message.text or "").strip()
    parts = text.split(None, 1)
    extra_desc = parts[1].strip() if len(parts) > 1 else ""

    # 1. 清除维护标记
    await redis.delete(f"uc:maintenance:{chat_id}")

    # 2. 扫描所有有积分记录的用户并发放补偿
    uids: list[str] = []
    cursor = 0
    while True:
        cursor, keys = await points_redis.scan(cursor, match="user_balance:*", count=200)
        for k in keys:
            uid = k.decode() if isinstance(k, bytes) else k
            uids.append(uid.removeprefix("user_balance:"))
        if cursor == 0:
            break

    for uid in uids:
        try:
            await update_balance(uid, COMPENSATION_AMOUNT)
        except Exception:
            logger.exception("[uc_compensate] 补偿发放失败 uid=%s", uid)

    # 3. 解除旧的维护/补偿置顶
    for old_key in [f"uc:maintenance_pin:{chat_id}", f"uc:compensation_pin:{chat_id}"]:
        old_id = await redis.get(old_key)
        if old_id:
            old_msg = int(old_id.split(":")[0])
            try:
                await bot.unpin_chat_message(chat_id=chat_id, message_id=old_msg)
            except Exception:
                pass
            try:
                await bot.delete_message(chat_id=chat_id, message_id=old_msg)
            except Exception:
                pass
            await redis.delete(old_key)

    # 4. 发送补偿公告并置顶
    desc = (extra_desc or "本次为稳定性维护与体验优化。").strip()
    body = (
        f"🔧 <b>【停机补偿公告】</b>\n\n"
        f"✅ 维护已完成，服务恢复正常。\n"
        f"🎁 已向全体 <b>{len(uids)}</b> 名玩家发放补偿：\n"
        f"• 🪙 积分 <b>+{COMPENSATION_AMOUNT}</b>\n\n"
        f"📋 <b>更新内容</b>\n"
        f"• {desc}\n\n"
        f"感谢耐心等待，继续游戏！"
    )
    try:
        announce = await bot.send_message(chat_id, body, message_thread_id=thread_id)
        await bot.pin_chat_message(chat_id=chat_id, message_id=announce.message_id, disable_notification=False)
        await redis.set(f"uc:compensation_pin:{chat_id}", f"{announce.message_id}:{int(time.time())}")
        asyncio.create_task(_compensation_cleanup(chat_id, announce.message_id, 1800, f"uc:compensation_pin:{chat_id}"))
    except Exception as e:
        logger.warning("[uc_compensate] 公告发送/置顶失败: %s", e)

    try:
        await message.delete()
    except Exception:
        pass


@router.callback_query(F.data == "uc_noop")
async def uc_noop(cb: types.CallbackQuery):
    await cb.answer()


@router.callback_query(F.data.startswith("uc_panel:"))
async def uc_panel_action(cb: types.CallbackQuery):
    msg = cb.message
    if not msg:
        await cb.answer("消息不可用", show_alert=True)
        return

    action = cb.data.split(":", 1)[1]
    scope = scope_id(msg.chat.id, msg.message_thread_id)
    uid = cb.from_user.id
    name = cb.from_user.full_name

    if action == "refresh":
        await cb.answer("已刷新")
        await _render_from_callback(cb)
        return

    if action == "new":
        await _maybe_hint_private_start(msg.chat.id, msg.message_thread_id, uid, name)
        try:
            await svc.create_room(scope, uid, name)
            await cb.answer("创建成功")
            tip = f"✅ 房间已创建，房主：{mention(uid, name)}"
        except ValueError as e:
            await cb.answer(str(e), show_alert=True)
            tip = f"❌ {safe_html(str(e))}"
        await _render_from_callback(cb, tip=tip)
        return

    if action == "join":
        await _maybe_hint_private_start(msg.chat.id, msg.message_thread_id, uid, name)
        try:
            await svc.join_room(scope, uid, name)
            ids = await svc.list_player_ids(scope)
            await cb.answer("加入成功")
            tip = f"✅ {mention(uid, name)} 加入成功，当前人数：<b>{len(ids)}</b>"
        except ValueError as e:
            await cb.answer(str(e), show_alert=True)
            tip = f"❌ {safe_html(str(e))}"
        await _render_from_callback(cb, tip=tip)
        return

    if action == "leave":
        room = await svc.get_room(scope)
        if not room:
            await cb.answer("当前没有房间", show_alert=True)
            await _render_from_callback(cb, tip="❌ 当前没有房间")
            return

        if room.get("state") == "lobby":
            try:
                result = await svc.leave_room(scope, uid)
                tip, ended = await _process_leave_result(scope, uid, name, result)
                await cb.answer("已处理")
            except ValueError as e:
                await cb.answer(str(e), show_alert=True)
                tip = f"❌ {safe_html(str(e))}"
                ended = False
            if ended:
                await svc.destroy_room(scope)
                await redis.delete(_panel_key(scope))
                await redis.delete(_vote_result_key(scope))
            await _render_from_callback(cb, tip=f"ℹ️ {safe_html(tip)}")
            return

        await redis.setex(svc.leave_confirm_key(scope, uid), 60, "1")
        await cb.answer("请二次确认", show_alert=True)
        await msg.answer(
            "⚠️ 你正在对局中，确认退出后会按出局处理；若你是卧底，系统会直接结束本局并结算奖池。",
            reply_markup=_leave_confirm_kb(),
        )
        return

    if action == "leave_confirm":
        token = await redis.get(svc.leave_confirm_key(scope, uid))
        if token != "1":
            await cb.answer("确认已过期，请重新点击退出", show_alert=True)
            return
        await redis.delete(svc.leave_confirm_key(scope, uid))
        try:
            result = await svc.leave_room(scope, uid)
            tip, ended = await _process_leave_result(scope, uid, name, result)
            await cb.answer("已退出")
        except ValueError as e:
            await cb.answer(str(e), show_alert=True)
            tip = f"❌ {safe_html(str(e))}"
            ended = False
        if ended:
            await svc.destroy_room(scope)
            await redis.delete(_panel_key(scope))
            await redis.delete(_vote_result_key(scope))
        await _render_from_callback(cb, tip=f"ℹ️ {safe_html(tip)}")
        return

    if action == "leave_cancel":
        await redis.delete(svc.leave_confirm_key(scope, uid))
        await cb.answer("已取消")
        return

    if action == "status":
        await cb.answer("已刷新状态")
        await _render_from_callback(cb)
        return

    if action == "bal":
        bal = await get_or_init_balance(uid)
        await cb.answer(f"当前积分：{bal:.2f}", show_alert=True)
        return

    if action == "say":
        room = await svc.get_room(scope)
        if not room or room.get("state") != "speaking":
            await cb.answer("当前不是发言阶段", show_alert=True)
            return
        current_uid = room.get("current_speaker_uid", "")
        current_p = await svc.get_player(scope, current_uid) if current_uid else None
        if str(uid) != current_uid:
            cname = current_p.get("name", current_uid) if current_p else "未知"
            await cb.answer(f"当前轮到 {cname} 发言", show_alert=True)
            return

        await cb.answer("请发送发言")
        await msg.answer(
            f"🗣 {mention(uid, name)} 现在轮到你，请发送：\n"
            "<code>/uc_say 你的描述</code>"
        )
        return

    if action == "start":
        ok, tip = await _try_start(scope, uid)
        if ok:
            await cb.answer("游戏开始")
            panel_id = await redis.get(_panel_key(scope))
            await redis.delete(_panel_key(scope))
            await redis.delete(_vote_result_key(scope))
            if panel_id:
                try:
                    await bot.delete_message(chat_id=msg.chat.id, message_id=int(panel_id))
                except Exception:
                    pass
            await _announce_current_speaker(scope, tip=tip)
        else:
            await cb.answer("无法开局", show_alert=True)
            await msg.answer(tip)
            await _render_from_callback(cb)
        return

    if action == "end":
        room = await svc.get_room(scope)
        if not room:
            await cb.answer("当前没有房间", show_alert=True)
            await _render_from_callback(cb)
            return
        if room.get("host_uid") != str(uid):
            await cb.answer("只有房主可解散", show_alert=True)
            return
        await svc.refund_all_stakes(scope)
        await svc.destroy_room(scope)
        await cb.answer("房间已解散")
        await _render_from_callback(cb, tip="🧹 房间已解散，押注已原路退回。")
        return

    await cb.answer("未知操作", show_alert=True)


@router.callback_query(F.data.startswith("uc_vote:"))
async def uc_vote_cb(cb: types.CallbackQuery):
    target_uid = int(cb.data.split(":", 1)[1])
    msg = cb.message
    if not msg:
        await cb.answer("消息不可用", show_alert=True)
        return

    scope = scope_id(msg.chat.id, msg.message_thread_id)
    try:
        voted, alive = await svc.submit_vote(scope, cb.from_user.id, target_uid)
    except ValueError as e:
        await cb.answer(str(e), show_alert=True)
        return

    await cb.answer("投票成功")
    await redis.set(_vote_message_key(scope), str(cb.message.message_id))
    try:
        await cb.message.edit_text(await _voting_message_text(scope), reply_markup=await _voting_kb(scope))
    except Exception:
        pass

    if voted < alive:
        return
    chat_id, thread_id = _parse_scope(scope)
    await _finalize_voting(scope, chat_id, thread_id)


async def speech_watchdog_loop() -> None:
    while True:
        try:
            now = int(time.time())
            for scope in await svc.open_rooms():
                room = await svc.get_room(scope)
                if not room:
                    continue

                state = room.get("state")
                if state == "lobby":
                    deadline = int(float(room.get("auto_start_deadline", "0") or 0))
                    if deadline > 0 and deadline <= now:
                        chat_id, thread_id = _parse_scope(scope)
                        players = await svc.list_players(scope)
                        if len(players) < MIN_PLAYERS:
                            await svc.refund_all_stakes(scope)
                            await svc.destroy_room(scope)
                            panel_id = await redis.get(_panel_key(scope))
                            trigger_id = await redis.get(_trigger_key(scope))
                            await redis.delete(_panel_key(scope))
                            await redis.delete(_vote_result_key(scope))
                            await redis.delete(_trigger_key(scope))
                            for msg_id in filter(None, [panel_id, trigger_id]):
                                try:
                                    await bot.delete_message(chat_id=chat_id, message_id=int(msg_id))
                                except Exception:
                                    pass
                            notice = await bot.send_message(
                                chat_id,
                                (
                                    f"⏳ {AUTO_START_IDLE_SECONDS} 秒内无人继续加入，当前人数不足，无法自动开局。"
                                    "\n本局已取消，如有押注会自动退回。"
                                ),
                                message_thread_id=thread_id,
                            )
                            asyncio.create_task(_delete_message_later(chat_id, notice.message_id, 15))
                            continue

                        host_uid = int(room.get("host_uid", "0") or 0)
                        ok, tip = await _try_start(scope, host_uid)
                        if ok:
                            panel_id = await redis.get(_panel_key(scope))
                            await redis.delete(_panel_key(scope))
                            await redis.delete(_vote_result_key(scope))
                            if panel_id:
                                try:
                                    await bot.delete_message(chat_id=chat_id, message_id=int(panel_id))
                                except Exception:
                                    pass
                            await _announce_current_speaker(scope, tip=tip)
                        else:
                            await _send_temp(chat_id, tip, thread_id)
                            await svc.refund_all_stakes(scope)
                            await svc.destroy_room(scope)
                            await redis.delete(_panel_key(scope))
                            await redis.delete(_vote_result_key(scope))
                    continue

                for pending in await svc.pending_blank_guess_players(scope):
                    if pending.blank_guess_deadline > 0 and pending.blank_guess_deadline <= now:
                        await redis.hset(
                            svc.player_key(scope, pending.uid),
                            mapping={"blank_guess_state": "failed", "blank_guess_deadline": "0"},
                        )
                        chat_id, thread_id = _parse_scope(scope)
                        await _send_temp(
                            chat_id,
                            f"⏰ {mention(int(pending.uid), pending.name)} 白板猜词超时，判定失败。",
                            thread_id,
                        )

                if state == "voting":
                    deadline = int(float(room.get("voting_deadline", "0") or 0))
                    if deadline > 0 and deadline <= now:
                        chat_id, thread_id = _parse_scope(scope)
                        filled, _ = await svc.auto_fill_missing_votes(scope)
                        tip = "⏰ 投票时间到。"
                        if filled > 0:
                            tip += " 未投票玩家已默认投给自己。"
                        await _finalize_voting(scope, chat_id, thread_id, result_tip=tip)
                    continue

                if state != "speaking":
                    continue

                current_uid = room.get("current_speaker_uid", "")
                if not current_uid:
                    nxt = await svc.advance_speaker(scope)
                    if nxt:
                        await _set_speaker_deadline(scope)
                        await _announce_current_speaker(scope, tip="➡️ 自动推进到下一位")
                    else:
                        await svc.begin_voting(scope)
                        chat_id, thread_id = _parse_scope(scope)
                        await _announce_voting_with_summary(scope, chat_id, thread_id, tip="🗳 自动进入投票阶段（无人可继续发言）")
                    continue

                deadline = int(float(room.get("current_speaker_deadline", "0") or 0))
                reminded = int(room.get("current_speaker_reminded", "0") or 0)

                if deadline <= 0:
                    await _set_speaker_deadline(scope)
                    await _announce_current_speaker(scope)
                    continue

                remain = deadline - now
                if remain <= 0:
                    result = await svc.timeout_current_speaker(scope)
                    if not result.get("ok"):
                        continue
                    chat_id, thread_id = _parse_scope(scope)
                    eliminated_uid = result.get("eliminated_uid", "")
                    elim_name = ""
                    if eliminated_uid:
                        ep = await svc.get_player(scope, eliminated_uid)
                        elim_name = ep.get("name", eliminated_uid) if ep else eliminated_uid

                    if result.get("type") == "ended":
                        winner = result.get("winner")
                        winner_cn = "平民" if winner == "civilian" else ("白板" if winner == "whiteboard" else "卧底")
                        civ_word, uc_word, uc_name, wb_info = await svc.reveal_words(scope)
                        await _finish_game(
                            scope,
                            chat_id,
                            thread_id,
                            (
                                f"⏰ {safe_html(elim_name)} 发言超时，判负出局。\n"
                                f"🏁 游戏结束，胜利阵营：<b>{winner_cn}</b>\n"
                                f"平民词：<b>{safe_html(civ_word)}</b>\n"
                                f"卧底词：<b>{safe_html(uc_word)}</b>\n"
                                f"卧底有：{uc_name}"
                                + _whiteboard_text(wb_info)
                                + f"{_payout_text(result.get('payout', {}))}"
                            ),
                        )
                        continue

                    if result.get("type") == "voting":
                        await _announce_voting_with_summary(
                            scope,
                            chat_id,
                            thread_id,
                            tip=f"⏰ {safe_html(elim_name)} 发言超时，判负出局。\n🗳 自动进入投票阶段。",
                        )
                        continue

                    if result.get("type") == "next_speaker":
                        nxt = result.get("next_speaker", {})
                        await _send_temp(
                            chat_id,
                            (
                                f"⏰ {safe_html(elim_name)} 发言超时，判负出局。\n"
                                f"➡️ 下一位：{mention(int(nxt.get('uid')), nxt.get('name', nxt.get('uid', '')))}"
                            ),
                            thread_id,
                        )
                        await _set_speaker_deadline(scope)
                        await _announce_current_speaker(scope)
                        continue

                if remain <= SPEAK_REMIND_BEFORE_SECONDS and reminded == 0:
                    p = await svc.get_player(scope, current_uid)
                    if p:
                        chat_id, thread_id = _parse_scope(scope)
                        sent = await bot.send_message(
                            chat_id,
                            (
                                f"⏳ {mention(int(current_uid), p.get('name', current_uid))} "
                                f"请尽快发言，剩余 <b>{max(remain, 0)}</b> 秒。"
                            ),
                            message_thread_id=thread_id,
                        )
                        await _track_speaker_prompt(scope, sent.message_id)
                        asyncio.create_task(_delete_message_later(chat_id, sent.message_id, max(remain, 15)))
                        await redis.hset(svc.room_key(scope), "current_speaker_reminded", "1")
        except Exception as e:
            logger.exception("watchdog error: %s", e)

        await asyncio.sleep(WATCHDOG_INTERVAL_SECONDS)
