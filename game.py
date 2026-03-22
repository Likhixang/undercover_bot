import random
import time
from dataclasses import dataclass

from aiogram.exceptions import TelegramBadRequest

from balance import get_or_init_balance, update_balance
from config import AUTO_START_IDLE_SECONDS, BET_PER_PLAYER, MAX_PLAYERS, MIN_PLAYERS, VOTING_TIMEOUT_SECONDS, WORD_PAIRS
from core import bot, redis
from utils import mention


@dataclass
class Player:
    uid: str
    name: str
    alive: bool
    role: str
    word: str
    blank_bonus: bool = False
    blank_guess_state: str = ""
    blank_guess_deadline: int = 0


class UndercoverService:
    @staticmethod
    def room_key(scope: str) -> str:
        return f"uc:room:{scope}"

    @staticmethod
    def players_key(scope: str) -> str:
        return f"uc:room:{scope}:players"

    @staticmethod
    def player_key(scope: str, uid: str) -> str:
        return f"uc:room:{scope}:player:{uid}"

    @staticmethod
    def spoken_key(scope: str) -> str:
        return f"uc:room:{scope}:spoken"

    @staticmethod
    def votes_key(scope: str) -> str:
        return f"uc:room:{scope}:votes"

    @staticmethod
    def speech_log_key(scope: str) -> str:
        return f"uc:room:{scope}:speech_logs"

    @staticmethod
    def stake_key(scope: str) -> str:
        return f"uc:room:{scope}:stakes"

    @staticmethod
    def active_scope_key() -> str:
        return "uc:active_scope"

    @staticmethod
    def leave_confirm_key(scope: str, uid: int | str) -> str:
        return f"uc:leave_confirm:{scope}:{uid}"

    @staticmethod
    def transient_keys(scope: str) -> list[str]:
        return [
            f"uc:panel:{scope}",
            f"uc:trigger:{scope}",
            f"uc:speaker_prompt:{scope}",
            f"uc:speaker_prompts:{scope}",
            f"uc:vote_message:{scope}",
            f"uc:vote_result:{scope}",
            f"uc:vote_finalize_guard:{scope}",
        ]

    @staticmethod
    def role_plan(player_count: int) -> tuple[int, int]:
        if player_count <= 4:
            return 1, 0
        if player_count == 5:
            return 1, 1
        if player_count == 6:
            return 2, 1
        return 2, 2

    async def room_exists(self, scope: str) -> bool:
        return await redis.exists(self.room_key(scope)) == 1

    async def create_room(self, scope: str, host_uid: int, host_name: str) -> None:
        room_k = self.room_key(scope)
        if await redis.exists(room_k):
            raise ValueError("房间已存在")
        rooms = await self.open_rooms()
        if rooms and scope not in rooms:
            raise ValueError("当前已有对局房间，请先结束后再创建")
        active_scope = await redis.get(self.active_scope_key())
        if active_scope and active_scope != scope:
            raise ValueError("当前已有其他对局在进行，请等待其结束")
        if await self.user_in_any_room(host_uid):
            raise ValueError("你已在其他对局中，无法重复加入")
        now = int(time.time())
        async with redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                room_k,
                mapping={
                    "state": "lobby",
                    "host_uid": str(host_uid),
                    "host_name": host_name,
                    "round": "0",
                    "created_at": str(now),
                    "auto_start_deadline": str(now + AUTO_START_IDLE_SECONDS),
                    "speak_order": "",
                    "speak_index": "0",
                    "current_speaker_uid": "",
                    "current_speaker_deadline": "0",
                    "current_speaker_reminded": "0",
                    "voting_deadline": "0",
                },
            )
            pipe.rpush(self.players_key(scope), str(host_uid))
            pipe.hset(
                self.player_key(scope, str(host_uid)),
                mapping={
                    "uid": str(host_uid),
                    "name": host_name,
                    "alive": "1",
                    "role": "",
                    "word": "",
                    "blank_bonus": "0",
                    "blank_guess_state": "",
                    "blank_guess_deadline": "0",
                },
            )
            await pipe.execute()

    async def get_room(self, scope: str) -> dict[str, str]:
        return await redis.hgetall(self.room_key(scope))

    async def get_player(self, scope: str, uid: int | str) -> dict[str, str]:
        return await redis.hgetall(self.player_key(scope, str(uid)))

    async def list_player_ids(self, scope: str) -> list[str]:
        return await redis.lrange(self.players_key(scope), 0, -1)

    async def list_players(self, scope: str) -> list[Player]:
        uids = await self.list_player_ids(scope)
        data: list[Player] = []
        for uid in uids:
            p = await redis.hgetall(self.player_key(scope, uid))
            if not p:
                continue
            data.append(
                Player(
                    uid=uid,
                    name=p.get("name", uid),
                    alive=p.get("alive", "1") == "1",
                    role=p.get("role", ""),
                    word=p.get("word", ""),
                    blank_bonus=p.get("blank_bonus", "0") == "1",
                    blank_guess_state=p.get("blank_guess_state", ""),
                    blank_guess_deadline=int(float(p.get("blank_guess_deadline", "0") or 0)),
                )
            )
        return data

    async def join_room(self, scope: str, uid: int, name: str) -> None:
        room = await self.get_room(scope)
        if not room:
            raise ValueError("当前没有房间")
        active_scope = await redis.get(self.active_scope_key())
        if active_scope and active_scope != scope:
            raise ValueError("当前已有其他对局在进行，请勿跨局加入")
        if room.get("state") != "lobby":
            raise ValueError("游戏已开始，无法加入")
        ids = await self.list_player_ids(scope)
        if str(uid) in ids:
            raise ValueError("你已经在房间里")
        if await self.user_in_any_room(uid):
            raise ValueError("你已在其他对局中，无法重复加入")
        if len(ids) >= MAX_PLAYERS:
            raise ValueError(f"人数已满（最多 {MAX_PLAYERS} 人）")

        async with redis.pipeline(transaction=True) as pipe:
            pipe.rpush(self.players_key(scope), str(uid))
            pipe.hset(
                self.player_key(scope, str(uid)),
                mapping={
                    "uid": str(uid),
                    "name": name,
                    "alive": "1",
                    "role": "",
                    "word": "",
                    "blank_bonus": "0",
                    "blank_guess_state": "",
                    "blank_guess_deadline": "0",
                },
            )
            pipe.hset(self.room_key(scope), "auto_start_deadline", str(int(time.time()) + AUTO_START_IDLE_SECONDS))
            await pipe.execute()

    async def payout(self, winner: str, scope: str) -> dict:
        players = await self.list_players(scope)
        if winner == "whiteboard":
            winners = [p for p in players if p.role == "whiteboard" and p.alive]
        else:
            winners = [p for p in players if p.role == winner]
        for p in players:
            if p.role == "whiteboard" and p.blank_bonus and p not in winners:
                winners.append(p)
        return await self.payout_players(scope, winners)

    async def payout_players(self, scope: str, winners: list[Player]) -> dict:
        pot_raw = await redis.hvals(self.stake_key(scope))
        total_pot = int(sum(float(x or 0) for x in pot_raw))

        if not winners or total_pot <= 0:
            return {"winner_count": 0, "share": 0, "total_pot": total_pot}

        share = total_pot // len(winners)
        remainder = total_pot - share * len(winners)

        for idx, p in enumerate(winners):
            add = share + (1 if idx < remainder else 0)
            await update_balance(p.uid, add)

        return {
            "winner_count": len(winners),
            "share": share,
            "remainder": remainder,
            "total_pot": total_pot,
            "winner_uids": [p.uid for p in winners],
        }

    async def leave_room(self, scope: str, uid: int) -> dict:
        room = await self.get_room(scope)
        if not room:
            raise ValueError("当前没有房间")

        p = await self.get_player(scope, uid)
        if not p:
            raise ValueError("你不在房间内")

        state = room.get("state", "")
        if state == "lobby":
            async with redis.pipeline(transaction=True) as pipe:
                pipe.lrem(self.players_key(scope), 0, str(uid))
                pipe.delete(self.player_key(scope, str(uid)))
                await pipe.execute()
            left = await self.list_player_ids(scope)
            if not left:
                await self.destroy_room(scope)
                return {
                    "ok": True,
                    "type": "lobby_leave",
                    "message": "你已退出。房间无人，已自动解散。",
                    "notify": f"{p.get('name', str(uid))} 退出房间，房间无人，已自动解散。",
                }
            if room.get("host_uid") == str(uid):
                new_host = left[0]
                np = await self.get_player(scope, new_host)
                await redis.hset(
                    self.room_key(scope),
                    mapping={
                        "host_uid": new_host,
                        "host_name": np.get("name", new_host),
                        "auto_start_deadline": str(int(time.time()) + AUTO_START_IDLE_SECONDS),
                    },
                )
                return {
                    "ok": True,
                    "type": "lobby_leave",
                    "message": "你已退出。你是房主，已转移房主。",
                    "notify": f"{p.get('name', str(uid))} 退出房间，房主已转移给 {np.get('name', new_host)}。",
                }
            await redis.hset(self.room_key(scope), "auto_start_deadline", str(int(time.time()) + AUTO_START_IDLE_SECONDS))
            return {
                "ok": True,
                "type": "lobby_leave",
                "message": "你已退出房间。",
                "notify": f"{p.get('name', str(uid))} 已退出房间。",
            }

        if p.get("alive", "1") == "0":
            return {"ok": True, "type": "already_out", "message": "你已经出局。"}

        await redis.hset(self.player_key(scope, str(uid)), "alive", "0")
        await redis.srem(self.spoken_key(scope), str(uid))
        await redis.hdel(self.votes_key(scope), str(uid))

        alive = await self.alive_players(scope)

        winner = await self.check_winner(scope)
        if winner:
            await redis.hset(self.room_key(scope), "state", "ended")
            payout = await self.payout(winner, scope)
            return {
                "ok": True,
                "type": "ended_after_leave",
                "message": "你已中途退出，本局按出局处理，并已触发终局结算。",
                "leaver_uid": str(uid),
                "leaver_name": p.get("name", str(uid)),
                "winner": winner,
                "payout": payout,
            }

        # 发言阶段当前玩家退出：直接推进下一位或进入投票
        if room.get("state") == "speaking" and room.get("current_speaker_uid") == str(uid):
            nxt = await self.advance_speaker(scope)
            if not nxt:
                await self.begin_voting(scope)
                return {
                    "ok": True,
                    "type": "left_to_voting",
                    "message": "你已中途退出，本局按出局处理。",
                    "leaver_uid": str(uid),
                    "leaver_name": p.get("name", str(uid)),
                }
            return {
                "ok": True,
                "type": "left_next_speaker",
                "message": "你已中途退出，本局按出局处理。",
                "leaver_uid": str(uid),
                "leaver_name": p.get("name", str(uid)),
                "next_speaker": nxt,
            }

        return {
            "ok": True,
            "type": "left_in_game",
            "message": "你已中途退出，本局按出局处理。",
            "leaver_uid": str(uid),
            "leaver_name": p.get("name", str(uid)),
        }

    async def destroy_room(self, scope: str) -> None:
        ids = await self.list_player_ids(scope)
        keys = [
            self.room_key(scope),
            self.players_key(scope),
            self.spoken_key(scope),
            self.votes_key(scope),
            self.speech_log_key(scope),
            self.stake_key(scope),
        ]
        keys.extend(self.player_key(scope, uid) for uid in ids)
        keys.extend(self.transient_keys(scope))
        leave_confirm_keys = await redis.keys(f"uc:leave_confirm:{scope}:*")
        keys.extend(leave_confirm_keys)
        if keys:
            await redis.delete(*keys)
        active_scope = await redis.get(self.active_scope_key())
        if active_scope == scope:
            await redis.delete(self.active_scope_key())

    async def start_game(self, scope: str) -> tuple[list[int], list[str], str, int]:
        room = await self.get_room(scope)
        if not room:
            raise ValueError("当前没有房间")
        if room.get("state") != "lobby":
            raise ValueError("游戏已在进行中")
        active_scope = await redis.get(self.active_scope_key())
        if active_scope and active_scope != scope:
            raise ValueError("当前已有其他对局在进行，请等待其结束")

        players = await self.list_players(scope)
        if len(players) < MIN_PLAYERS:
            raise ValueError(f"人数不足，至少 {MIN_PLAYERS} 人")

        # 先检查余额
        insufficient: list[str] = []
        for p in players:
            bal = await get_or_init_balance(p.uid)
            if bal < BET_PER_PLAYER:
                insufficient.append(f"{p.name}({int(float(bal))})")
        if insufficient:
            raise ValueError(
                "以下玩家积分不足，无法开局（每人需100）: " + "、".join(insufficient)
            )

        # 扣注
        for p in players:
            await update_balance(p.uid, -BET_PER_PLAYER)
            await redis.hset(self.stake_key(scope), p.uid, str(BET_PER_PLAYER))

        recent_key = "uc:recent_word_pairs"
        avoid = len(WORD_PAIRS) // 2
        recent_raw = await redis.lrange(recent_key, 0, avoid - 1)
        recent_indices = {int(x) for x in recent_raw}
        candidates = [i for i in range(len(WORD_PAIRS)) if i not in recent_indices] or list(range(len(WORD_PAIRS)))
        chosen_idx = random.choice(candidates)
        pair = WORD_PAIRS[chosen_idx]
        await redis.lpush(recent_key, str(chosen_idx))
        await redis.ltrim(recent_key, 0, avoid - 1)
        if random.random() < 0.5:
            civ_word, uc_word = pair[0], pair[1]
        else:
            civ_word, uc_word = pair[1], pair[0]

        undercover_count, whiteboard_count = self.role_plan(len(players))
        shuffled = players[:]
        random.shuffle(shuffled)
        undercover_uids = {p.uid for p in shuffled[:undercover_count]}
        whiteboard_uids = {p.uid for p in shuffled[undercover_count:undercover_count + whiteboard_count]}

        for p in players:
            if p.uid in undercover_uids:
                role = "undercover"
                word = uc_word
            elif p.uid in whiteboard_uids:
                role = "whiteboard"
                word = ""
            else:
                role = "civilian"
                word = civ_word
            await redis.hset(
                self.player_key(scope, p.uid),
                mapping={
                    "role": role,
                    "word": word,
                    "alive": "1",
                    "blank_bonus": "0",
                    "blank_guess_state": "",
                    "blank_guess_deadline": "0",
                },
            )

        speak_order = ",".join([p.uid for p in players])
        await redis.hset(
            self.room_key(scope),
            mapping={
                "state": "speaking",
                "round": "1",
                "host_uid": room.get("host_uid", ""),
                "speak_order": speak_order,
                "speak_index": "0",
                "current_speaker_uid": players[0].uid,
                "current_speaker_deadline": "0",
                "current_speaker_reminded": "0",
                "auto_start_deadline": "0",
                "voting_deadline": "0",
            },
        )
        await redis.set(self.active_scope_key(), scope)
        await redis.delete(self.spoken_key(scope))
        await redis.delete(self.votes_key(scope))
        await redis.delete(self.speech_log_key(scope))

        failed_dm: list[int] = []
        for p in players:
            rec = await self.get_player(scope, p.uid)
            role = rec.get("role", "")
            role_cn = "卧底" if role == "undercover" else ("白板" if role == "whiteboard" else "平民")
            text = f"🎭 <b>谁是卧底 - 身份发放</b>\n你的身份：<b>{role_cn}</b>\n"
            if role == "whiteboard":
                text += "你本局没有词语，请根据场上信息伪装自己。\n"
            else:
                text += f"你的词语：<b>{rec.get('word', '')}</b>\n"

            if role == "undercover":
                mates = [x.name for x in players if x.uid in undercover_uids and x.uid != p.uid]
                text += f"同阵营卧底：<b>{'、'.join(mates) if mates else '无'}</b>\n"

            text += "\n请不要把词语原文说出来。"
            try:
                await bot.send_message(int(p.uid), text)
            except TelegramBadRequest:
                failed_dm.append(int(p.uid))
            except Exception:
                failed_dm.append(int(p.uid))

        if failed_dm:
            await self.refund_all_stakes(scope)
            await redis.hset(self.room_key(scope), mapping={"state": "lobby", "round": "0"})
            for p in players:
                await redis.hset(
                    self.player_key(scope, p.uid),
                    mapping={
                        "role": "",
                        "word": "",
                        "alive": "1",
                        "blank_bonus": "0",
                        "blank_guess_state": "",
                        "blank_guess_deadline": "0",
                    },
                )
            await redis.delete(self.stake_key(scope))
            await redis.delete(self.active_scope_key())
            return failed_dm, "", "", 0, 0, 0, 0

        total_pot = BET_PER_PLAYER * len(players)
        first_uid = players[0].uid
        first_player = await self.get_player(scope, first_uid)
        civilian_count = len(players) - undercover_count - whiteboard_count
        return [], sorted(list(undercover_uids)), first_player.get("name", first_uid), total_pot, civilian_count, undercover_count, whiteboard_count

    async def alive_players(self, scope: str) -> list[Player]:
        players = await self.list_players(scope)
        return [p for p in players if p.alive]

    async def current_speaker(self, scope: str) -> dict[str, str] | None:
        room = await self.get_room(scope)
        uid = room.get("current_speaker_uid", "")
        if not uid:
            return None
        p = await self.get_player(scope, uid)
        if not p or p.get("alive", "0") != "1":
            return None
        return p

    async def validate_speech(self, scope: str, uid: int) -> None:
        """Validate that uid is allowed to speak now, without advancing state."""
        room = await self.get_room(scope)
        if room.get("state") != "speaking":
            raise ValueError("当前不是发言阶段")
        p = await self.get_player(scope, uid)
        if not p:
            raise ValueError("你不在本局中")
        if p.get("alive", "0") != "1":
            raise ValueError("你已出局，不能发言")
        current_uid = room.get("current_speaker_uid", "")
        if str(uid) != current_uid:
            current = await self.get_player(scope, current_uid)
            current_name = current.get("name", current_uid) if current else current_uid
            raise ValueError(f"当前轮到 {current_name} 发言")

    async def submit_speech(self, scope: str, uid: int) -> tuple[bool, str, str, int]:
        room = await self.get_room(scope)
        if room.get("state") != "speaking":
            raise ValueError("当前不是发言阶段")

        p = await self.get_player(scope, uid)
        if not p:
            raise ValueError("你不在本局中")
        if p.get("alive", "0") != "1":
            raise ValueError("你已出局，不能发言")

        current_uid = room.get("current_speaker_uid", "")
        if str(uid) != current_uid:
            current = await self.get_player(scope, current_uid)
            current_name = current.get("name", current_uid) if current else current_uid
            raise ValueError(f"当前轮到 {current_name} 发言")

        await redis.sadd(self.spoken_key(scope), str(uid))

        next_p = await self.advance_speaker(scope)
        if next_p:
            return False, next_p.get("uid", ""), next_p.get("name", ""), 0

        await self.begin_voting(scope)
        alive_count = len(await self.alive_players(scope))
        return True, "", "", alive_count

    async def advance_speaker(self, scope: str) -> dict[str, str] | None:
        room = await self.get_room(scope)
        order = [x for x in room.get("speak_order", "").split(",") if x]
        if not order:
            return None

        current_idx = int(room.get("speak_index", "0"))
        total = len(order)
        idx = current_idx + 1
        while idx < total:
            uid = order[idx]
            p = await self.get_player(scope, uid)
            spoken = await redis.sismember(self.spoken_key(scope), uid)
            if p and p.get("alive", "0") == "1" and not spoken:
                await redis.hset(
                    self.room_key(scope),
                    mapping={
                        "speak_index": str(idx),
                        "current_speaker_uid": uid,
                        "current_speaker_deadline": "0",
                        "current_speaker_reminded": "0",
                    },
                )
                return p
            idx += 1

        await redis.hset(
            self.room_key(scope),
            mapping={
                "current_speaker_uid": "",
                "current_speaker_deadline": "0",
                "current_speaker_reminded": "0",
            },
        )
        return None

    async def begin_voting(self, scope: str) -> None:
        room = await self.get_room(scope)
        if room.get("state") != "speaking":
            raise ValueError("当前不能进入投票")
        await redis.hset(
            self.room_key(scope),
            mapping={
                "state": "voting",
                "current_speaker_uid": "",
                "current_speaker_deadline": "0",
                "current_speaker_reminded": "0",
                "voting_deadline": str(int(time.time()) + VOTING_TIMEOUT_SECONDS),
            },
        )
        await redis.delete(self.votes_key(scope))

    async def submit_vote(self, scope: str, voter_uid: int, target_uid: int) -> tuple[int, int]:
        room = await self.get_room(scope)
        if room.get("state") != "voting":
            raise ValueError("当前不是投票阶段")

        voter = await self.get_player(scope, voter_uid)
        target = await self.get_player(scope, target_uid)
        if not voter or not target:
            raise ValueError("玩家不存在")
        if voter.get("alive", "0") != "1":
            raise ValueError("你已出局，不能投票")
        if target.get("alive", "0") != "1":
            raise ValueError("目标已出局")
        if voter_uid == target_uid:
            raise ValueError("不能投自己")

        await redis.hset(self.votes_key(scope), str(voter_uid), str(target_uid))
        voted = await redis.hlen(self.votes_key(scope))
        alive = len(await self.alive_players(scope))
        return voted, alive

    async def finish_voting(self, scope: str) -> dict:
        votes = await redis.hgetall(self.votes_key(scope))
        alive = await self.alive_players(scope)
        alive_ids = {p.uid for p in alive}

        bucket: dict[str, int] = {}
        for _, target in votes.items():
            if target in alive_ids:
                bucket[target] = bucket.get(target, 0) + 1

        if not bucket:
            winner = await self.check_winner(scope)
            if winner:
                payout = await self.payout(winner, scope)
                return {"tie": True, "reason": "无人有效投票", "eliminated": None, "winner": winner, "payout": payout, "next_speaker": None}
            nxt = await self._next_round(scope)
            return {
                "tie": True,
                "reason": "无人有效投票",
                "eliminated": None,
                "winner": None,
                "next_speaker": nxt,
            }

        max_votes = max(bucket.values())
        top = [uid for uid, c in bucket.items() if c == max_votes]
        if len(top) > 1:
            winner = await self.check_winner(scope)
            if winner:
                payout = await self.payout(winner, scope)
                return {"tie": True, "reason": f"平票（{max_votes} 票）", "eliminated": None, "winner": winner, "payout": payout, "next_speaker": None}
            nxt = await self._next_round(scope)
            return {
                "tie": True,
                "reason": f"平票（{max_votes} 票）",
                "eliminated": None,
                "winner": None,
                "next_speaker": nxt,
            }

        eliminated_uid = top[0]
        await redis.hset(self.player_key(scope, eliminated_uid), "alive", "0")
        eliminated_player = await self.get_player(scope, eliminated_uid)
        eliminated_speech = ""
        for speaker_uid, content in await self.list_speech_logs(scope):
            if speaker_uid == eliminated_uid:
                eliminated_speech = content
                break
        blank_guess_pending = False
        if eliminated_player.get("role") == "whiteboard":
            blank_guess_pending = True
            await redis.hset(
                self.player_key(scope, eliminated_uid),
                mapping={
                    "blank_guess_state": "pending",
                    "blank_guess_deadline": str(int(time.time()) + 30),
                },
            )

        winner = await self.check_winner(scope)
        if winner:
            await redis.hset(self.room_key(scope), "state", "ended")
            payout = await self.payout(winner, scope)
            return {
                "tie": False,
                "eliminated": eliminated_uid,
                "winner": winner,
                "votes": bucket,
                "payout": payout,
                "blank_guess_pending": blank_guess_pending,
                "eliminated_speech": eliminated_speech,
            }

        nxt = await self._next_round(scope)
        return {
            "tie": False,
            "eliminated": eliminated_uid,
            "winner": None,
            "votes": bucket,
            "next_speaker": nxt,
            "blank_guess_pending": blank_guess_pending,
            "eliminated_speech": eliminated_speech,
        }

    async def auto_fill_missing_votes(self, scope: str) -> tuple[int, int]:
        alive = await self.alive_players(scope)
        votes = await redis.hgetall(self.votes_key(scope))
        voted_uids = set(votes.keys())
        filled = 0
        for p in alive:
            if p.uid in voted_uids:
                continue
            await redis.hset(self.votes_key(scope), p.uid, p.uid)
            filled += 1
        return filled, len(alive)

    async def _next_round(self, scope: str) -> dict[str, str] | None:
        room = await self.get_room(scope)
        nxt_round = int(room.get("round", "1")) + 1
        alive = await self.alive_players(scope)
        order = ",".join([p.uid for p in alive])
        first_uid = alive[0].uid if alive else ""

        async with redis.pipeline(transaction=True) as pipe:
            pipe.hset(
                self.room_key(scope),
                mapping={
                    "state": "speaking",
                    "round": str(nxt_round),
                    "speak_order": order,
                    "speak_index": "0",
                    "current_speaker_uid": first_uid,
                    "current_speaker_deadline": "0",
                    "current_speaker_reminded": "0",
                },
            )
            pipe.delete(self.spoken_key(scope))
            pipe.delete(self.votes_key(scope))
            pipe.delete(self.speech_log_key(scope))
            await pipe.execute()

        if not first_uid:
            return None
        return await self.get_player(scope, first_uid)

    async def check_winner(self, scope: str) -> str | None:
        alive = await self.alive_players(scope)
        uc_alive = [p for p in alive if p.role == "undercover"]
        civ_alive = [p for p in alive if p.role == "civilian"]
        wb_alive = [p for p in alive if p.role == "whiteboard"]

        if not uc_alive and not civ_alive:
            # 只剩白板，或白板独活
            return "whiteboard" if wb_alive else "civilian"
        if not uc_alive:
            # 卧底全出，平民（+白板）获胜
            return "civilian"
        if not civ_alive and not wb_alive:
            # 平民和白板全出，卧底获胜
            return "undercover"
        if not wb_alive and uc_alive and len(uc_alive) >= len(civ_alive):
            # 卧底人数 >= 平民，卧底获胜
            return "undercover"
        if wb_alive and not civ_alive:
            # 无平民，白板还在，白板获胜
            return "whiteboard"
        return None

    async def eliminate_player(self, scope: str, uid: str) -> None:
        await redis.hset(self.player_key(scope, uid), "alive", "0")
        await redis.srem(self.spoken_key(scope), uid)
        await redis.hdel(self.votes_key(scope), uid)

    async def timeout_current_speaker(self, scope: str) -> dict:
        room = await self.get_room(scope)
        if room.get("state") != "speaking":
            return {"ok": False, "reason": "not_speaking"}
        uid = room.get("current_speaker_uid", "")
        if not uid:
            return {"ok": False, "reason": "no_speaker"}
        p = await self.get_player(scope, uid)
        if not p or p.get("alive", "0") != "1":
            nxt = await self.advance_speaker(scope)
            if nxt:
                return {"ok": True, "type": "next_speaker", "next_speaker": nxt}
            await self.begin_voting(scope)
            return {"ok": True, "type": "voting"}

        await self.eliminate_player(scope, uid)

        winner = await self.check_winner(scope)
        if winner:
            await redis.hset(self.room_key(scope), "state", "ended")
            payout = await self.payout(winner, scope)
            return {
                "ok": True,
                "type": "ended",
                "winner": winner,
                "eliminated_uid": uid,
                "payout": payout,
            }

        nxt = await self.advance_speaker(scope)
        if nxt:
            return {"ok": True, "type": "next_speaker", "eliminated_uid": uid, "next_speaker": nxt}

        await self.begin_voting(scope)
        return {"ok": True, "type": "voting", "eliminated_uid": uid}

    async def refund_all_stakes(self, scope: str) -> None:
        stakes = await redis.hgetall(self.stake_key(scope))
        for uid, amount in stakes.items():
            val = int(float(amount or 0))
            if val > 0:
                await update_balance(uid, val)

    async def reveal_words(self, scope: str) -> tuple[str, str, str, list[dict]]:
        players = await self.list_players(scope)
        undercover_word = ""
        civilian_word = ""
        undercover_names: list[str] = []
        whiteboard_info: list[dict] = []
        for p in players:
            if p.role == "undercover":
                undercover_word = p.word
                undercover_names.append(mention(int(p.uid), p.name))
            elif p.role == "civilian":
                civilian_word = p.word
            elif p.role == "whiteboard":
                whiteboard_info.append({
                    "name": mention(int(p.uid), p.name),
                    "bonus": p.get("blank_bonus", "0") == "1",
                })
        return civilian_word, undercover_word, "、".join(undercover_names), whiteboard_info

    async def pending_blank_guess_players(self, scope: str) -> list[Player]:
        players = await self.list_players(scope)
        return [p for p in players if p.role == "whiteboard" and p.blank_guess_state == "pending"]

    async def submit_blank_guess(self, scope: str, uid: int, guess: str) -> dict:
        room = await self.get_room(scope)
        if not room or room.get("state") == "ended":
            raise ValueError("当前对局已结束，白板猜词失败。")

        p = await self.get_player(scope, uid)
        if not p or p.get("role") != "whiteboard":
            raise ValueError("你不是本局白板。")
        if p.get("blank_guess_state", "") != "pending":
            raise ValueError("你当前没有待处理的白板猜词机会。")

        deadline = int(float(p.get("blank_guess_deadline", "0") or 0))
        if deadline <= 0 or deadline < int(time.time()):
            await redis.hset(
                self.player_key(scope, str(uid)),
                mapping={"blank_guess_state": "failed", "blank_guess_deadline": "0"},
            )
            raise ValueError("白板猜词已超时。")

        civ_word, uc_word, _ = await self.reveal_words(scope)
        normalized = (guess or "").strip()
        matched = normalized in {civ_word, uc_word}
        await redis.hset(
            self.player_key(scope, str(uid)),
            mapping={
                "blank_guess_state": "success" if matched else "failed",
                "blank_guess_deadline": "0",
                "blank_bonus": "1" if matched else "0",
            },
        )
        return {"matched": matched, "guess": normalized}

    async def append_speech_log(self, scope: str, uid: int | str, content: str) -> None:
        await redis.rpush(self.speech_log_key(scope), f"{uid}\t{content}")

    async def list_speech_logs(self, scope: str) -> list[tuple[str, str]]:
        raw = await redis.lrange(self.speech_log_key(scope), 0, -1)
        rows: list[tuple[str, str]] = []
        for item in raw:
            uid, sep, content = item.partition("\t")
            if not sep:
                rows.append((uid, ""))
                continue
            rows.append((uid, content))
        return rows

    async def open_rooms(self) -> list[str]:
        cursor = 0
        scopes: list[str] = []
        while True:
            cursor, keys = await redis.scan(cursor, match="uc:room:*", count=100)
            for key in keys:
                # 仅保留真正的房间主键：uc:room:{chat_id}:{thread_id}
                # 子键如 uc:room:{scope}:players / :votes / :player:* 需要跳过
                suffix = key.split("uc:room:", 1)[1]
                if suffix.count(":") != 1:
                    continue
                try:
                    if await redis.type(key) != "hash":
                        continue
                except Exception:
                    continue
                scopes.append(suffix)
            if cursor == 0:
                break
        return scopes

    async def user_in_any_room(self, uid: int | str) -> bool:
        target = str(uid)
        for scope in await self.open_rooms():
            ids = await self.list_player_ids(scope)
            if target in ids:
                return True
        return False
