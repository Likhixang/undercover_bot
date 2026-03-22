import os

TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not TOKEN:
    raise ValueError("BOT_TOKEN missing")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://axon.khixang.cc.cd/v1").strip()

RUN_MODE = os.getenv("RUN_MODE", "polling").strip().lower()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram/webhook").strip() or "/telegram/webhook"
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST", "0.0.0.0").strip() or "0.0.0.0"
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8787"))
WEBHOOK_SECRET_TOKEN = os.getenv("WEBHOOK_SECRET_TOKEN", "").strip()

REDIS_HOST = os.getenv("REDIS_HOST", "redis").strip() or "redis"
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "").strip() or None

# 可选：积分互通 Redis（留空则使用本地 redis）
POINTS_REDIS_HOST = os.getenv("POINTS_REDIS_HOST", "").strip()
POINTS_REDIS_PORT = int(os.getenv("POINTS_REDIS_PORT", "6379"))
POINTS_REDIS_DB = int(os.getenv("POINTS_REDIS_DB", "0"))
POINTS_REDIS_PASSWORD = os.getenv("POINTS_REDIS_PASSWORD", "").strip()

# 话题限制（0 = 不限制）
ALLOWED_CHAT_ID = int(os.getenv("ALLOWED_CHAT_ID", "0"))
ALLOWED_THREAD_ID = int(os.getenv("ALLOWED_THREAD_ID", "0"))

MIN_PLAYERS = int(os.getenv("MIN_PLAYERS", "4"))
MAX_PLAYERS = int(os.getenv("MAX_PLAYERS", "7"))

BET_PER_PLAYER = int(os.getenv("BET_PER_PLAYER", "100"))
SHARED_POINTS_INIT = float(os.getenv("SHARED_POINTS_INIT", "20000"))
COMPENSATION_AMOUNT = int(os.getenv("COMPENSATION_AMOUNT", "200"))

SPEAK_TIMEOUT_SECONDS = int(os.getenv("SPEAK_TIMEOUT_SECONDS", "45"))
SPEAK_REMIND_BEFORE_SECONDS = int(os.getenv("SPEAK_REMIND_BEFORE_SECONDS", "30"))
AUTO_START_IDLE_SECONDS = int(os.getenv("AUTO_START_IDLE_SECONDS", "15"))
VOTING_TIMEOUT_SECONDS = int(os.getenv("VOTING_TIMEOUT_SECONDS", "30"))
WATCHDOG_INTERVAL_SECONDS = int(os.getenv("WATCHDOG_INTERVAL_SECONDS", "3"))


def _parse_admin_ids(raw: str) -> set[int]:
    ids: set[int] = set()
    for part in (raw or "").split(","):
        s = part.strip()
        if not s:
            continue
        try:
            ids.add(int(s))
        except ValueError:
            continue
    return ids


def _parse_word_pairs(raw: str) -> list[tuple[str, str]]:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("WORD_PAIRS missing")

    pairs: list[tuple[str, str]] = []
    for item in raw.split(";"):
        part = item.strip()
        if not part:
            continue
        left, sep, right = part.partition("|")
        left = left.strip()
        right = right.strip()
        if sep != "|" or not left or not right:
            raise ValueError("WORD_PAIRS 格式错误，需使用 `词1|词2;词3|词4`")
        pairs.append((left, right))

    if not pairs:
        raise ValueError("WORD_PAIRS missing")
    return pairs


SUPER_ADMIN_IDS = _parse_admin_ids(os.getenv("SUPER_ADMIN_IDS", ""))
WORD_PAIRS = _parse_word_pairs(os.getenv("WORD_PAIRS", ""))
