import html


def safe_html(s: str) -> str:
    return html.escape(s or "")


def mention(uid: int | str, name: str) -> str:
    return f'<a href="tg://user?id={uid}">{safe_html(name or str(uid))}</a>'


def scope_id(chat_id: int, thread_id: int | None) -> str:
    return f"{chat_id}:{thread_id or 0}"
