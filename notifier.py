import requests

TELEGRAM_MAX_CHARS = 4096


def send_telegram_message(bot_token: str, chat_id: str, text: str) -> dict:
    """
    Send a plain-text message to Telegram, splitting it into chunks if needed.
    Uses no parse_mode to avoid Markdown special-character errors.
    """
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    chunks = _split_message(text)
    last_response = {}
    for chunk in chunks:
        response = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": chunk,
                "disable_web_page_preview": True,
            },
            timeout=30,
        )
        response.raise_for_status()
        last_response = response.json()

    return last_response


def _split_message(text: str, max_len: int = TELEGRAM_MAX_CHARS) -> list:
    """
    Split a message into chunks of at most max_len characters,
    breaking at newlines where possible.
    """
    if len(text) <= max_len:
        return [text]

    chunks = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        # Find the last newline within the limit
        split_at = text.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = max_len
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")

    return chunks
