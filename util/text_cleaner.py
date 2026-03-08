import re
import emoji

def clean_text(text: str) -> str:
    if not text:
        return ""

    text = text.lower()

    text = re.sub(r"<[^>]+>", "", text)

    text = re.sub(r"http\S+|www\S+", "", text)

    text = re.sub(r"[@#]\w+", "", text)

    text = emoji.replace_emoji(text, replace="")

    text = re.sub(r"[\n\t]", " ", text)

    text = re.sub(r"[^a-zа-яёіїєґ0-9\s]", "", text)

    text = re.sub(r"\s+", " ", text).strip()

    return text
