import re

PATTERNS = {
    "air_alert": re.compile(
        r"(повітрян\w*\s*тривог\w*|тривог\w*|відбій\s*тривог\w*|відбій"
        r"|воздушн\w*\s*тревог\w*|тревог\w*|отбой\s*тревог\w*|отбой"
        r"|air\s*(alert|alarm|raid|warning)|all\s*clear|siren\w*)",
        re.I
    ),
    "missiles": re.compile(
        r"(ракет\w*|missil\w*|cruise\s*missil\w*|ballistic\s*missil\w*"
        r"|міжконтинентальн\w*\s*балістичн\w*|межконтинентальн\w*\s*баллистичн\w*"
        r"|мбр|icbm|intercontinental\s*ballistic)",
        re.I
    ),
    "drones": re.compile(
        r"(дрон\w*|бпла|uav\w*|uas\w*|shahed\s?\d*|шахед\w*"
        r"|камікадзе\s*дрон\w*|kamikaze\s*drone\w*|attack\s*drone\w*|герань\w*|geran\w*)",
        re.I
    ),
    "strike": re.compile(
        r"(удар\w*|обстр\w*|прил[её]т\w*|влуч\w*|атак\w*|авіаудар\w*|авиаудар\w*"
        r"|strike\w*|air\s*strike\w*|bombard\w*|shell\w*|attack\w*)",
        re.I
    ),
    "explosion": re.compile(
        r"(вибух\w*|детонац\w*|взрыв\w*|detonat\w*|blast\w*|explos\w*)",
        re.I
    ),
    "air_defense": re.compile(
        r"(пво|протиповітрян\w*\s*оборон\w*|противовоздушн\w*\s*оборон\w*"
        r"|перехоплен\w*|перехват\w*|збит\w*|сбит\w*"
        r"|air\s*defen[cs]e|intercept\w*|shot\s*down)",
        re.I
    ),
    "casualties": re.compile(
        r"(жертв\w*|поранен\w*|загиб\w*|ліквідован\w*|знищен\w*|ураженн\w*"
        r"|ранен\w*|погиб\w*|ликвидирован\w*|уничтожен\w*|поражен\w*"
        r"|killed|wounded|casualt\w*|dead|losses|destroyed)",
        re.I
    ),
    "infrastructure": re.compile(
        r"(електростанц\w*|підстанц\w*|електромереж\w*|аеродром\w*"
        r"|электростанц\w*|подстанц\w*|электросет\w*|аэродром\w*"
        r"|power\s*plant|substation|power\s*grid|airfield|ammo\w*\s*depot|ammunition\s*depot)",
        re.I
    ),
}

WEAPON_PATTERNS = {
    "kh_series": re.compile(
        r"\b([хxkh]{1,2}\s?(22|32|55|59|31|101|555))\b",
        re.I
    ),
    "s_system": re.compile(
        r"\b([сs]\s?(300|400|500))\b",
        re.I
    ),
    "kn23": re.compile(
        r"\b(kn\s?23|кн\s?23)\b",
        re.I
    ),
    "zircon": re.compile(
        r"(3[мm]\s?22|циркон|zircon)",
        re.I
    ),
    "oreshnik": re.compile(
        r"(орешні[кк]\w*|oreshnik\w*)",
        re.I
    ),
    "iskander": re.compile(
        r"(іскандер\w*|искандер\w*|iskander\w*)",
        re.I
    ),
    "kinzhal": re.compile(
        r"(кинджал\w*|кинжал\w*|kinzhal\w*)",
        re.I
    ),
    "shahed": re.compile(
        r"(шахед\w*|shahed\s?\d*)",
        re.I
    ),
    "kalibr": re.compile(
        r"(калібр\w*|калибр\w*|kalibr\w*|caliber\w*)",
        re.I
    ),
    "himars": re.compile(
        r"(хаймарс\w*|himars\w*)",
        re.I
    ),
    "patriot": re.compile(
        r"(патріот\w*|патриот\w*|patriot\w*)",
        re.I
    ),
    "izdeliye30": re.compile(
        r"(виріб\s?30|изделие\s?30|izdeliy?e\s?30)",
        re.I
    ),
}

IGNORE = re.compile(
    r"(training\s*exercise|simulation|history|museum|игра|гра|відеогр\w*|видеоигр\w*)",
    re.I
)

def detect_events(text: str) -> list:
    if not text or IGNORE.search(text):
        return []

    events = []

    for name, pattern in PATTERNS.items():
        if pattern.search(text):
            events.append(name)

    for name, pattern in WEAPON_PATTERNS.items():
        if pattern.search(text):
            events.append(name)

    return list(set(events))

NUMBER_PATTERN = re.compile(r"\b\d+\b")

def extract_numbers(text: str) -> list:
    return NUMBER_PATTERN.findall(text)
