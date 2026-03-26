"""
product_identity.py — query intent, listing role, identity match, and eligibility inputs.

Rules-based catalog intelligence used by discovery_filters (wrapper) and ranking.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Pattern

# ---------------------------------------------------------------------------
# Query intent
# ---------------------------------------------------------------------------


class QueryType(str, Enum):
    EXACT_MODEL = "exact_model"
    NAMED_PRODUCT = "named_product"
    PRODUCT_LINE = "product_line"
    CATEGORY = "category"


@dataclass
class QueryIntent:
    query_type: QueryType
    raw_normalized: str
    family: dict[str, Any] | None
    accessory_intent: bool
    model_token: str | None
    brand: str | None = None
    required_tokens: tuple[str, ...] = ()
    soft_variant_tokens: tuple[str, ...] = ()
    hard_variant_tokens: tuple[str, ...] = ()

    @property
    def family_def(self) -> dict[str, Any] | None:
        return self.family


def normalize_user_query(raw_user_query: str) -> str:
    text = (raw_user_query or "").lower().strip()
    text = text.replace("×", "x")
    text = re.sub(r"(\d+)\s*[- ]\s*in\s*[- ]\s*(\d+)", r"\1in\2", text)
    text = re.sub(r"(\d+)\s*mm\b", r"\1mm", text)
    text = re.sub(r"(\d+(?:\.\d+)?)\s*(?:quart|quarts)\b", r"\1qt", text)
    text = re.sub(r"(\d+)\s*x\s*(\d+)", r"\1x\2", text)
    return re.sub(r"\s+", " ", text)


_STRUCTURED_TOKEN_PATTERNS: tuple[Pattern[str], ...] = (
    re.compile(r"\b\d+in\d+\b", re.I),
    re.compile(r"\b\d{2,3}mm\b", re.I),
    re.compile(r"\b\d+(?:\.\d+)?qt\b", re.I),
    re.compile(r"\b\d{2,3}x\d{2,3}\b", re.I),
)

_SOFT_VARIANT_QUERY_PATTERNS: tuple[Pattern[str], ...] = (
    re.compile(
        r"\b(?:black|white|silver|blue|red|green|pink|gray|grey|midnight|"
        r"starlight|purple|gold|graphite|ivory|navy|beige|orange|yellow)\b",
        re.I,
    ),
)

_NON_ALNUM = re.compile(r"[^a-z0-9]+", re.I)


def _normalize_identity_token(value: str | None) -> str | None:
    if not value:
        return None
    out = _NON_ALNUM.sub("", value.lower())
    return out or None


def _dedupe_tokens(values: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    seen: list[str] = []
    for raw in values:
        token = normalize_user_query(raw).strip()
        if token and token not in seen:
            seen.append(token)
    return tuple(seen)


def _extract_structured_tokens(text: str) -> tuple[str, ...]:
    normalized = normalize_user_query(text)
    found: list[str] = []
    for pattern in _STRUCTURED_TOKEN_PATTERNS:
        for match in pattern.finditer(normalized):
            token = normalize_user_query(match.group(0))
            if token not in found:
                found.append(token)
    return tuple(found)


def _extract_soft_variant_tokens(text: str) -> tuple[str, ...]:
    normalized = normalize_user_query(text)
    found: list[str] = []
    for pattern in _SOFT_VARIANT_QUERY_PATTERNS:
        for match in pattern.finditer(normalized):
            token = normalize_user_query(match.group(0))
            if token not in found:
                found.append(token)
    return tuple(found)


# ---------------------------------------------------------------------------
# Condition (title + URL)
# ---------------------------------------------------------------------------

_OPEN_BOX = re.compile(r"\bopen[- ]?box\b", re.I)

_COND_RANK = {"new": 0, "open_box": 1, "renewed": 2, "refurbished": 3, "used": 4}


def _prepare_condition_text(title_lower: str) -> str:
    t = (title_lower or "").lower()
    t = re.sub(
        r"\b(open[- ]?box|renewed|refurb(?:ished)?|recertified|restored|pre[- ]?owned)(?=[a-z0-9])",
        r"\1 ",
        t,
        flags=re.I,
    )
    return t


def detect_condition(title_lower: str) -> str:
    t = _prepare_condition_text(title_lower)
    if _OPEN_BOX.search(t) and not re.search(
        r"\b(refurb|renewed|used|pre[- ]?owned)\b", t, re.I
    ):
        return "open_box"
    if re.search(
        r"\brenewed\b|\[renewed\]|\(renewed\)|\bamazon\s+renewed\b|"
        r"[-–—|]\s*renewed\b",
        t,
        re.I,
    ):
        return "renewed"
    if re.search(
        r"\b(refurb(?:ished)?|recertified|restored|certified\s+refurbished|"
        r"seller\s+refurbished|professionally\s+refurbished)\b",
        t,
        re.I,
    ):
        return "refurbished"
    if re.search(
        r"\b(pre[- ]?owned|used|like\s+new|excellent\s+condition|grade\s*a)\b",
        t,
        re.I,
    ):
        return "used"
    return "new"


def condition_hint_from_url(url: str | None) -> str | None:
    if not url:
        return None
    u = url.lower()
    if "renewed" in u:
        return "renewed"
    if (
        "refurb" in u
        or "certified_refurbished" in u
        or "certified-refurbished" in u
    ):
        return "refurbished"
    if "open+box" in u or "openbox" in u or "open-box" in u or "%20open%20box" in u:
        return "open_box"
    if any(
        x in u
        for x in (
            "condition=used",
            "condition%3Dused",
            "p_n_condition-type=used",
            "rt%3dnc",
        )
    ):
        return "used"
    return None


def merge_condition_title_url(title: str, url: str | None) -> str:
    ct = detect_condition((title or "").lower())
    cu = condition_hint_from_url(url)
    if cu is None:
        return ct
    if ct == "new":
        return cu
    return ct if _COND_RANK.get(ct, 0) >= _COND_RANK.get(cu, 0) else cu


# ---------------------------------------------------------------------------
# Accessory / compatibility
# ---------------------------------------------------------------------------


def _strip_bundle_case_phrases(title_lower: str) -> str:
    return re.sub(
        r"magsafe\s+charging\s+case|wireless\s+magsafe\s+charging\s+case|"
        r"wireless\s+charging\s+case|charging\s+case\s*\([^)]{0,40}\)",
        " ",
        title_lower,
        flags=re.I,
    )


_WATCH_PRIMARY_SIGNALS = re.compile(
    r"\bsmartwatch\b|\bgps\b|\bcellular\b|\b\d{2}\s*mm\b|"
    r"\b(?:aluminum|titanium|stainless\s+steel)\s+case\b",
    re.I,
)


def _strip_watch_primary_phrases(title_lower: str) -> str:
    if not _WATCH_PRIMARY_SIGNALS.search(title_lower or ""):
        return title_lower
    return re.sub(
        r"\b(?:aluminum|titanium|stainless\s+steel)\s+case\b|"
        r"\b(?:sport|solo|magnetic|milanese|ocean|trail|nike(?:\s+sport)?)\s+"
        r"(?:band|loop)\b|"
        r"\b(?:s/m|m/l|s\b|m\b|l\b)\b",
        " ",
        title_lower,
        flags=re.I,
    )


_ACCESSORY_WORDS = re.compile(
    r"\b("
    r"case|cases|cover|covers|skin|skins|ear\s*tips?|eartips?|\btips\b|"
    r"charger|charging\s*pad|charging\s*stand|charging\s*station|"
    r"\bdock\b|docks|replacement\s*case|holder|strap|straps|hook|hooks|"
    r"transmitter|adapter|adapters|cushion|earpads?|foam|silicone|"
    r"protective|shell|shells|sleeve|sleeves|keychain|lanyard|"
    r"cleaning\s*kit|replacement\s*ear|wing\s*tips?|memory\s*foam|"
            r"wrist\s*rest|keycap|keycaps|screen\s*protector|lens\s*cap|tripod|"
            r"cooling\s*pad|controller\s*grip|battery\b(?!life)|mouse\s*pad"
    r")\b",
    re.I,
)

_COMPAT_PHRASES = re.compile(
    r"\b("
    r"compatible\s+with|works\s+with|fits\b|designed\s+for|"
    r"for\s+(?:apple\s+|samsung\s+|google\s+|sony\s+|microsoft\s+|bose\s+|beats\s+|anker\s+)?airpods?|"
    r"for\s+(?:apple\s+)?iphone|"
    r"for\s+apple\s+watch|"
    r"for\s+apple\b|for\s+samsung\b|"
    r"replacement\s+for|alternative\s+to|similar\s+to"
    r")\b",
    re.I,
)

_BUNDLE_WORDS = re.compile(
    r"\b(bundle|combo|kit\b|set\s+of\s+\d|pack\s+of\s+\d|\d[- ]?\s*pack)\b",
    re.I,
)

_ACCESSORY_INTENT_QUERY = re.compile(
    r"\b("
    r"case|cases|cover|charger|charging|stand|dock|strap|skin|tips?|"
    r"holder|adapter|replacement|accessory|accessories|protector"
    r")\b",
    re.I,
)


def query_has_accessory_intent(query_norm: str) -> bool:
    return bool(_ACCESSORY_INTENT_QUERY.search(query_norm))


# Universal model hints when family has no model_patterns
_UNIVERSAL_MODEL_PATTERNS: tuple[Pattern[str], ...] = (
    re.compile(r"\brtx\s*\d{3,4}\s*(?:ti|super)?\b", re.I),
    re.compile(r"\bgtx\s*\d{4}\b", re.I),
    re.compile(r"\bwh[- ]?1000xm[45]\b", re.I),
    re.compile(
        r"\biphone\s*(?:\d{2})\s*(?:pro\s*max|pro|plus|mini)?\b", re.I
    ),
    re.compile(r"\b(?:galaxy|pixel)\s*[sz]\s*\d{2,3}\b", re.I),
    re.compile(r"\bairpods?\s*(?:pro|max)?\s*(?:2|3|4)?\b", re.I),
    re.compile(r"\bmp[ol]\s*-\s*\d{2,4}\b", re.I),
)


def _norm_model(s: str) -> str:
    return _normalize_identity_token(s) or ""


def _extract_model_token(q: str, family: dict[str, Any] | None) -> str | None:
    patterns: list[Pattern[str]] = []
    if family:
        mp = family.get("model_patterns")
        if mp:
            patterns.extend(mp if isinstance(mp, (list, tuple)) else [mp])
    patterns.extend(_UNIVERSAL_MODEL_PATTERNS)
    seen: set[str] = set()
    for p in patterns:
        m = p.search(q)
        if m:
            tok = _norm_model(m.group(0))
            if len(tok) >= 3 and tok not in seen:
                return m.group(0).strip()
    return None


def _model_mismatch_exact(
    query_type: QueryType,
    intent_model: str | None,
    title_lower: str,
) -> bool:
    if query_type != QueryType.EXACT_MODEL or not intent_model:
        return False
    qn = _norm_model(intent_model)
    # Any same-normalized token in title from universal patterns
    for p in _UNIVERSAL_MODEL_PATTERNS:
        for m in p.finditer(title_lower):
            if _norm_model(m.group(0)) == qn:
                return False
    # Fallback: substring
    if qn and qn in (_normalize_identity_token(title_lower) or ""):
        return False
    return True


# ---------------------------------------------------------------------------
# Family definitions
# ---------------------------------------------------------------------------

_FAMILY_DEFS: list[dict[str, Any]] = [
    {
        "id": "airpods",
        "match_query": re.compile(r"\bairpods?\b|\bair\s*pods?\b", re.I),
        "title_core": re.compile(r"\bairpods?\b|\bair\s*pods?\b", re.I),
        "brand_tokens": ("apple",),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": (),
        "accessory_signals": (),
        "negative_signals": (),
        "search_alias_templates": (),
        "model_patterns": [
            re.compile(r"\bairpods?\s*pro\s*\d\b", re.I),
            re.compile(r"\bairpods?\s*\d\b", re.I),
            re.compile(r"\bairpods?\s*max\b", re.I),
        ],
        "hard_block": re.compile(
            r"\bairtag\b|\btile\s|galaxy\s*buds|pixel\s*buds|oneplus\s*buds|"
            r"nothing\s*ear|jabra|jabra\s",
            re.I,
        ),
        "other_brand_earbuds": re.compile(
            r"\b(beats|soundcore|jlab|skullcandy|anker|tozo|tribit|bose\s*quietcomfort|"
            r"sony\s*wf[- ]?1000|jbl\s|sennheiser)\b",
            re.I,
        ),
        "category_accessory_words": (),
    },
    {
        "id": "iphone",
        "match_query": re.compile(r"\biphone\b", re.I),
        "title_core": re.compile(r"\biphone\b", re.I),
        "brand_tokens": ("apple",),
        "model_patterns": [
            re.compile(r"\biphone\s*(?:\d{2})\s*(?:pro\s*max|pro|plus|mini)?\b", re.I),
        ],
        "hard_block": re.compile(
            r"\bipad\b|\bmacbook\b|\bwatch\b|\bairtag\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": (),
    },
    {
        "id": "macbook",
        "match_query": re.compile(r"\bmac\s*book\b|\bmacbook\b", re.I),
        "title_core": re.compile(r"\bmac\s*book\b|\bmacbook\b", re.I),
        "brand_tokens": ("apple",),
        "hard_block": re.compile(r"\bipad\b", re.I),
        "other_brand_earbuds": None,
        "category_accessory_words": ("sleeve", "bag", "adapter", "charger"),
    },
    {
        "id": "apple_watch",
        "match_query": re.compile(r"\bapple\s*watch\b", re.I),
        "title_core": re.compile(
            r"\bapple\s*watch\b|\bwatch\s*series\b|\bwatch\s*ultra\b", re.I
        ),
        "brand_tokens": ("apple",),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("smartwatch", "gps", "cellular", "40mm", "44mm"),
        "accessory_signals": ("band", "strap", "charger", "case"),
        "negative_signals": (),
        "search_alias_templates": (
            "{brand} {raw}",
            "{raw}",
        ),
        "model_patterns": [
            re.compile(r"\b(?:apple\s*)?watch\s*se(?:\s*(?:\(?\d(?:nd|rd|th)?\s*gen\)?|\d))?\b", re.I),
            re.compile(r"\b(?:apple\s*)?watch\s*series\s*\d{1,2}\b", re.I),
            re.compile(r"\b(?:apple\s*)?watch\s*ultra\s*\d?\b", re.I),
        ],
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": ("band", "strap", "charger", "case"),
    },
    {
        "id": "ipad",
        "match_query": re.compile(r"\bipad\b", re.I),
        "title_core": re.compile(r"\bipad\b", re.I),
        "brand_tokens": ("apple",),
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": ("case", "cover", "stylus", "keyboard"),
    },
    {
        "id": "monitor",
        "match_query": re.compile(r"\bmonitor\b|\bdisplay\b|\bscreen\b", re.I),
        "title_core": re.compile(
            r"\bmonitor\b|\bdisplay\b|\b\d{2,3}\s*hz\b|\bips\b|\bva\b|\boled\b",
            re.I,
        ),
        "brand_tokens": (
            "samsung", "lg", "asus", "acer", "msi", "dell", "benq",
            "viewsonic", "alienware", "gigabyte", "hp", "lenovo",
        ),
        "hard_block": re.compile(
            r"\bfire\s*tv\b|\bfirestick\b|\bfire\s*stick\b|\bhdmi\s*cable\b|"
            r"\bwebcam\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("mount", "arm", "stand", "cable"),
    },
    {
        "id": "headphones",
        "match_query": re.compile(
            r"\bheadphones?\b|\bheadset\b|\bover[- ]ear\b|\bon[- ]ear\b|"
            r"\bnoise\s*cancell",
            re.I,
        ),
        "title_core": re.compile(
            r"\bheadphones?\b|\bheadset\b|\bover[- ]ear\b|\bon[- ]ear\b",
            re.I,
        ),
        "brand_tokens": (
            "sony", "bose", "sennheiser", "audio-technica", "beyerdynamic",
            "shure", "akg", "jabra", "plantronics", "steelseries", "hyperx",
        ),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": (),
        "accessory_signals": ("ear pad", "earpad", "cushion", "hanger", "stand"),
        "negative_signals": (),
        "search_alias_templates": (
            "{brand} {model_token} headphones",
            "{model_token}",
        ),
        "model_patterns": [
            re.compile(r"\bwh[- ]?1000xm[45]\b", re.I),
            re.compile(r"\bqc\s*\d{2,3}\b", re.I),
        ],
        "hard_block": re.compile(
            r"\bearbuds?\b|\bin[- ]ear\b|\btws\b|airpods|galaxy\s*buds",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": (
            "ear pad", "earpad", "cushion", "hanger", "stand",
        ),
    },
    {
        "id": "gpu",
        "match_query": re.compile(
            r"\brtx\b|\bgtx\b|\bgpu\b|\bgeforce\b|\bradeon\b|\bgraphics\s*card\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\brtx\b|\bgtx\b|\bradeon\b|\bgraphics\s*card\b|\bgpu\b",
            re.I,
        ),
        "brand_tokens": (
            "nvidia", "geforce", "amd", "radeon", "msi", "asus", "gigabyte",
            "zotac", "pny", "evga",
        ),
        "brand_policy": "platform_plus_partner",
        "partner_brands": ("msi", "asus", "gigabyte", "zotac", "pny", "evga"),
        "primary_signals": ("graphics card", "gpu", "geforce", "radeon"),
        "accessory_signals": ("riser", "cable", "bracket", "vertical mount"),
        "negative_signals": ("riser", "cable", "bracket", "water block"),
        "search_alias_templates": (
            "{brand} {model_token} graphics card",
            "{model_token} graphics card",
            "{raw}",
        ),
        "model_patterns": [
            re.compile(r"\brtx\s*\d{3,4}\s*(?:ti|super)?\b", re.I),
            re.compile(r"\brx\s*\d{3,4}\s*(?:xt)?\b", re.I),
        ],
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": ("riser", "cable", "bracket"),
    },
    {
        "id": "pressure_cooker",
        "match_query": re.compile(
            r"\binstant\s*pot\b|\bpressure\s*cooker\b|\bmulticooker\b|\bmulti\s*cooker\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\binstant\s*pot\b|\bpressure\s*cooker\b|\bmulticooker\b|\bmulti\s*cooker\b",
            re.I,
        ),
        "brand_tokens": ("instant pot", "ninja", "crock-pot", "crockpot"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("pressure cooker", "multicooker", "multi cooker", "7in1", "9in1"),
        "accessory_signals": (
            "replacement lid", "lid", "seal ring", "gasket", "cookbook",
            "steamer basket", "steam rack",
        ),
        "negative_signals": (
            "replacement lid", "cookbook", "seal ring", "gasket", "steamer basket",
            "air fryer oven",
        ),
        "search_alias_templates": (
            "{brand} {required_tokens} pressure cooker",
            "{raw}",
        ),
        "hard_block": re.compile(
            r"\bcookbook\b|\breplacement\s*lid\b|\bseal\s*ring\b|\bgasket\b|"
            r"\bsteam(?:er)?\s*basket\b|\bair\s*fryer\s*oven\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": (
            "lid", "seal ring", "gasket", "basket", "cookbook",
        ),
    },
    {
        "id": "air_fryer",
        "match_query": re.compile(
            r"\bair\s*fryer\b|\bairfryer\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\bair\s*fryer\b|\bairfryer\b",
            re.I,
        ),
        "brand_tokens": ("ninja", "instant", "instant pot", "cosori", "philips", "gourmia"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("air fryer", "basket", "qt", "quart"),
        "accessory_signals": ("liner", "paper", "rack", "tray", "replacement basket"),
        "negative_signals": ("liner", "paper", "rack", "tray", "replacement basket", "toaster oven"),
        "search_alias_templates": (
            "{raw}",
            "{brand} air fryer",
        ),
        "hard_block": re.compile(
            r"\bliner\b|\bpaper\b|\brack\b|\btray\b|\breplacement\s*basket\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("liner", "paper", "rack", "tray", "basket"),
        "brand_plus_family_named": True,
    },
    {
        "id": "standing_desk",
        "match_query": re.compile(
            r"\bstanding\s*desk\b|\bsit[- ]?stand\s*desk\b|\bheight\s*adjustable\s*desk\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\bstanding\s*desk\b|\bsit[- ]?stand\s*desk\b|\belectric\s*desk\b|"
            r"\bheight\s*adjustable\s*desk\b",
            re.I,
        ),
        "brand_tokens": ("flexispot", "fezibo", "vari", "uplift", "branch", "huanuo"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("standing desk", "sit stand desk", "electric desk", "height adjustable desk"),
        "accessory_signals": (
            "converter", "frame", "desk frame", "leg", "monitor arm",
            "keyboard tray", "caster", "drawer",
        ),
        "negative_signals": (
            "converter", "frame", "desk frame", "monitor arm",
            "keyboard tray", "caster", "drawer",
        ),
        "search_alias_templates": (
            "{brand} standing desk {hard_variant_tokens}",
            "{raw}",
        ),
        "hard_block": re.compile(
            r"\bconverter\b|\bdesk\s*frame\b|\bmonitor\s*arm\b|\bkeyboard\s*tray\b|"
            r"\bcasters?\b|\bdrawer\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": (
            "converter", "frame", "monitor arm", "keyboard tray", "caster", "drawer",
        ),
        "brand_plus_family_named": True,
    },
    {
        "id": "tv",
        "match_query": re.compile(
            r"\btv\b|\btelevision\b|\boled\s*tv\b|\bqled\b|\b4k\s*tv\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\btv\b|\btelevision\b|\boled\b|\bqled\b|\b4k\b",
            re.I,
        ),
        "brand_tokens": (
            "samsung", "lg", "sony", "tcl", "hisense", "vizio", "panasonic",
        ),
        "hard_block": re.compile(r"\bstreaming\s*stick\b|\bfire\s*tv\b", re.I),
        "other_brand_earbuds": None,
        "category_accessory_words": ("mount", "stand", "remote"),
    },
    {
        "id": "mouse",
        "match_query": re.compile(
            r"\bmouse\b|\bmice\b|\bgaming\s*mouse\b",
            re.I,
        ),
        "title_core": re.compile(r"\bmouse\b|\bmice\b", re.I),
        "brand_tokens": (
            "logitech", "razer", "corsair", "steelseries", "glorious", "zalman",
        ),
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": ("pad", "feet", "skates"),
    },
    {
        "id": "keyboard",
        "match_query": re.compile(
            r"\bkeyboard\b|\bmechanical\s*keyboard\b|\btkl\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\bkeyboard\b|\bmechanical\b|\btkl\b|\btenkeyless\b",
            re.I,
        ),
        "brand_tokens": (
            "logitech", "corsair", "razer", "keychron", "ducky", "steelseries",
        ),
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": (
            "keycap", "switch", "wrist rest", "foam", "cable",
        ),
    },
    {
        "id": "laptop",
        "match_query": re.compile(
            r"\blaptop\b|\bnotebook\b|\bchromebook\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\blaptop\b|\bnotebook\b|\bchromebook\b",
            re.I,
        ),
        "brand_tokens": (
            "dell", "hp", "lenovo", "asus", "acer", "msi", "razer", "samsung",
        ),
        "hard_block": re.compile(r"\bmacbook\b|\bipad\b", re.I),
        "other_brand_earbuds": None,
        "category_accessory_words": (
            "sleeve", "bag", "stand", "cooler", "charger", "skin", "cover",
        ),
    },
    {
        "id": "camera",
        "match_query": re.compile(
            r"\bcamera\b|\bdslr\b|\bmirrorless\b",
            re.I,
        ),
        "title_core": re.compile(r"\bcamera\b|\bdslr\b|\bmirrorless\b", re.I),
        "brand_tokens": (
            "canon", "nikon", "sony", "fujifilm", "olympus", "panasonic", "gopro",
        ),
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": (
            "bag", "strap", "tripod", "lens cap", "filter", "battery",
        ),
    },
    {
        "id": "smartwatch",
        "match_query": re.compile(
            r"\bsmartwatch\b|\bsmart\s*watch\b|\bgalaxy\s*watch\b|\bpixel\s*watch\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\bsmartwatch\b|\bsmart\s*watch\b|\bgalaxy\s*watch\b|\bpixel\s*watch\b",
            re.I,
        ),
        "brand_tokens": (
            "samsung", "google", "garmin", "fitbit", "fossil",
        ),
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": ("band", "strap", "charger", "case"),
    },
    {
        "id": "speaker",
        "match_query": re.compile(
            r"\bspeaker\b|\bsoundbar\b|\bbluetooth\s*speaker\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\bspeaker\b|\bsoundbar\b",
            re.I,
        ),
        "brand_tokens": (
            "bose", "sony", "jbl", "ultimate ears", "harman", "marshall",
        ),
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": ("stand", "mount", "cable"),
    },
    {
        "id": "tablet",
        "match_query": re.compile(
            r"\btablet\b|\bgalaxy\s*tab\b|\bfire\s*tablet\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\btablet\b|\bgalaxy\s*tab\b|\bfire\s*tablet\b",
            re.I,
        ),
        "brand_tokens": ("samsung", "amazon", "lenovo", "xiaomi"),
        "hard_block": re.compile(r"\bipad\b", re.I),
        "other_brand_earbuds": None,
        "category_accessory_words": ("case", "cover", "stylus", "keyboard"),
    },
    {
        "id": "router",
        "match_query": re.compile(
            r"\brouter\b|\bmesh\s*wifi\b|\bwifi\s*6\b|\bwifi\s*7\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\brouter\b|\bmesh\b|\bwifi\b",
            re.I,
        ),
        "brand_tokens": (
            "asus", "tp-link", "netgear", "linksys", "eero", "google",
        ),
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": ("cable", "adapter", "mount"),
    },
    {
        "id": "ps5",
        "match_query": re.compile(r"\bps5\b|\bplaystation\s*5\b", re.I),
        "title_core": re.compile(r"\bps5\b|\bplaystation\s*5\b", re.I),
        "brand_tokens": ("sony", "playstation"),
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": (
            "controller", "charging", "stand", "skin", "case",
        ),
    },
    {
        "id": "xbox",
        "match_query": re.compile(r"\bxbox\b", re.I),
        "title_core": re.compile(r"\bxbox\b", re.I),
        "brand_tokens": ("microsoft", "xbox"),
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": ("controller", "charger", "stand"),
    },
]


def family_defs_list() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for family in _FAMILY_DEFS:
        merged = dict(family)
        merged.setdefault("brand_policy", "exact")
        merged.setdefault("partner_brands", ())
        merged.setdefault("primary_signals", ())
        merged.setdefault("accessory_signals", ())
        merged.setdefault("negative_signals", ())
        merged.setdefault("search_alias_templates", ())
        merged.setdefault("brand_plus_family_named", False)
        out.append(merged)
    return out


def _family_phrase_present(query_norm: str, phrases: tuple[str, ...]) -> list[str]:
    found: list[str] = []
    for phrase in phrases:
        phrase_norm = normalize_user_query(phrase)
        if phrase_norm and re.search(rf"\b{re.escape(phrase_norm)}\b", query_norm, re.I):
            found.append(phrase_norm)
    return found


def _extract_brand_from_query(
    query_norm: str,
    family: dict[str, Any] | None,
) -> str | None:
    if not family:
        return None
    for brand in family.get("brand_tokens", ()):
        if re.search(rf"\b{re.escape(normalize_user_query(brand))}\b", query_norm, re.I):
            return normalize_user_query(brand)
    return None


def _extract_named_required_tokens(
    query_norm: str,
    family: dict[str, Any] | None,
    brand: str | None,
) -> tuple[str, ...]:
    if not family:
        return ()
    required: list[str] = []
    if family.get("id") == "pressure_cooker":
        for phrase in ("duo plus", "duo", "rio", "pro crisp", "vortex"):
            if re.search(rf"\b{re.escape(phrase)}\b", query_norm, re.I) and phrase not in required:
                required.append(phrase)
    required.extend(tok for tok in _extract_structured_tokens(query_norm) if tok not in required)
    if brand and family.get("brand_plus_family_named") and not required:
        family_label = family.get("id", "").replace("_", " ")
        if family_label and family_label not in required:
            required.append(family_label)
    return _dedupe_tokens(required)


def _supports_named_product(
    query_norm: str,
    family: dict[str, Any] | None,
    brand: str | None,
    required_tokens: tuple[str, ...],
) -> bool:
    if not family:
        return False
    if required_tokens:
        return True
    if brand and family.get("brand_plus_family_named"):
        return True
    if brand and family.get("id") in {"pressure_cooker", "air_fryer"}:
        return True
    return False


def match_family_from_query(query_norm: str) -> dict[str, Any] | None:
    for fam in _FAMILY_DEFS:
        if fam["match_query"].search(query_norm):
            return fam
    for fam in _FAMILY_DEFS:
        model_patterns = fam.get("model_patterns") or ()
        if not isinstance(model_patterns, (list, tuple)):
            model_patterns = (model_patterns,)
        for pattern in model_patterns:
            if pattern.search(query_norm):
                return fam
    return None


def parse_query_intent(raw_user_query: str) -> QueryIntent:
    qn = normalize_user_query(raw_user_query)
    acc = query_has_accessory_intent(qn)
    family = match_family_from_query(qn)
    brand = _extract_brand_from_query(qn, family)
    model_token = _extract_model_token(qn, family)
    required_tokens = _extract_named_required_tokens(qn, family, brand)
    soft_variant_tokens = _extract_soft_variant_tokens(qn)
    hard_variant_tokens = _extract_structured_tokens(qn)
    if family and model_token:
        qtype = QueryType.EXACT_MODEL
    elif _supports_named_product(qn, family, brand, required_tokens):
        qtype = QueryType.NAMED_PRODUCT
    else:
        qtype = QueryType.CATEGORY
    return QueryIntent(
        query_type=qtype,
        raw_normalized=qn,
        family=family,
        accessory_intent=acc,
        model_token=model_token,
        brand=brand,
        required_tokens=required_tokens,
        soft_variant_tokens=soft_variant_tokens,
        hard_variant_tokens=hard_variant_tokens,
    )


def compute_trust_score(title: str, product_url: str | None) -> float:
    tl = (title or "").strip()
    if len(tl) < 8:
        return 0.15
    if len(tl) > 320:
        return 0.55
    if re.search(r"\b(limited\s*time|free\s*shipping|click\s*here|best\s*price)\b", tl, re.I):
        return 0.45
    wc = len(tl.split())
    if wc < 3:
        return 0.35
    alpha = sum(1 for c in tl if c.isalpha())
    if alpha / max(len(tl), 1) < 0.4:
        return 0.4
    score = 0.72
    if product_url and ("walmart.com" in product_url or "ebay.com" in product_url):
        score -= 0.05
    return min(1.0, max(0.0, score))


def compute_identity_match(
    intent: QueryIntent,
    *,
    has_core: bool,
    product_kind: str,
    model_mismatch_exact: bool,
    brand_in_title: bool,
) -> float:
    qt = intent.query_type
    if product_kind == "other_brand":
        return 0.05
    if model_mismatch_exact and qt == QueryType.EXACT_MODEL:
        return 0.08
    if qt == QueryType.CATEGORY:
        if has_core or brand_in_title:
            return 0.72
        return 0.35
    if qt in (QueryType.NAMED_PRODUCT, QueryType.PRODUCT_LINE):
        if has_core and brand_in_title:
            return 0.9
        if has_core:
            return 0.68
        return 0.35
    # EXACT_MODEL
    if model_mismatch_exact:
        return 0.12
    if has_core and brand_in_title:
        return 0.95
    if has_core:
        return 0.78
    return 0.28


def compute_confidence(
    intent: QueryIntent,
    *,
    has_core: bool,
    product_kind: str,
    trust: float,
    identity_match: float,
) -> float:
    base = 0.35 + (0.25 if has_core else 0) + (0.2 if product_kind == "primary_product" else 0)
    if intent.family:
        base += 0.1
    c = (base + trust + identity_match) / 3.0
    return round(min(1.0, max(0.0, c)), 3)


def classify_with_intent(
    title: str,
    product_url: str | None,
    intent: QueryIntent,
) -> dict[str, Any]:
    """Full classification: legacy product_kind + listing_role + scores."""
    tl = (title or "").lower()
    cond = merge_condition_title_url(title or "", product_url)
    family = intent.family
    accessory_intent = intent.accessory_intent

    listing_role = "primary_product"
    product_kind = "primary_product"
    structural_relevance = 0.45
    penalty_total = 0.0

    trust = compute_trust_score(title or "", product_url)
    model_mismatch = False

    has_compat = bool(_COMPAT_PHRASES.search(tl))
    has_bundle = bool(_BUNDLE_WORDS.search(tl))
    extra_acc = ()
    if family:
        extra_acc = family.get("category_accessory_words") or ()

    def _extra_accessory() -> bool:
        return any(
            re.search(rf"\b{re.escape(w)}\b", tl, re.I) for w in extra_acc
        )

    weak_accessory = bool(_ACCESSORY_WORDS.search(tl)) or _extra_accessory()

    if not family:
        if (
            not accessory_intent
            and weak_accessory
            and (has_compat or _extra_accessory())
        ):
            product_kind = "accessory"
            listing_role = "accessory"
            penalty_total += 0.35
        elif not accessory_intent and weak_accessory and not has_core_generic(tl):
            product_kind = "accessory"
            listing_role = "accessory"
            penalty_total += 0.4
        if cond not in ("new",):
            penalty_total += 0.05
        structural_relevance = 0.55
        identity = compute_identity_match(
            intent,
            has_core=not (product_kind == "accessory"),
            product_kind=product_kind,
            model_mismatch_exact=False,
            brand_in_title=_brand_match_generic(tl, intent),
        )
        conf = compute_confidence(
            intent,
            has_core=identity > 0.5,
            product_kind=product_kind,
            trust=trust,
            identity_match=identity,
        )
        return {
            "product_kind": product_kind,
            "listing_role": listing_role,
            "condition_class": cond,
            "structural_relevance": structural_relevance,
            "penalty_total": min(1.0, penalty_total),
            "identity_match": round(identity, 3),
            "trust_score": round(trust, 3),
            "confidence": conf,
            "query_type": intent.query_type.value,
        }

    fam_id = family["id"]
    core_rx = family["title_core"]
    brand_expect = family["brand_tokens"]

    has_core = bool(core_rx.search(tl))
    hard = family.get("hard_block")
    if hard and hard.search(tl):
        product_kind = "other_brand"
        listing_role = "different_type"
        penalty_total += 0.85
        structural_relevance = 0.08
        identity = compute_identity_match(
            intent,
            has_core=False,
            product_kind=product_kind,
            model_mismatch_exact=False,
            brand_in_title=False,
        )
        conf = compute_confidence(
            intent, has_core=False, product_kind=product_kind,
            trust=trust * 0.5, identity_match=identity,
        )
        return {
            "product_kind": product_kind,
            "listing_role": listing_role,
            "condition_class": cond,
            "structural_relevance": structural_relevance,
            "penalty_total": min(1.0, penalty_total),
            "identity_match": round(identity, 3),
            "trust_score": round(trust, 3),
            "confidence": conf,
            "query_type": intent.query_type.value,
        }

    ob = family.get("other_brand_earbuds")
    if ob and ob.search(tl) and not core_rx.search(tl):
        product_kind = "other_brand"
        listing_role = "different_brand"
        penalty_total += 0.9
        structural_relevance = 0.1
        identity = compute_identity_match(
            intent, has_core=False, product_kind=product_kind,
            model_mismatch_exact=False, brand_in_title=False,
        )
        conf = compute_confidence(
            intent, has_core=False, product_kind=product_kind,
            trust=trust * 0.6, identity_match=identity,
        )
        return {
            "product_kind": product_kind,
            "listing_role": listing_role,
            "condition_class": cond,
            "structural_relevance": structural_relevance,
            "penalty_total": min(1.0, penalty_total),
            "identity_match": round(identity, 3),
            "trust_score": round(trust, 3),
            "confidence": conf,
            "query_type": intent.query_type.value,
        }

    if intent.query_type == QueryType.EXACT_MODEL and intent.model_token:
        model_mismatch = _model_mismatch_exact(
            intent.query_type, intent.model_token, tl,
        )
        if model_mismatch:
            product_kind = "other_brand"
            listing_role = "different_model"
            penalty_total += 0.88
            structural_relevance = 0.1
            identity = compute_identity_match(
                intent, has_core=has_core, product_kind=product_kind,
                model_mismatch_exact=True, brand_in_title=any(b in tl for b in brand_expect),
            )
            conf = compute_confidence(
                intent, has_core=has_core, product_kind=product_kind,
                trust=trust * 0.55, identity_match=identity,
            )
            return {
                "product_kind": product_kind,
                "listing_role": listing_role,
                "condition_class": cond,
                "structural_relevance": structural_relevance,
                "penalty_total": min(1.0, penalty_total),
                "identity_match": round(identity, 3),
                "trust_score": round(trust, 3),
                "confidence": conf,
                "query_type": intent.query_type.value,
            }

    if not has_core:
        if fam_id == "airpods" and "beats" in tl and "airpod" not in tl:
            product_kind = "other_brand"
            listing_role = "different_brand"
            penalty_total += 0.85
            structural_relevance = 0.12
        else:
            product_kind = "compatible_generic"
            listing_role = "accessory" if has_compat or weak_accessory else "compatible_generic"
            penalty_total += 0.5
            structural_relevance = 0.2
        identity = compute_identity_match(
            intent,
            has_core=False,
            product_kind=product_kind,
            model_mismatch_exact=False,
            brand_in_title=any(b in tl for b in brand_expect),
        )
        conf = compute_confidence(
            intent, has_core=False, product_kind=product_kind,
            trust=trust, identity_match=identity,
        )
        return {
            "product_kind": product_kind,
            "listing_role": listing_role,
            "condition_class": cond,
            "structural_relevance": max(0.08, structural_relevance),
            "penalty_total": min(1.0, penalty_total),
            "identity_match": round(identity, 3),
            "trust_score": round(trust, 3),
            "confidence": conf,
            "query_type": intent.query_type.value,
        }

    struct = 0.55
    brand_in = any(b in tl for b in brand_expect)
    if brand_in:
        struct += 0.28
    elif fam_id == "airpods":
        struct += 0.18

    if has_bundle:
        listing_role = "bundle"
        penalty_total += 0.15

    if has_compat:
        product_kind = "compatible_generic"
        listing_role = "accessory"
        penalty_total += 0.55
        struct -= 0.35

    if fam_id == "airpods":
        tl_acc = _strip_bundle_case_phrases(tl)
    elif fam_id in ("apple_watch", "smartwatch"):
        tl_acc = _strip_watch_primary_phrases(tl)
    else:
        tl_acc = tl
    if not accessory_intent and _ACCESSORY_WORDS.search(tl_acc):
        product_kind = "accessory"
        listing_role = "accessory"
        penalty_total += 0.65
        struct -= 0.4

    if fam_id == "airpods" and re.search(r"\bairtag\b", tl):
        product_kind = "other_brand"
        listing_role = "different_type"
        penalty_total += 0.95
        struct = 0.05

    structural_relevance = max(0.0, min(1.0, struct))
    penalty_total = max(0.0, min(1.0, penalty_total))

    identity = compute_identity_match(
        intent,
        has_core=True,
        product_kind=product_kind,
        model_mismatch_exact=False,
        brand_in_title=brand_in,
    )
    conf = compute_confidence(
        intent,
        has_core=True,
        product_kind=product_kind,
        trust=trust,
        identity_match=identity,
    )

    return {
        "product_kind": product_kind,
        "listing_role": listing_role,
        "condition_class": cond,
        "structural_relevance": structural_relevance,
        "penalty_total": penalty_total,
        "identity_match": round(identity, 3),
        "trust_score": round(trust, 3),
        "confidence": conf,
        "query_type": intent.query_type.value,
    }


def has_core_generic(title_lower: str) -> bool:
    """Loose product signal for category-mode queries."""
    return bool(
        re.search(
            r"\b(phone|tablet|laptop|headphone|headset|monitor|keyboard|mouse|"
            r"camera|speaker|router|console|watch|tv\b|gpu|graphics|desk|"
            r"pressure\s*cooker|multicooker|air\s*fryer)",
            title_lower,
            re.I,
        )
    )


def _brand_match_generic(title_lower: str, intent: QueryIntent) -> bool:
    if intent.family:
        return any(b in title_lower for b in intent.family["brand_tokens"])
    return has_core_generic(title_lower)


def identity_threshold_for_query(intent: QueryIntent) -> float:
    if intent.query_type == QueryType.EXACT_MODEL:
        return 0.5
    if intent.query_type in (QueryType.NAMED_PRODUCT, QueryType.PRODUCT_LINE):
        return 0.35
    return 0.2


def passes_identity_gate(row: dict[str, Any], intent: QueryIntent) -> bool:
    thr = identity_threshold_for_query(intent)
    im = float(row.get("identity_match", 0.0))
    if im < thr:
        logging.debug(
            f"[eligibility] EXCLUDE identity_match={im:.2f} < {thr:.2f} "
            f"qt={intent.query_type.value}"
        )
        return False
    role = row.get("listing_role", "primary_product")
    if role == "suspicious":
        return False
    if intent.query_type == QueryType.EXACT_MODEL:
        if role in ("different_model", "different_type"):
            logging.debug("[eligibility] EXCLUDE exact model + wrong role %r", role)
            return False
    return True


def passes_confidence_gate(row: dict[str, Any], min_confidence: float) -> bool:
    c = float(row.get("confidence", 0.0))
    if c < min_confidence:
        logging.debug(
            f"[eligibility] EXCLUDE confidence={c:.2f} < {min_confidence:.2f}"
        )
        return False
    return True
