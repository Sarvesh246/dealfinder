"""Query normalization, condition detection, and low-level identity helpers."""

from __future__ import annotations

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
    re.compile(r"\bmx\s*master\s*3s?\b", re.I),
    re.compile(
        r"\biphone\s*(?:\d{2})\s*(?:pro\s*max|pro|plus|mini)?\b", re.I
    ),
    re.compile(r"\b(?:galaxy|pixel)\s*[sz]\s*\d{2,3}\b", re.I),
    re.compile(r"\bairpods?\s*(?:pro|max)?\s*(?:2|3|4)?\b", re.I),
    re.compile(r"\bmp[ol]\s*-\s*\d{2,4}\b", re.I),
    re.compile(r"\bhl[- ]?l\d{4}[a-z]{1,3}\b", re.I),
    re.compile(r"\bmfc[- ]?l\d{4}[a-z]{1,3}\b", re.I),
    re.compile(r"\bdcp[- ]?l\d{4}[a-z]{1,3}\b", re.I),
    re.compile(r"\bk[- ]?(?:express|mini|elite|compact|supreme)\b", re.I),
    re.compile(r"\b(?:archer\s*)?(?:axe|ax|be|ac)\s*\d{1,4}\b", re.I),
    re.compile(r"\bv(?:8|10|11|12|15)\b", re.I),
    re.compile(r"\b(?:9[78]0|9[89]0)\s*(?:pro|evo)\b", re.I),
    re.compile(r"\bsn\s*\d{3,4}x?\b", re.I),
    re.compile(r"\b737\b", re.I),
    re.compile(r"\bsteam\s*deck\s*oled\b|\bsteam\s*deck\b", re.I),
    re.compile(r"\bmeta\s*quest\s*3s?\b|\bquest\s*3s?\b", re.I),
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
    min_len = 2 if family else 3
    for p in patterns:
        m = p.search(q)
        if m:
            tok = _norm_model(m.group(0))
            if len(tok) >= min_len and tok not in seen:
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


