"""
Strict product-spec parsing and PDP identity verification.

Pipeline role:
1. Convert a query or saved tracker into a ProductSpec with exact/named/category semantics.
2. Build listing fingerprints from PDP titles, URLs, and page text.
3. Decide whether a candidate page is verified, ambiguous, related, or rejected.

This module is the hard quality gate after product_identity.py and
discovery_filters.py have already classified broad catalog intent. It should be
conservative: uncertain matches are downgraded or rejected here rather than
silently accepted into tracking or ranking.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup

from product_identity import (
    QueryType,
    family_defs_list,
    normalize_user_query,
    parse_query_intent,
)


@dataclass(frozen=True)
class ProductSpec:
    raw_query: str
    canonical_query: str
    brand: str | None
    family: str | None
    model_token: str | None
    normalized_model: str | None
    variant_tokens: tuple[str, ...]
    required_tokens: tuple[str, ...] = ()
    soft_variant_tokens: tuple[str, ...] = ()
    hard_variant_tokens: tuple[str, ...] = ()
    search_aliases: tuple[str, ...] = ()
    match_mode: str = "strict"
    query_type: str = QueryType.CATEGORY.value


@dataclass(frozen=True)
class ListingFingerprint:
    url: str
    domain: str
    title: str
    brand: str | None
    family: str | None
    model_tokens: tuple[str, ...]
    normalized_model_tokens: tuple[str, ...]
    variant_tokens: tuple[str, ...]
    current_price: float | None
    accessory_signal: bool
    compatibility_signal: bool
    bundle_signal: bool
    hard_block_signal: bool
    raw_text: str


@dataclass(frozen=True)
class VerificationResult:
    status: str
    reason: str
    health_state: str
    product_name: str
    current_price: float | None
    brand: str | None
    family: str | None
    model_token: str | None
    match_label: str
    fingerprint: ListingFingerprint


_FAMILY_DEFS = family_defs_list()
_FAMILY_BY_ID = {fam["id"]: fam for fam in _FAMILY_DEFS}

_MODEL_BRAND_HINTS: tuple[tuple[re.Pattern[str], str, str], ...] = (
    (re.compile(r"\bwh[- ]?1000xm\d\b", re.I), "headphones", "sony"),
    (re.compile(r"\bwf[- ]?1000xm\d\b", re.I), "airpods", "sony"),
    (re.compile(r"\bqc(?:\s*ultra|\s*\d{2,3})\b", re.I), "headphones", "bose"),
    (re.compile(r"\bairpods?\s*(?:pro|max)?\s*(?:2|3|4)?\b", re.I), "airpods", "apple"),
    (re.compile(r"\biphone\s*\d{2}\b", re.I), "iphone", "apple"),
    (re.compile(r"\bipad\b", re.I), "ipad", "apple"),
    (re.compile(r"\bmac\s*book\b|\bmacbook\b", re.I), "macbook", "apple"),
    (re.compile(r"\bps5\b|\bplaystation\s*5\b", re.I), "ps5", "sony"),
    (re.compile(r"\bxbox\b", re.I), "xbox", "microsoft"),
    (re.compile(r"\bnintendo\s*switch\s*(?:2|lite|oled)\b|\bswitch\s*(?:2|lite|oled)\b", re.I), "nintendo_switch", "nintendo"),
    (re.compile(r"\bkindle\s+paperwhite\b", re.I), "kindle_paperwhite", "amazon"),
    (re.compile(r"\broku\s+ultra\b", re.I), "roku_ultra", "roku"),
    (re.compile(r"\brtx\s*\d{3,4}\s*(?:ti|super)?\b", re.I), "gpu", "nvidia"),
    (re.compile(r"\brx\s*\d{3,4}\s*(?:xt)?\b", re.I), "gpu", "amd"),
    (re.compile(r"\bgalaxy\s*[sz]\s*\d{2,3}\b", re.I), "phone", "samsung"),
    (re.compile(r"\bpixel\s*[89]\b", re.I), "phone", "google"),
    (re.compile(r"\bmx\s*master\s*3s?\b", re.I), "mouse", "logitech"),
    (re.compile(r"\bhl[- ]?l\d{4}[a-z]{1,3}\b", re.I), "printer", "brother"),
    (re.compile(r"\bmfc[- ]?l\d{4}[a-z]{1,3}\b", re.I), "printer", "brother"),
    (re.compile(r"\bdcp[- ]?l\d{4}[a-z]{1,3}\b", re.I), "printer", "brother"),
    (re.compile(r"\bk[- ]?(?:express|mini|elite|compact|supreme)\b", re.I), "coffee_maker", "keurig"),
    (re.compile(r"\b(?:archer\s*)?(?:axe|ax|be|ac)\s*\d{1,4}\b", re.I), "router", "tp-link"),
    (re.compile(r"\banker\s*737\b|\b737\s*power\s*bank\b", re.I), "power_bank", "anker"),
    (re.compile(r"\blego\s+orchid\b", re.I), "building_set", "lego"),
    (re.compile(r"\bdyson\s*v(?:8|10|11|12|15)\b", re.I), "vacuum", "dyson"),
    (re.compile(r"\b(?:9[78]0|9[89]0)\s*(?:pro|evo)\b", re.I), "storage", "samsung"),
    (re.compile(r"\bsn\s*\d{3,4}x?\b", re.I), "storage", "wd"),
    (re.compile(r"\bsteam\s*deck\s*oled\b|\bsteam\s*deck\b", re.I), "steam_deck", "valve"),
    (re.compile(r"\bmeta\s*quest\s*3s?\b|\bquest\s*3s?\b", re.I), "meta_quest", "meta"),
    (re.compile(r"\baeron\b", re.I), "office_chair", "herman miller"),
    (re.compile(r"\bleap\s*v?2\b|\bgesture\b", re.I), "office_chair", "steelcase"),
)

_GENERIC_ACCESSORY = re.compile(
    r"\b("
    r"replacement\s+case|protective\s+case|silicone\s+case|case\s+cover|"
    r"cover|skin|sleeve|replacement|ear\s*pads?|earpads?|ear\s*cushions?|"
    r"cushions?|foam|tips?|eartips?|hook|hooks|strap|straps|stand|holder|"
    r"adapter|adapters|charger|cable|dock|kit|mount|grip|shell|"
    r"protector|tripod|lens\s*cap|remote|riser|bracket"
    r")\b",
    re.I,
)
_COMPATIBILITY = re.compile(
    r"\b("
    r"compatible\s+with|works\s+with|fits\b|designed\s+for|for\s+sony\b|"
    r"replacement\s+for|for\s+apple\b|for\s+samsung\b|for\s+iphone\b|"
    r"for\s+airpods?\b|for\s+ps5\b|alternative\s+to|similar\s+to"
    r")\b",
    re.I,
)
_BUNDLE = re.compile(
    r"\b(bundle|combo|kit\b|set\s+of\s+\d|pack\s+of\s+\d|\d[- ]?\s*pack)\b",
    re.I,
)
_COLOR_TOKENS = (
    "black", "white", "silver", "blue", "red", "green", "pink",
    "gray", "grey", "midnight", "starlight", "purple", "gold",
    "graphite", "ivory", "navy", "beige", "orange", "yellow",
)
_VARIANT_PATTERNS = (
    re.compile(r"\b\d{2,4}\s*(?:gb|tb)\b", re.I),
    re.compile(r"\b\d+(?:\.\d+)?\s*(?:inch|in)\b", re.I),
    re.compile(r"\b(?:gen|generation)\s*\d+\b", re.I),
    re.compile(r"\b\d+(?:st|nd|rd|th)\s+gen\b", re.I),
)
_STRUCTURED_TOKEN_PATTERNS = (
    re.compile(r"\b\d+in\d+\b", re.I),
    re.compile(r"\b\d{2,3}mm\b", re.I),
    re.compile(r"\b\d+(?:\.\d+)?qt\b", re.I),
    re.compile(r"\b\d{2,3}x\d{2,3}\b", re.I),
)
_TITLE_SELECTORS = (
    "h1",
    '[data-automation="product-title"]',
    '[data-test="product-title"]',
    '[data-testid*="product-title"]',
    '[data-testid*="title"]',
    '[itemprop="name"]',
)
_BRAND_SELECTORS = (
    '[itemprop="brand"]',
    '[data-testid*="brand"]',
    '[data-test*="brand"]',
)
_META_TITLE_KEYS = ("og:title", "twitter:title")
_META_DESC_KEYS = ("description", "og:description")
_FAMILY_QUERY_TERM = {
    "headphones": "headphones",
    "monitor": "monitor",
    "gpu": "graphics card",
    "tv": "tv",
    "mouse": "mouse",
    "keyboard": "keyboard",
    "laptop": "laptop",
    "camera": "camera",
    "speaker": "speaker",
    "phone": "smartphone",
    "iphone": "iphone",
    "macbook": "macbook",
    "ipad": "ipad",
    "airpods": "earbuds",
    "air_fryer": "air fryer",
    "pressure_cooker": "pressure cooker",
    "standing_desk": "standing desk",
    "printer": "printer",
    "router": "router",
    "coffee_maker": "coffee maker",
    "power_bank": "power bank",
    "building_set": "building set",
    "vacuum": "vacuum",
    "storage": "ssd",
    "ps5": "console",
    "xbox": "console",
    "nintendo_switch": "console",
    "kindle_paperwhite": "ereader",
    "roku_ultra": "streaming player",
    "steam_deck": "handheld",
    "meta_quest": "vr headset",
    "office_chair": "office chair",
}


def _norm_token(value: str | None) -> str | None:
    if not value:
        return None
    out = re.sub(r"[^a-z0-9]+", "", value.lower())
    return out or None


def _normalize_text_blob(value: str | None) -> str:
    text = unquote(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[_/|]+", " ", text)
    text = re.sub(r"[^a-zA-Z0-9.+\- ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def _contains_model_phrase(text: str | None, model_token: str | None) -> bool:
    if not text or not model_token:
        return False
    parts = [re.escape(part) for part in re.split(r"[\s\-_/()[\]]+", model_token) if part]
    if not parts:
        return False
    pattern = r"(?<![a-z0-9])" + r"[\s\-_/()[\]]*".join(parts) + r"(?![a-z0-9])"
    return bool(re.search(pattern, text, re.I))


def _serialize_variants(variants: tuple[str, ...]) -> str:
    return json.dumps(list(variants))


def deserialize_variant_tokens(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return ()
    if not isinstance(parsed, list):
        return ()
    out: list[str] = []
    for item in parsed:
        item_s = str(item).strip().lower()
        if item_s and item_s not in out:
            out.append(item_s)
    return tuple(out)


def _dedupe(values: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    seen: list[str] = []
    for value in values:
        token = normalize_user_query(value).strip()
        if token and token not in seen:
            seen.append(token)
    return tuple(seen)


def _append_canonical_part(parts: list[str], value: str | None) -> None:
    token = normalize_user_query(value or "").strip()
    if not token:
        return
    remove_parts: list[str] = []
    for part in parts:
        item = normalize_user_query(part).strip()
        if not item:
            continue
        if token == item or token in item:
            return
        if item in token:
            remove_parts.append(part)
    for part in remove_parts:
        if part in parts:
            parts.remove(part)
    parts.append(value.strip())


def _extract_structured_tokens(text: str) -> tuple[str, ...]:
    normalized = normalize_user_query(text)
    found: list[str] = []
    for pattern in _STRUCTURED_TOKEN_PATTERNS:
        for match in pattern.finditer(normalized):
            token = normalize_user_query(match.group(0))
            if token not in found:
                found.append(token)
    return tuple(found)


def _contains_token_phrase(text: str | None, token: str) -> bool:
    if not text or not token:
        return False
    parts = [re.escape(part) for part in re.split(r"[\s\-_/()[\]]+", token) if part]
    if not parts:
        return False
    pattern = r"(?<![a-z0-9])" + r"[\s\-_/()[\]]*".join(parts) + r"(?![a-z0-9])"
    return bool(re.search(pattern, text, re.I))


def _has_family_signal(text: str | None, phrases: tuple[str, ...]) -> bool:
    if not text or not phrases:
        return False
    for phrase in phrases:
        if _contains_token_phrase(text, normalize_user_query(phrase)):
            return True
    return False


def _format_alias_template(template: str, spec: "ProductSpec", family_term: str | None) -> str:
    values = {
        "raw": spec.raw_query,
        "brand": spec.brand or "",
        "model_token": spec.model_token or "",
        "family_term": family_term or "",
        "required_tokens": " ".join(spec.required_tokens),
        "hard_variant_tokens": " ".join(spec.hard_variant_tokens),
    }
    alias = template.format(**values)
    return re.sub(r"\s+", " ", alias).strip()


def product_spec_to_fields(spec: ProductSpec) -> dict[str, Any]:
    return {
        "raw_query": spec.raw_query,
        "canonical_query": spec.canonical_query,
        "brand": spec.brand,
        "family": spec.family,
        "model_token": spec.model_token,
        "variant_tokens": _serialize_variants(spec.variant_tokens),
        "match_mode": spec.match_mode,
        "query_type": spec.query_type,
    }


def product_spec_from_row(row: Any) -> ProductSpec:
    data = dict(row) if row is not None else {}
    raw_query = (data.get("raw_query") or data.get("name") or "").strip()
    if not raw_query:
        raw_query = str(data.get("canonical_query") or "").strip()
    spec = parse_product_spec(raw_query)
    return ProductSpec(
        raw_query=raw_query,
        canonical_query=(data.get("canonical_query") or spec.canonical_query),
        brand=(data.get("brand") or spec.brand),
        family=(data.get("family") or spec.family),
        model_token=(data.get("model_token") or spec.model_token),
        normalized_model=_norm_token(data.get("model_token") or spec.model_token),
        variant_tokens=deserialize_variant_tokens(data.get("variant_tokens")) or spec.variant_tokens,
        required_tokens=spec.required_tokens,
        soft_variant_tokens=spec.soft_variant_tokens,
        hard_variant_tokens=spec.hard_variant_tokens,
        search_aliases=spec.search_aliases,
        match_mode=(data.get("match_mode") or spec.match_mode or "strict"),
        query_type=(data.get("query_type") or spec.query_type),
    )


def _infer_brand_from_query(query_norm: str, family: dict[str, Any] | None) -> str | None:
    if family:
        for brand in family.get("brand_tokens", ()):
            if re.search(rf"\b{re.escape(brand)}\b", query_norm, re.I):
                return brand.lower()
    for pattern, _family_id, brand in _MODEL_BRAND_HINTS:
        if pattern.search(query_norm):
            return brand
    return None


def _infer_family_id_from_query(query_norm: str, current_family: dict[str, Any] | None) -> str | None:
    if current_family:
        return current_family["id"]
    for pattern, family_id, _brand in _MODEL_BRAND_HINTS:
        if pattern.search(query_norm):
            return family_id
    return None


def _extract_variant_tokens(text: str) -> tuple[str, ...]:
    found: list[str] = []
    tl = normalize_user_query(text)
    for color in _COLOR_TOKENS:
        if re.search(rf"\b{re.escape(color)}\b", tl):
            found.append(color)
    for pattern in _VARIANT_PATTERNS:
        for match in pattern.finditer(tl):
            token = match.group(0).strip().lower()
            if token not in found:
                found.append(token)
    return tuple(found)


def parse_product_spec(raw_query: str) -> ProductSpec:
    intent = parse_query_intent(raw_query)
    raw_clean = raw_query.strip()
    qn = normalize_user_query(raw_query)
    family_id = _infer_family_id_from_query(qn, intent.family)
    family = _FAMILY_BY_ID.get(family_id) if family_id else intent.family
    brand = intent.brand or _infer_brand_from_query(qn, family)
    model_token = intent.model_token
    required_tokens = tuple(intent.required_tokens or ())
    hard_variant_tokens = tuple(intent.hard_variant_tokens or ())
    soft_variant_tokens = tuple(intent.soft_variant_tokens or ())
    canonical_parts: list[str] = []
    if brand:
        _append_canonical_part(canonical_parts, brand)
    if model_token:
        _append_canonical_part(canonical_parts, model_token)
    elif required_tokens:
        for required in required_tokens:
            _append_canonical_part(canonical_parts, required)
    elif raw_clean:
        raw_norm = normalize_user_query(raw_clean)
        existing = normalize_user_query(" ".join(canonical_parts))
        if raw_norm and (not existing or existing not in raw_norm):
            _append_canonical_part(canonical_parts, raw_clean)
    family_term = _FAMILY_QUERY_TERM.get(family_id or "")
    if family_term and family_term.lower() not in " ".join(canonical_parts).lower():
        _append_canonical_part(canonical_parts, family_term)
    canonical_query = re.sub(r"\s+", " ", " ".join(canonical_parts)).strip() or raw_query.strip()
    alias_values: list[str] = [canonical_query]
    if raw_clean and raw_clean.lower() != canonical_query.lower():
        alias_values.append(raw_clean)
    if brand and model_token:
        model_norm = normalize_user_query(model_token)
        brand_norm = normalize_user_query(brand)
        if not model_norm.startswith(brand_norm):
            alias_values.append(f"{brand} {model_token}")
    if family:
        for template in family.get("search_alias_templates", ()):
            if intent.query_type == QueryType.EXACT_MODEL and "{model_token}" not in template and "{raw}" not in template:
                continue
            alias = _format_alias_template(template, ProductSpec(
                raw_query=raw_clean,
                canonical_query=canonical_query,
                brand=brand,
                family=family_id,
                model_token=model_token,
                normalized_model=_norm_token(model_token),
                variant_tokens=soft_variant_tokens,
                required_tokens=required_tokens,
                soft_variant_tokens=soft_variant_tokens,
                hard_variant_tokens=hard_variant_tokens,
                search_aliases=(),
                match_mode="strict",
                query_type=intent.query_type.value,
            ), family_term)
            if alias:
                alias_values.append(alias)
    search_aliases = _dedupe(alias_values)
    return ProductSpec(
        raw_query=raw_clean,
        canonical_query=canonical_query,
        brand=brand,
        family=family_id,
        model_token=model_token,
        normalized_model=_norm_token(model_token),
        variant_tokens=soft_variant_tokens or _extract_variant_tokens(qn),
        required_tokens=required_tokens,
        soft_variant_tokens=soft_variant_tokens,
        hard_variant_tokens=hard_variant_tokens,
        search_aliases=search_aliases,
        match_mode="strict",
        query_type=intent.query_type.value,
    )


def _iter_json_dicts(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from _iter_json_dicts(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_json_dicts(item)


def _extract_schema_name_brand(soup: BeautifulSoup) -> tuple[str | None, str | None]:
    best_name = None
    best_brand = None
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for item in _iter_json_dicts(data):
            types = item.get("@type")
            if isinstance(types, list):
                type_text = " ".join(str(x) for x in types)
            else:
                type_text = str(types or "")
            if "product" not in type_text.lower() and "offer" in type_text.lower():
                continue
            if best_name is None:
                name = item.get("name")
                if isinstance(name, str) and name.strip():
                    best_name = name.strip()
            if best_brand is None:
                brand = item.get("brand")
                if isinstance(brand, dict):
                    brand = brand.get("name")
                if isinstance(brand, str) and brand.strip():
                    best_brand = brand.strip().lower()
            if best_name and best_brand:
                return best_name, best_brand
    return best_name, best_brand


def _meta_content(soup: BeautifulSoup, attr_name: str, value: str) -> str | None:
    tag = soup.find("meta", {attr_name: value})
    if not tag:
        return None
    content = (tag.get("content") or "").strip()
    return content or None


def _extract_title_candidates(soup: BeautifulSoup) -> list[str]:
    seen: list[str] = []
    schema_name, _ = _extract_schema_name_brand(soup)
    if schema_name:
        seen.append(schema_name)
    for key in _META_TITLE_KEYS:
        meta = _meta_content(soup, "property", key) or _meta_content(soup, "name", key)
        if meta and meta not in seen:
            seen.append(meta)
    if soup.title:
        title = soup.title.get_text(" ", strip=True)
        if title and title not in seen:
            seen.append(title)
    for selector in _TITLE_SELECTORS:
        for node in soup.select(selector):
            title = node.get_text(" ", strip=True)
            if title and len(title) >= 4 and title not in seen:
                seen.append(title)
            if len(seen) >= 8:
                return seen
    return seen


def _extract_brand(soup: BeautifulSoup, title_blob: str, family_id: str | None) -> str | None:
    _schema_name, schema_brand = _extract_schema_name_brand(soup)
    if schema_brand:
        return schema_brand.lower()
    for selector in _BRAND_SELECTORS:
        node = soup.select_one(selector)
        if node:
            text = node.get_text(" ", strip=True).lower()
            if text:
                return text
    family = _FAMILY_BY_ID.get(family_id or "")
    if family:
        for brand in family.get("brand_tokens", ()):
            if re.search(rf"\b{re.escape(brand)}\b", title_blob, re.I):
                return brand.lower()
    for _pattern, _family_id, brand in _MODEL_BRAND_HINTS:
        if re.search(rf"\b{re.escape(brand)}\b", title_blob, re.I):
            return brand
    return None


def _extract_page_text(soup: BeautifulSoup, url: str) -> str:
    pieces: list[str] = []
    for key in _META_DESC_KEYS:
        meta = _meta_content(soup, "property", key) or _meta_content(soup, "name", key)
        if meta:
            pieces.append(meta)
    body_text = soup.get_text(" ", strip=True)
    if body_text:
        pieces.append(body_text[:6000])
    slug = unquote(urlparse(url).path.replace("/", " "))
    if slug:
        pieces.append(slug)
    return _normalize_text_blob(" ".join(pieces))


def _collect_model_tokens(text: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    raw_tokens: list[str] = []
    normalized: list[str] = []
    for family in _FAMILY_DEFS:
        model_patterns = family.get("model_patterns") or ()
        if not isinstance(model_patterns, (list, tuple)):
            model_patterns = (model_patterns,)
        for pattern in model_patterns:
            for match in pattern.finditer(text):
                token = match.group(0).strip()
                norm = _norm_token(token)
                if norm and norm not in normalized:
                    raw_tokens.append(token)
                    normalized.append(norm)
    for pattern, _family_id, _brand in _MODEL_BRAND_HINTS:
        for match in pattern.finditer(text):
            token = match.group(0).strip()
            norm = _norm_token(token)
            if norm and norm not in normalized:
                raw_tokens.append(token)
                normalized.append(norm)
    return tuple(raw_tokens), tuple(normalized)


def _infer_family_from_text(text: str, family_hint: str | None) -> str | None:
    if family_hint:
        fam = _FAMILY_BY_ID.get(family_hint)
        if fam:
            if fam["title_core"].search(text):
                return family_hint
            model_patterns = fam.get("model_patterns") or ()
            if not isinstance(model_patterns, (list, tuple)):
                model_patterns = (model_patterns,)
            if any(pattern.search(text) for pattern in model_patterns):
                return family_hint
    for fam in _FAMILY_DEFS:
        if fam["title_core"].search(text):
            return fam["id"]
        model_patterns = fam.get("model_patterns") or ()
        if not isinstance(model_patterns, (list, tuple)):
            model_patterns = (model_patterns,)
        if any(pattern.search(text) for pattern in model_patterns):
            return fam["id"]
    for _pattern, family_id, _brand in _MODEL_BRAND_HINTS:
        if _pattern.search(text):
            return family_id
    return family_hint


def fingerprint_listing_document(
    url: str,
    soup: BeautifulSoup,
    *,
    current_price: float | None,
    family_hint: str | None = None,
) -> ListingFingerprint:
    titles = _extract_title_candidates(soup)
    title = titles[0] if titles else ""
    title_blob = _normalize_text_blob(" ".join(titles))
    page_text = _extract_page_text(soup, url)
    combined = _normalize_text_blob(f"{title_blob} {page_text}")
    slug_text = _normalize_text_blob(unquote(urlparse(url).path.replace("/", " ")))
    signal_text = _normalize_text_blob(f"{title_blob} {slug_text}")
    family_id = _infer_family_from_text(signal_text, family_hint)
    if not family_id and family_hint and combined != signal_text:
        family_id = _infer_family_from_text(combined, family_hint)
    family = _FAMILY_BY_ID.get(family_id or "")
    model_tokens, normalized_models = _collect_model_tokens(combined)
    accessory_signal = bool(_GENERIC_ACCESSORY.search(signal_text))
    compatibility_signal = bool(_COMPATIBILITY.search(signal_text))
    bundle_signal = bool(_BUNDLE.search(signal_text))
    hard_block_signal = bool(
        family and family.get("hard_block") and family["hard_block"].search(signal_text)
    )
    if family_id == "power_bank" and _has_family_signal(
        signal_text,
        ("power bank", "portable charger", "battery pack"),
    ):
        accessory_signal = False
    return ListingFingerprint(
        url=url,
        domain=urlparse(url).netloc.lower().replace("www.", ""),
        title=title or titles[0] if titles else url,
        brand=_extract_brand(soup, combined, family_id),
        family=family_id,
        model_tokens=model_tokens,
        normalized_model_tokens=normalized_models,
        variant_tokens=_extract_variant_tokens(combined),
        current_price=current_price,
        accessory_signal=accessory_signal,
        compatibility_signal=compatibility_signal,
        bundle_signal=bundle_signal,
        hard_block_signal=hard_block_signal,
        raw_text=combined,
    )


def fallback_listing_fingerprint(
    url: str,
    title: str,
    *,
    current_price: float | None,
    family_hint: str | None = None,
) -> ListingFingerprint:
    text = _normalize_text_blob(f"{title} {urlparse(url).path}")
    model_tokens, normalized_models = _collect_model_tokens(text)
    return ListingFingerprint(
        url=url,
        domain=urlparse(url).netloc.lower().replace("www.", ""),
        title=title or url,
        brand=None,
        family=family_hint,
        model_tokens=model_tokens,
        normalized_model_tokens=normalized_models,
        variant_tokens=_extract_variant_tokens(text),
        current_price=current_price,
        accessory_signal=False,
        compatibility_signal=False,
        bundle_signal=False,
        hard_block_signal=False,
        raw_text=text,
    )


def _mismatched_variant(spec: ProductSpec, fp: ListingFingerprint) -> bool:
    if not spec.variant_tokens:
        return False
    if not fp.variant_tokens:
        return False
    for variant in spec.variant_tokens:
        if re.search(r"\b(?:black|white|silver|blue|red|green|pink|gray|grey|midnight|starlight|purple|gold|graphite|ivory|navy|beige|orange|yellow)\b", variant):
            continue
        if variant not in fp.variant_tokens:
            return True
    return False


def _brand_mismatch(spec: ProductSpec, fp: ListingFingerprint) -> bool:
    if not spec.brand:
        return False
    family = _FAMILY_BY_ID.get(spec.family or "")
    brand_policy = family.get("brand_policy", "exact") if family else "exact"
    if fp.brand:
        fp_brand = normalize_user_query(fp.brand)
        spec_brand = normalize_user_query(spec.brand)
        if brand_policy == "platform_plus_partner":
            partners = {normalize_user_query(b) for b in family.get("partner_brands", ())}
            allowed = {spec_brand, "geforce"} | partners
            if spec_brand == "amd":
                allowed.add("radeon")
            if fp_brand not in allowed:
                return True
        elif fp_brand != spec_brand:
            return True
    if not family:
        return False
    if brand_policy == "platform_plus_partner":
        partners = {normalize_user_query(b) for b in family.get("partner_brands", ())}
        forbidden = {"amd", "radeon"} if normalize_user_query(spec.brand) in {"nvidia", "geforce"} else {"nvidia", "geforce"}
        for brand in forbidden:
            if re.search(rf"\b{re.escape(brand)}\b", fp.raw_text, re.I):
                return True
        return False
    for brand in family.get("brand_tokens", ()):
        if brand == spec.brand:
            continue
        if re.search(rf"\b{re.escape(brand)}\b", fp.raw_text, re.I):
            return True
    return False


def _primary_identity_blob(fingerprint: ListingFingerprint) -> str:
    slug = unquote(urlparse(fingerprint.url).path.replace("/", " "))
    return _normalize_text_blob(f"{fingerprint.title} {slug}")


def _required_tokens_present(text: str, tokens: tuple[str, ...]) -> bool:
    normalized_text = normalize_user_query(text)
    structured = set(_extract_structured_tokens(normalized_text))
    hard_tokens = set(_extract_hard_variant_tokens(normalized_text))
    for token in tokens:
        if token in structured or token in hard_tokens:
            continue
        if _contains_token_phrase(normalized_text, token):
            continue
        return False
    return True


def _extract_hard_variant_tokens(text: str) -> tuple[str, ...]:
    found = list(_extract_structured_tokens(text))
    normalized = normalize_user_query(text)
    for phrase in ("duo plus", "duo", "rio", "pro crisp", "super", "ti"):
        if _contains_token_phrase(normalized, phrase) and phrase not in found:
            found.append(phrase)
    return tuple(found)


def _hard_variant_conflict(spec: ProductSpec, primary_identity: str) -> bool:
    if not spec.hard_variant_tokens:
        return False
    present_tokens = _extract_hard_variant_tokens(primary_identity)
    if not present_tokens:
        return False
    for token in spec.hard_variant_tokens:
        if token.endswith("in1"):
            for present in present_tokens:
                if present.endswith("in1") and present != token:
                    return True
        elif token.endswith("qt"):
            for present in present_tokens:
                if present.endswith("qt") and present != token:
                    return True
        elif "x" in token and re.fullmatch(r"\d{2,3}x\d{2,3}", token):
            for present in present_tokens:
                if re.fullmatch(r"\d{2,3}x\d{2,3}", present) and present != token:
                    return True
        elif token.endswith("mm"):
            for present in present_tokens:
                if present.endswith("mm") and present != token:
                    return True
        elif token in {"duo", "rio"} and _contains_token_phrase(primary_identity, "duo plus"):
            return True
        elif token == "rtx 4070":
            if _contains_token_phrase(primary_identity, "rtx 4070 super") or _contains_token_phrase(primary_identity, "rtx 5070"):
                return True
        elif token not in present_tokens and _contains_token_phrase(primary_identity, token):
            continue
    return False


def verify_listing(spec: ProductSpec, fingerprint: ListingFingerprint) -> VerificationResult:
    product_name = fingerprint.title or spec.raw_query
    family = _FAMILY_BY_ID.get(spec.family or "")
    same_family = not spec.family or fingerprint.family == spec.family
    primary_identity = _primary_identity_blob(fingerprint)
    strict_exact_only = spec.family == "gpu"
    _primary_tokens, normalized_primary_models = _collect_model_tokens(primary_identity)
    exact_model_in_primary = bool(
        spec.normalized_model
        and (
            spec.normalized_model in normalized_primary_models
            or (
                not strict_exact_only
                and _contains_model_phrase(primary_identity, spec.model_token)
            )
        )
    )
    exact_model = bool(
        spec.normalized_model
        and (
            exact_model_in_primary
            or (
                not normalized_primary_models
                and (
                    spec.normalized_model in fingerprint.normalized_model_tokens
                    or (
                        not strict_exact_only
                        and _contains_model_phrase(fingerprint.title, spec.model_token)
                    )
                )
            )
        )
    )
    different_model = bool(
        spec.normalized_model
        and (
            (
                normalized_primary_models
                and spec.normalized_model not in normalized_primary_models
            )
            or (
                not normalized_primary_models
                and fingerprint.normalized_model_tokens
                and spec.normalized_model not in fingerprint.normalized_model_tokens
            )
        )
    )
    family_core_match = bool(
        family and family["title_core"].search(fingerprint.raw_text)
    ) if family else same_family
    primary_signals = tuple(family.get("primary_signals", ())) if family else ()
    negative_signals = tuple(family.get("negative_signals", ())) if family else ()
    require_primary_signal = bool(family.get("require_primary_signal")) if family else False
    primary_signal_match = _has_family_signal(primary_identity, primary_signals)
    negative_signal_match = _has_family_signal(primary_identity, negative_signals)
    if fingerprint.hard_block_signal or fingerprint.compatibility_signal:
        return VerificationResult(
            status="rejected",
            reason="compatible_or_wrong_type",
            health_state="healthy",
            product_name=product_name,
            current_price=fingerprint.current_price,
            brand=fingerprint.brand,
            family=fingerprint.family,
            model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
            match_label="related",
            fingerprint=fingerprint,
        )
    if fingerprint.accessory_signal or fingerprint.bundle_signal:
        return VerificationResult(
            status="rejected",
            reason="accessory_or_bundle",
            health_state="healthy",
            product_name=product_name,
            current_price=fingerprint.current_price,
            brand=fingerprint.brand,
            family=fingerprint.family,
            model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
            match_label="related",
            fingerprint=fingerprint,
        )
    if same_family and family_core_match and negative_signal_match and not primary_signal_match:
        return VerificationResult(
            status="rejected",
            reason="family_negative_signal",
            health_state="healthy",
            product_name=product_name,
            current_price=fingerprint.current_price,
            brand=fingerprint.brand,
            family=fingerprint.family,
            model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
            match_label="related",
            fingerprint=fingerprint,
        )
    if same_family and family_core_match and require_primary_signal and not primary_signal_match:
        return VerificationResult(
            status="rejected",
            reason="primary_signals_missing",
            health_state="healthy",
            product_name=product_name,
            current_price=fingerprint.current_price,
            brand=fingerprint.brand,
            family=fingerprint.family,
            model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
            match_label="related",
            fingerprint=fingerprint,
        )
    if not same_family and spec.family:
        return VerificationResult(
            status="rejected",
            reason="family_mismatch",
            health_state="healthy",
            product_name=product_name,
            current_price=fingerprint.current_price,
            brand=fingerprint.brand,
            family=fingerprint.family,
            model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
            match_label="related",
            fingerprint=fingerprint,
        )
    if _brand_mismatch(spec, fingerprint):
        return VerificationResult(
            status="rejected",
            reason="brand_mismatch",
            health_state="healthy",
            product_name=product_name,
            current_price=fingerprint.current_price,
            brand=fingerprint.brand,
            family=fingerprint.family,
            model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
            match_label="related",
            fingerprint=fingerprint,
        )
    if spec.normalized_model:
        if exact_model:
            if _mismatched_variant(spec, fingerprint):
                return VerificationResult(
                    status="ambiguous",
                    reason="variant_needs_confirmation",
                    health_state="healthy",
                    product_name=product_name,
                    current_price=fingerprint.current_price,
                    brand=fingerprint.brand,
                    family=fingerprint.family,
                    model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else spec.model_token,
                    match_label="verified_related",
                    fingerprint=fingerprint,
                )
            if fingerprint.current_price is None:
                return VerificationResult(
                    status="ambiguous",
                    reason="price_unavailable",
                    health_state="quarantined",
                    product_name=product_name,
                    current_price=fingerprint.current_price,
                    brand=fingerprint.brand,
                    family=fingerprint.family,
                    model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else spec.model_token,
                    match_label="verified_related",
                    fingerprint=fingerprint,
                )
            return VerificationResult(
                status="verified",
                reason="exact_model_verified",
                health_state="healthy",
                product_name=product_name,
                current_price=fingerprint.current_price,
                brand=fingerprint.brand or spec.brand,
                family=fingerprint.family or spec.family,
                model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else spec.model_token,
                match_label="verified_exact",
                fingerprint=fingerprint,
            )
        if different_model:
            return VerificationResult(
                status="rejected",
                reason="different_model",
                health_state="healthy",
                product_name=product_name,
                current_price=fingerprint.current_price,
                brand=fingerprint.brand,
                family=fingerprint.family,
                model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
                match_label="related",
                fingerprint=fingerprint,
            )
        if same_family and family_core_match:
            health = "quarantined" if fingerprint.current_price is None else "healthy"
            return VerificationResult(
                status="ambiguous",
                reason="model_not_proven",
                health_state=health,
                product_name=product_name,
                current_price=fingerprint.current_price,
                brand=fingerprint.brand,
                family=fingerprint.family,
                model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
                match_label="verified_related",
                fingerprint=fingerprint,
            )
        return VerificationResult(
            status="rejected",
            reason="model_missing",
            health_state="healthy",
            product_name=product_name,
            current_price=fingerprint.current_price,
            brand=fingerprint.brand,
            family=fingerprint.family,
            model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
                match_label="related",
                fingerprint=fingerprint,
            )
    if spec.query_type in {QueryType.NAMED_PRODUCT.value, QueryType.PRODUCT_LINE.value}:
        required_ok = _required_tokens_present(primary_identity, spec.required_tokens)
        if same_family and family_core_match:
            if _hard_variant_conflict(spec, primary_identity):
                return VerificationResult(
                    status="rejected",
                    reason="hard_variant_conflict",
                    health_state="healthy",
                    product_name=product_name,
                    current_price=fingerprint.current_price,
                    brand=fingerprint.brand,
                    family=fingerprint.family,
                    model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
                    match_label="related",
                    fingerprint=fingerprint,
                )
        if same_family and family_core_match and required_ok:
            if _mismatched_variant(spec, fingerprint):
                return VerificationResult(
                    status="ambiguous",
                    reason="variant_needs_confirmation",
                    health_state="healthy",
                    product_name=product_name,
                    current_price=fingerprint.current_price,
                    brand=fingerprint.brand,
                    family=fingerprint.family,
                    model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
                    match_label="verified_related",
                    fingerprint=fingerprint,
                )
            if fingerprint.current_price is None:
                return VerificationResult(
                    status="ambiguous",
                    reason="price_unavailable",
                    health_state="quarantined",
                    product_name=product_name,
                    current_price=fingerprint.current_price,
                    brand=fingerprint.brand or spec.brand,
                    family=fingerprint.family or spec.family,
                    model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else spec.model_token,
                    match_label="verified_named",
                    fingerprint=fingerprint,
                )
            return VerificationResult(
                status="verified",
                reason="named_product_verified",
                health_state="healthy",
                product_name=product_name,
                current_price=fingerprint.current_price,
                brand=fingerprint.brand or spec.brand,
                family=fingerprint.family or spec.family,
                model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else spec.model_token,
                match_label="verified_named",
                fingerprint=fingerprint,
            )
        if same_family and family_core_match:
            health = "quarantined" if fingerprint.current_price is None else "healthy"
            return VerificationResult(
                status="ambiguous",
                reason="named_product_needs_confirmation",
                health_state=health,
                product_name=product_name,
                current_price=fingerprint.current_price,
                brand=fingerprint.brand,
                family=fingerprint.family,
                model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
                match_label="verified_related" if fingerprint.current_price is not None else "related",
                fingerprint=fingerprint,
            )
        return VerificationResult(
            status="rejected",
            reason="insufficient_identity",
            health_state="healthy",
            product_name=product_name,
            current_price=fingerprint.current_price,
            brand=fingerprint.brand,
            family=fingerprint.family,
            model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
            match_label="related",
            fingerprint=fingerprint,
        )
    if same_family and family_core_match and (spec.family or fingerprint.family):
        health = "quarantined" if fingerprint.current_price is None else "healthy"
        return VerificationResult(
            status="ambiguous",
            reason="category_primary_match",
            health_state=health,
            product_name=product_name,
            current_price=fingerprint.current_price,
            brand=fingerprint.brand,
            family=fingerprint.family,
            model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
            match_label="category_primary" if fingerprint.current_price is not None else "related",
            fingerprint=fingerprint,
        )
    return VerificationResult(
        status="rejected",
        reason="insufficient_identity",
        health_state="healthy",
        product_name=product_name,
        current_price=fingerprint.current_price,
        brand=fingerprint.brand,
        family=fingerprint.family,
        model_token=fingerprint.model_tokens[0] if fingerprint.model_tokens else None,
        match_label="related",
        fingerprint=fingerprint,
    )


def verification_result_to_fields(result: VerificationResult) -> dict[str, Any]:
    return {
        "verification_state": result.status,
        "verification_reason": result.reason,
        "health_state": result.health_state,
        "matched_product_name": result.product_name,
        "fingerprint_brand": result.brand,
        "fingerprint_family": result.family,
        "fingerprint_model": result.model_token,
        "match_label": result.match_label,
        "fingerprint_json": json.dumps(asdict(result.fingerprint)),
    }
