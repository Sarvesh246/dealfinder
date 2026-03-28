"""Query-to-family matching and intent derivation."""

from __future__ import annotations

import re
from typing import Any

from .family_catalog import _FAMILY_DEFS
from .query import (
    QueryIntent,
    QueryType,
    _dedupe_tokens,
    _extract_model_token,
    _extract_soft_variant_tokens,
    _extract_structured_tokens,
    normalize_user_query,
    query_has_accessory_intent,
)

def _family_phrase_present(query_norm: str, phrases: tuple[str, ...]) -> list[str]:
    found: list[str] = []
    for phrase in phrases:
        phrase_norm = normalize_user_query(phrase)
        if phrase_norm and re.search(rf"\b{re.escape(phrase_norm)}\b", query_norm, re.I):
            found.append(phrase_norm)
    return found


def _has_family_phrase(text_norm: str, phrases: tuple[str, ...]) -> bool:
    return bool(_family_phrase_present(text_norm, phrases))


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


def _infer_brand_from_model_context(
    query_norm: str,
    family: dict[str, Any] | None,
    model_token: str | None,
) -> str | None:
    if not family or not model_token:
        return None
    model_norm = normalize_user_query(model_token)
    match = re.search(rf"^(.*?)\b{re.escape(model_norm)}\b", query_norm, re.I)
    if not match:
        return None
    prefix = normalize_user_query(match.group(1) or "").strip()
    if not prefix:
        return None
    parts = prefix.split()
    if not parts:
        return None
    candidate = parts[-1].strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9&-]{1,20}", candidate):
        return None
    generic = {
        family.get("id", "").replace("_", " "),
        "wireless",
        "mechanical",
        "gaming",
        "bluetooth",
        "portable",
        "smart",
        "electric",
        "noise",
        "canceling",
        "cancelling",
    }
    if candidate in generic:
        return None
    return candidate


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
    if family.get("id") == "nintendo_switch":
        for phrase in ("nintendo switch",):
            if re.search(rf"\b{re.escape(phrase)}\b", query_norm, re.I) and phrase not in required:
                required.append(phrase)
    if family.get("id") == "ps5":
        for phrase in ("playstation 5", "ps5"):
            if re.search(rf"\b{re.escape(phrase)}\b", query_norm, re.I) and phrase not in required:
                required.append(phrase)
    if family.get("id") == "xbox":
        for phrase in ("xbox series x", "xbox series s"):
            if re.search(rf"\b{re.escape(phrase)}\b", query_norm, re.I) and phrase not in required:
                required.append(phrase)
    if family.get("id") == "kindle_paperwhite":
        for phrase in ("kindle paperwhite", "paperwhite"):
            if re.search(rf"\b{re.escape(phrase)}\b", query_norm, re.I) and phrase not in required:
                required.append(phrase)
    if family.get("id") == "roku_ultra":
        for phrase in ("roku ultra",):
            if re.search(rf"\b{re.escape(phrase)}\b", query_norm, re.I) and phrase not in required:
                required.append(phrase)
    if family.get("id") == "building_set":
        match = re.search(r"\blego\s+(.+)$", query_norm, re.I)
        if match:
            tail = normalize_user_query(match.group(1))
            tail = re.sub(r"\b(?:building\s*set|botanicals|flowers?)\b", " ", tail, flags=re.I)
            tail = re.sub(r"\s+", " ", tail).strip()
            if tail:
                required.append(tail)
    if family.get("id") == "power_bank":
        if re.search(r"\b737\b", query_norm):
            required.append("737")
    if family.get("id") == "coffee_maker":
        for phrase in ("k express", "k mini", "k elite", "k compact", "k supreme"):
            if re.search(rf"\b{re.escape(phrase)}\b", query_norm, re.I) and phrase not in required:
                required.append(phrase)
    required.extend(tok for tok in _extract_structured_tokens(query_norm) if tok not in required)
    if brand and family:
        brand_rx = re.compile(rf"^\s*{re.escape(normalize_user_query(brand))}\b", re.I)
        tail = brand_rx.sub("", query_norm, count=1).strip()
        family_terms = [
            family.get("id", "").replace("_", " "),
            "mouse",
            "keyboard",
            "power bank",
            "portable charger",
            "coffee maker",
            "building set",
            "printer",
            "router",
            "vacuum",
        ]
        for family_term in family_terms:
            if family_term:
                tail = re.sub(rf"\b{re.escape(family_term)}\b", " ", tail, flags=re.I)
        tail = re.sub(
            r"\b(?:wireless|mechanical|gaming|bluetooth|portable|smart|electric|"
            r"cordless|laser|inkjet|single\s*serve)\b",
            " ",
            tail,
            flags=re.I,
        )
        tail = re.sub(r"\s+", " ", tail).strip()
        if tail:
            pieces = [piece for piece in tail.split() if piece]
            if 1 <= len(pieces) <= 4 and any(re.search(r"\d", piece) or len(piece) >= 4 for piece in pieces):
                required.append(" ".join(pieces))
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
    if brand and family.get("id") in {"pressure_cooker", "air_fryer", "coffee_maker", "building_set", "power_bank"}:
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
    model_token = _extract_model_token(qn, family)
    brand = _extract_brand_from_query(qn, family) or _infer_brand_from_model_context(qn, family, model_token)
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
