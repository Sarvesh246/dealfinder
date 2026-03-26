"""
product_verifier.py - strict product spec parsing and PDP identity verification.

Tracked products use this module to decide whether a page is the same core product,
an ambiguous related candidate, or a hard rejection.
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
    (re.compile(r"\brtx\s*\d{3,4}\s*(?:ti|super)?\b", re.I), "gpu", "nvidia"),
    (re.compile(r"\brx\s*\d{3,4}\s*(?:xt)?\b", re.I), "gpu", "amd"),
    (re.compile(r"\bgalaxy\s*[sz]\s*\d{2,3}\b", re.I), "phone", "samsung"),
    (re.compile(r"\bpixel\s*[89]\b", re.I), "phone", "google"),
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
    "ps5": "ps5",
    "xbox": "xbox",
}


def _norm_token(value: str | None) -> str | None:
    if not value:
        return None
    out = re.sub(r"[\s\-_/]+", "", value.lower())
    return out or None


def _normalize_text_blob(value: str | None) -> str:
    text = unquote(value or "")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[_/|]+", " ", text)
    text = re.sub(r"[^a-zA-Z0-9.+\- ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


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
    qn = normalize_user_query(raw_query)
    family_id = _infer_family_id_from_query(qn, intent.family)
    family = _FAMILY_BY_ID.get(family_id) if family_id else intent.family
    brand = _infer_brand_from_query(qn, family)
    model_token = intent.model_token
    canonical_parts: list[str] = []
    if brand:
        canonical_parts.append(brand)
    if model_token:
        canonical_parts.append(model_token)
    elif raw_query.strip():
        canonical_parts.append(raw_query.strip())
    family_term = _FAMILY_QUERY_TERM.get(family_id or "")
    if family_term and family_term.lower() not in " ".join(canonical_parts).lower():
        canonical_parts.append(family_term)
    canonical_query = re.sub(r"\s+", " ", " ".join(canonical_parts)).strip() or raw_query.strip()
    return ProductSpec(
        raw_query=raw_query.strip(),
        canonical_query=canonical_query,
        brand=brand,
        family=family_id,
        model_token=model_token,
        normalized_model=_norm_token(model_token),
        variant_tokens=_extract_variant_tokens(qn),
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
    family_id = _infer_family_from_text(combined, family_hint)
    family = _FAMILY_BY_ID.get(family_id or "")
    model_tokens, normalized_models = _collect_model_tokens(combined)
    accessory_signal = bool(_GENERIC_ACCESSORY.search(signal_text))
    compatibility_signal = bool(_COMPATIBILITY.search(signal_text))
    bundle_signal = bool(_BUNDLE.search(signal_text))
    hard_block_signal = bool(
        family and family.get("hard_block") and family["hard_block"].search(signal_text)
    )
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
    if fp.brand and fp.brand != spec.brand:
        return True
    family = _FAMILY_BY_ID.get(spec.family or "")
    if not family:
        return False
    for brand in family.get("brand_tokens", ()):
        if brand == spec.brand:
            continue
        if re.search(rf"\b{re.escape(brand)}\b", fp.raw_text, re.I):
            return True
    return False


def verify_listing(spec: ProductSpec, fingerprint: ListingFingerprint) -> VerificationResult:
    product_name = fingerprint.title or spec.raw_query
    family = _FAMILY_BY_ID.get(spec.family or "")
    same_family = not spec.family or fingerprint.family == spec.family
    exact_model = bool(
        spec.normalized_model
        and (
            spec.normalized_model in fingerprint.normalized_model_tokens
            or spec.normalized_model in _normalize_text_blob(fingerprint.title).replace(" ", "")
        )
    )
    different_model = bool(
        spec.normalized_model
        and fingerprint.normalized_model_tokens
        and spec.normalized_model not in fingerprint.normalized_model_tokens
    )
    family_core_match = bool(
        family and family["title_core"].search(fingerprint.raw_text)
    ) if family else same_family
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
    if same_family and family_core_match:
        health = "quarantined" if fingerprint.current_price is None else "healthy"
        return VerificationResult(
            status="ambiguous",
            reason="family_match_requires_confirmation",
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
