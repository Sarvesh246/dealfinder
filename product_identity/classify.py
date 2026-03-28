"""Listing classification, scoring, and confidence gates for discovery."""

from __future__ import annotations

import logging
import re
from typing import Any

from .intent import _has_family_phrase
from .query import (
    QueryIntent,
    QueryType,
    _ACCESSORY_WORDS,
    _BUNDLE_WORDS,
    _COMPAT_PHRASES,
    _model_mismatch_exact,
    _strip_bundle_case_phrases,
    _strip_watch_primary_phrases,
    merge_condition_title_url,
    normalize_user_query,
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
    primary_signals = tuple(normalize_user_query(s) for s in family.get("primary_signals", ()))
    negative_signals = tuple(normalize_user_query(s) for s in family.get("negative_signals", ()))
    require_primary_signal = bool(family.get("require_primary_signal"))
    require_brand_presence = bool(family.get("require_brand_presence"))
    brand_in = any(b in tl for b in brand_expect)

    has_core = bool(core_rx.search(tl))
    has_primary_signal = _has_family_phrase(tl, primary_signals) if primary_signals else False
    has_negative_signal = _has_family_phrase(tl, negative_signals) if negative_signals else False
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

    if has_core and require_brand_presence and not brand_in:
        product_kind = "other_brand"
        listing_role = "different_brand"
        penalty_total += 0.82
        structural_relevance = 0.1
        identity = compute_identity_match(
            intent,
            has_core=has_core,
            product_kind=product_kind,
            model_mismatch_exact=False,
            brand_in_title=False,
        )
        conf = compute_confidence(
            intent,
            has_core=has_core,
            product_kind=product_kind,
            trust=trust * 0.6,
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

    if has_core and not accessory_intent:
        if has_negative_signal and not has_primary_signal:
            product_kind = "other_brand"
            listing_role = "different_type"
            penalty_total += 0.9
            structural_relevance = 0.08
            identity = compute_identity_match(
                intent,
                has_core=False,
                product_kind=product_kind,
                model_mismatch_exact=False,
                brand_in_title=brand_in,
            )
            conf = compute_confidence(
                intent,
                has_core=False,
                product_kind=product_kind,
                trust=trust * 0.55,
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
        if require_primary_signal and not has_primary_signal:
            product_kind = "compatible_generic"
            listing_role = "different_type"
            penalty_total += 0.8
            structural_relevance = 0.12
            identity = compute_identity_match(
                intent,
                has_core=False,
                product_kind=product_kind,
                model_mismatch_exact=False,
                brand_in_title=brand_in,
            )
            conf = compute_confidence(
                intent,
                has_core=False,
                product_kind=product_kind,
                trust=trust * 0.65,
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
                model_mismatch_exact=True, brand_in_title=brand_in,
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
