"""
hf_utils.py — Hugging Face AI integr    ation for smarter deal discovery.

Uses the free HF Inference API (300 req/hour) for:
  1. Relevance scoring  — sentence similarity filters irrelevant scrape results
  2. Query enhancement  — text generation improves vague search terms
  3. Product dedup      — local string matching groups same products across sources
  4. Deal scoring       — composite score combining discount, relevance, and signals

All features degrade gracefully when the API is unavailable or the user has no
HF_TOKEN configured.  The app works identically without HF — just less smartly.
"""

import json
import hashlib
import logging
import re
import threading
from collections import OrderedDict
from datetime import datetime
from difflib import SequenceMatcher

import requests

try:
    from huggingface_hub import InferenceClient
    HF_AVAILABLE = True
except ImportError:
    HF_AVAILABLE = False

from config import (
    HF_QUERY_CACHE_MAX_ENTRIES,
    HF_QUERY_CACHE_TTL_SECONDS,
    HF_RELEVANCE_CACHE_TTL_SECONDS,
    HF_TOKEN,
    QUERY_ENHANCE_MODEL,
)
from discovery_filters import (
    enrich_result_metadata,
    passes_eligibility,
    resolve_family_and_intent,
)

SIMILARITY_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def _normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace, normalize model numbers."""
    name = name.lower()
    name = re.sub(r"[^\w\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"(\d)\s+(\d)", r"\1\2", name)
    stopwords = {"the", "a", "an", "and", "or", "for", "with", "new", "best",
                 "top", "pack", "bundle", "edition", "version"}
    return " ".join(w for w in name.split() if w not in stopwords)


class SmartEngine:
    """AI-powered enhancements for deal discovery using the free HF Inference API."""

    def __init__(self):
        self._client = None
        self._enabled = HF_AVAILABLE
        self._relevance_cache: dict[tuple[str, str], tuple[datetime, list[float]]] = {}
        self._query_cache: OrderedDict[str, tuple[datetime, str]] = OrderedDict()
        self._cache_lock = threading.Lock()
        self._warm_started = False
        if self._enabled:
            try:
                self._client = InferenceClient(token=HF_TOKEN)
                logging.info(f"[{datetime.now()}] HF SmartEngine initialized"
                             f"{' (authenticated)' if HF_TOKEN else ' (anonymous)'}")
                self._start_warmup()
            except Exception as exc:
                logging.warning(f"[{datetime.now()}] HF SmartEngine init failed: {exc}")
                self._enabled = False

    @property
    def available(self) -> bool:
        return self._enabled and self._client is not None

    def _start_warmup(self) -> None:
        if self._warm_started or not self.available:
            return
        self._warm_started = True

        def warm() -> None:
            try:
                requests.get(
                    f"https://huggingface.co/api/models/{SIMILARITY_MODEL}",
                    timeout=10,
                )
                requests.get(
                    f"https://huggingface.co/api/models/{SIMILARITY_MODEL}?expand=inferenceProviderMapping",
                    timeout=10,
                )
            except Exception as exc:
                logging.debug(f"[{datetime.now()}] HF warmup skipped: {exc}")

        threading.Thread(target=warm, daemon=True).start()

    def _relevance_cache_key(self, query: str, product_names: list[str]) -> tuple[str, str]:
        normalized_query = _normalize_name(query)
        payload = json.dumps([_normalize_name(name) for name in product_names], separators=(",", ":"))
        titles_hash = hashlib.sha1(payload.encode("utf-8")).hexdigest()
        return normalized_query, titles_hash

    def _query_cache_get(self, query: str) -> str | None:
        key = (query or "").strip()
        if not key:
            return None
        with self._cache_lock:
            cached = self._query_cache.get(key)
            if cached is None:
                return None
            cached_at, value = cached
            if (datetime.now() - cached_at).total_seconds() > HF_QUERY_CACHE_TTL_SECONDS:
                self._query_cache.pop(key, None)
                return None
            self._query_cache.move_to_end(key)
            return value

    def _query_cache_set(self, query: str, value: str) -> None:
        key = (query or "").strip()
        if not key:
            return
        with self._cache_lock:
            self._query_cache[key] = (datetime.now(), value)
            self._query_cache.move_to_end(key)
            while len(self._query_cache) > max(1, HF_QUERY_CACHE_MAX_ENTRIES):
                self._query_cache.popitem(last=False)

    # ------------------------------------------------------------------
    # 1.  Relevance scoring  (1 API call — sentence_similarity)
    # ------------------------------------------------------------------
    def score_relevance(self, query: str, results: list[dict]) -> list[dict]:
        """
        Score how relevant each result is to the search query.
        Adds a 'relevance_score' field (0.0–1.0) to every result dict.
        Falls back to keyword-overlap heuristic when the API is down.
        """
        if not results:
            return results

        product_names = [r.get("product_name", "") for r in results]
        cache_key = self._relevance_cache_key(query, product_names)

        if self.available:
            now = datetime.now()
            with self._cache_lock:
                cached = self._relevance_cache.get(cache_key)
                if cached is not None:
                    cached_at, scores = cached
                    if (now - cached_at).total_seconds() <= HF_RELEVANCE_CACHE_TTL_SECONDS:
                        for r, score in zip(results, scores):
                            r["relevance_score"] = round(max(0.0, min(1.0, float(score))), 3)
                        return results
            try:
                scores = self._client.sentence_similarity(
                    query,
                    other_sentences=product_names,
                    model=SIMILARITY_MODEL,
                )
                with self._cache_lock:
                    self._relevance_cache[cache_key] = (datetime.now(), [float(score) for score in scores])
                for r, score in zip(results, scores):
                    r["relevance_score"] = round(max(0.0, min(1.0, float(score))), 3)
                logging.info(f"[{datetime.now()}] HF relevance scored {len(results)} results")
                return results
            except Exception as exc:
                logging.warning(f"[{datetime.now()}] HF relevance scoring failed: {exc}")

        query_words = set(_normalize_name(query).split())
        for r in results:
            name_words = set(_normalize_name(r.get("product_name", "")).split())
            overlap = len(query_words & name_words) / max(len(query_words), 1)
            # Penalise accessory/non-primary listings so they don't outscore real
            # products in the fallback path (structural_relevance is the gate, but
            # a low relevance_score helps blended-relevance cut as well).
            kind = r.get("product_kind", "primary_product")
            if kind in ("accessory", "compatible_generic"):
                overlap *= 0.30
            elif kind == "other_brand":
                overlap *= 0.10
            cond = r.get("condition_class", "new")
            if cond in ("renewed", "refurbished", "used"):
                overlap *= 0.70
            r["relevance_score"] = round(min(1.0, overlap), 3)
        return results

    # ------------------------------------------------------------------
    # 2.  Query enhancement  (1 API call — text_generation)
    # ------------------------------------------------------------------
    def enhance_query(self, query: str) -> str:
        """
        Return an improved search string by asking an LLM for a more specific
        product-search reformulation.  Returns the original query on failure.
        """
        if not self.available:
            return query

        cached = self._query_cache_get(query)
        if cached is not None:
            return cached

        try:
            msg = (
                "Rewrite this into a single short product search term for Amazon "
                "or Best Buy (no quotes, no explanation, one line only).\n\n"
                f"Query: {query}"
            )
            resp = self._client.chat_completion(
                model=QUERY_ENHANCE_MODEL,
                messages=[
                    {"role": "system", "content": "You output only the search keywords."},
                    {"role": "user", "content": msg},
                ],
                max_tokens=48,
                temperature=0.3,
            )
            msg_obj = resp.choices[0].message
            text = msg_obj.content if hasattr(msg_obj, "content") else msg_obj["content"]
            text = str(text).strip()
            cleaned = text.strip('"').strip("'").strip()
            cleaned = cleaned.split("\n")[0].strip()
            if 3 < len(cleaned) < 120:
                self._query_cache_set(query, cleaned)
                logging.info(f"[{datetime.now()}] HF query enhanced: "
                             f"'{query}' → '{cleaned}'")
                return cleaned
        except Exception as exc:
            logging.warning(f"[{datetime.now()}] HF query enhancement failed: {exc}")

        return query

    # ------------------------------------------------------------------
    # 3.  Product deduplication  (0 API calls — local string matching)
    # ------------------------------------------------------------------
    def deduplicate_results(self, results: list[dict],
                            threshold: float = 0.72) -> list[dict]:
        """
        Group duplicate products across sources using fuzzy name matching.
        Adds: group_id, is_best_in_group, group_size, also_available_at.
        """
        if not results:
            return results

        groups: list[list[int]] = []
        assigned: set[int] = set()

        for i, ri in enumerate(results):
            if i in assigned:
                continue
            group = [i]
            assigned.add(i)
            name_i = _normalize_name(ri.get("product_name", ""))

            for j in range(i + 1, len(results)):
                if j in assigned:
                    continue
                name_j = _normalize_name(results[j].get("product_name", ""))
                if SequenceMatcher(None, name_i, name_j).ratio() >= threshold:
                    group.append(j)
                    assigned.add(j)

            groups.append(group)

        for gid, group in enumerate(groups):
            ranked = sorted(group,
                            key=lambda idx: results[idx].get("current_price",
                                                              float("inf")))
            for rank, idx in enumerate(ranked):
                results[idx]["group_id"] = gid
                results[idx]["is_best_in_group"] = (rank == 0)
                results[idx]["group_size"] = len(group)
                if len(group) > 1 and rank == 0:
                    primary_source = (results[idx].get("source_name") or "").strip()
                    seen_sources: set[str] = set()
                    others = []
                    for g in group:
                        if g == idx:
                            continue
                        source_name = (results[g].get("source_name") or "").strip()
                        if (
                            not source_name
                            or source_name == primary_source
                            or source_name in seen_sources
                        ):
                            continue
                        seen_sources.add(source_name)
                        others.append(source_name)
                    results[idx]["also_available_at"] = [o for o in others if o]

        return results

    # ------------------------------------------------------------------
    # 4.  Composite deal score  (0 API calls — arithmetic)
    # ------------------------------------------------------------------
    def compute_deal_score(self, results: list[dict]) -> list[dict]:
        """
        Separate factors: identity_match and condition act as multiplicative gates;
        deal_value (discount + multi-source + HF relevance) ranks survivors.
        """
        cond_map = {
            "new": 1.0,
            "open_box": 0.74,
            "renewed": 0.58,
            "refurbished": 0.55,
            "used": 0.38,
        }
        for r in results:
            disc = min(float(r.get("discount_percent") or 0), 45.0) / 45.0
            struct = float(r.get("structural_relevance", 0.5))
            hfrel = float(r.get("relevance_score", 0.5))
            blend = r.get("blended_relevance")
            if blend is None:
                blend = 0.55 * struct + 0.45 * hfrel
                r["blended_relevance"] = round(blend, 3)
            else:
                blend = float(blend)

            multi = min(int(r.get("group_size", 1) or 1), 4) / 4.0
            best = 1.0 if r.get("is_best_in_group", True) else 0.45

            deal_value = (
                disc * 0.35
                + float(multi) * 0.25
                + best * 0.15
                + blend * 0.25
            )

            cond = r.get("condition_class", "new")
            cond_gate = cond_map.get(cond, 0.88)

            identity_gate = float(r.get("identity_match", struct))
            trust_w = float(r.get("confidence", r.get("trust_score", 0.7)))
            trust_w = max(0.15, min(1.0, trust_w))

            pen = float(r.get("listing_penalty", 0) or 0)

            score_01 = deal_value * identity_gate * cond_gate * trust_w - pen * 0.35
            r["deal_value"] = round(deal_value, 4)
            r["deal_score"] = round(max(0.0, min(100.0, score_01 * 100.0)), 1)

        return results

    # ------------------------------------------------------------------
    # 5.  Full pipeline  (single entry point)
    # ------------------------------------------------------------------
    def filter_irrelevant(self, results: list[dict],
                          min_relevance: float = 0.20) -> list[dict]:
        """Drop results below a minimum relevance threshold."""
        return [r for r in results if r.get("relevance_score", 0.5) >= min_relevance]

    def filter_by_blended_relevance(
        self, results: list[dict], min_blended: float = 0.22,
    ) -> list[dict]:
        """Keep rows whose blend of structural + HF scores clears the bar."""
        out: list[dict] = []
        for r in results:
            struct = float(r.get("structural_relevance", 0.5))
            hfrel = float(r.get("relevance_score", 0.5))
            blend = 0.52 * struct + 0.48 * hfrel
            r["blended_relevance"] = round(blend, 3)
            if blend >= min_blended:
                out.append(r)
        return out

    def process_discovery_results(
        self,
        query: str,
        results: list[dict],
        *,
        condition_filter: str = "new_only",
        product_filter: str = "primary_only",
        brand_filter: str = "exact",
    ) -> list[dict]:
        """
        Classify & gate listings → HF relevance → blended cut → dedupe → deal score.
        """
        if not results:
            return results

        family, accessory_intent = resolve_family_and_intent(query)
        for r in results:
            enrich_result_metadata(
                r, query, family=family, accessory_intent=accessory_intent
            )

        gated = [
            r
            for r in results
            if passes_eligibility(
                r,
                condition_filter=condition_filter,
                product_filter=product_filter,
                brand_filter=brand_filter,
                family=family,
                accessory_intent=accessory_intent,
                query_for_intent=query,
                min_confidence=0.25,
            )
        ]
        logging.info(
            f"[{datetime.now()}] Discovery pipeline: "
            f"cond={condition_filter} product={product_filter} brand={brand_filter}; "
            f"raw={len(results)} after_gate={len(gated)}"
        )
        if not gated:
            return []

        gated = self.score_relevance(query, gated)
        # Soft trim: identity + confidence already gated; blend only culls noise
        gated = self.filter_by_blended_relevance(gated, min_blended=0.18)
        if not gated:
            return []

        gated = self.deduplicate_results(gated)
        gated = self.compute_deal_score(gated)
        gated.sort(
            key=lambda r: (
                -r.get("deal_score", 0),
                -float(r.get("identity_match", 0)),
                -float(r.get("structural_relevance", 0)),
                float(r.get("current_price") or 1e9),
            )
        )
        return gated


# ------------------------------------------------------------------
# Module-level singleton
# ------------------------------------------------------------------
_engine: SmartEngine | None = None


def get_smart_engine() -> SmartEngine:
    global _engine
    if _engine is None:
        _engine = SmartEngine()
    return _engine
