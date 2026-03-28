"""Family catalog definitions for product identity classification."""

from __future__ import annotations

import re
from typing import Any

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
        "id": "printer",
        "match_query": re.compile(
            r"\bprinter\b|\blaser\s*printer\b|\binkjet\b|\bhl[- ]?l\d{4}[a-z]{1,3}\b|"
            r"\bmfc[- ]?l\d{4}[a-z]{1,3}\b|\bdcp[- ]?l\d{4}[a-z]{1,3}\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\bprinter\b|\blaser\b|\binkjet\b|\bmonochrome\b|\bduplex\b|"
            r"\ball[- ]in[- ]one\b|\bmultifunction\b",
            re.I,
        ),
        "brand_tokens": ("brother", "hp", "canon", "epson"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("printer", "laser", "inkjet", "duplex", "monochrome"),
        "accessory_signals": (
            "toner", "toner cartridge", "cartridge", "drum", "ink", "label tape",
            "replacement toner", "refill", "copy paper", "photo paper", "labels",
        ),
        "negative_signals": (
            "toner", "cartridge", "drum", "ink", "label tape", "refill",
            "copy paper", "photo paper", "labels",
        ),
        "search_alias_templates": (
            "{brand} {model_token} printer",
            "{brand} {model_token} monochrome laser printer",
            "{brand} {model_token} wireless compact monochrome laser printer",
            "{model_token} printer",
            "{raw}",
        ),
        "model_patterns": [
            re.compile(r"\bhl[- ]?l\d{4}[a-z]{1,3}\b", re.I),
            re.compile(r"\bmfc[- ]?l\d{4}[a-z]{1,3}\b", re.I),
            re.compile(r"\bdcp[- ]?l\d{4}[a-z]{1,3}\b", re.I),
        ],
        "hard_block": re.compile(
            r"\btoner\b|\bcartridge\b|\bdrum\b|\bink\b|\blabel\s*tape\b|\brefill\b|"
            r"\bcopy\s*paper\b|\bphoto\s*paper\b|\blabels?\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("toner", "cartridge", "drum", "ink", "label tape", "refill", "copy paper", "photo paper", "labels"),
    },
    {
        "id": "vacuum",
        "match_query": re.compile(
            r"\bvacuum\b|\bcordless\s*vacuum\b|\bstick\s*vacuum\b|\bhandheld\s*vacuum\b|"
            r"\bdyson\s*v(?:8|10|11|12|15)\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\bvacuum\b|\bstick\s*vacuum\b|\bcordless\b|\bhandheld\b",
            re.I,
        ),
        "brand_tokens": ("dyson", "shark", "bissell", "tineco", "eufy"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("vacuum", "cordless", "stick vacuum", "handheld"),
        "accessory_signals": (
            "filter", "battery", "charger", "replacement head", "brush head",
            "mop pad", "wall mount", "attachment kit",
        ),
        "negative_signals": (
            "filter", "battery", "charger", "replacement head", "brush head",
            "mop pad", "wall mount", "attachment kit",
        ),
        "search_alias_templates": (
            "{brand} {model_token} cordless vacuum",
            "{brand} {model_token} vacuum",
            "{raw}",
        ),
        "model_patterns": [
            re.compile(r"\bv(?:8|10|11|12|15)\b", re.I),
        ],
        "hard_block": re.compile(
            r"\bfilter\b|\bbattery\b|\bcharger\b|\breplacement\s*head\b|\bbrush\s*head\b|"
            r"\bmop\s*pad\b|\bwall\s*mount\b|\battachment\s*kit\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": (
            "filter", "battery", "charger", "replacement head", "brush head",
            "mop pad", "wall mount", "attachment kit",
        ),
    },
    {
        "id": "storage",
        "match_query": re.compile(
            r"\bssd\b|\bnvme\b|\bm\.?2\b|\bsolid\s*state\b|\b(?:9[78]0|9[89]0)\s*(?:pro|evo)\b|"
            r"\bsn\s*\d{3,4}x?\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\bssd\b|\bnvme\b|\bm\.?2\b|\bsolid\s*state\b|\bpcie\b|\bheatsink\b",
            re.I,
        ),
        "brand_tokens": ("samsung", "wd", "western digital", "crucial", "kingston", "sabrent", "sk hynix"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("ssd", "nvme", "m.2", "solid state", "pcie"),
        "accessory_signals": ("enclosure", "adapter", "dock", "cable", "heatsink cover"),
        "negative_signals": ("enclosure", "adapter", "dock", "cable"),
        "search_alias_templates": (
            "{brand} {model_token} ssd",
            "{raw}",
        ),
        "model_patterns": [
            re.compile(r"\b(?:9[78]0|9[89]0)\s*(?:pro|evo)\b", re.I),
            re.compile(r"\bsn\s*\d{3,4}x?\b", re.I),
        ],
        "hard_block": re.compile(
            r"\benclosure\b|\badapter\b|\bdock\b|\bcable\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("enclosure", "adapter", "dock", "cable"),
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
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("mouse", "wireless", "bluetooth", "dpi", "scroll"),
        "accessory_signals": ("pad", "feet", "skates"),
        "negative_signals": ("pad", "feet", "skates"),
        "search_alias_templates": (
            "{brand} {model_token} mouse",
            "{raw}",
        ),
        "model_patterns": [
            re.compile(r"\bmx\s*master\s*3s?\b", re.I),
            re.compile(r"\bmx\s*vertical\b", re.I),
        ],
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": ("pad", "feet", "skates"),
    },
    {
        "id": "coffee_maker",
        "match_query": re.compile(
            r"\bkeurig\b|\bk[- ]?(?:express|mini|elite|compact|supreme)\b|"
            r"\bk-cup\b|\bcoffee\s*maker\b|\bsingle\s*serve\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\bcoffee\s*maker\b|\bk-cup\b|\bsingle\s*serve\b|\bkeurig\b",
            re.I,
        ),
        "brand_tokens": ("keurig", "nespresso", "breville", "mr coffee", "hamilton beach"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("coffee maker", "single serve", "k-cup", "brew sizes"),
        "accessory_signals": ("filter", "descaling", "pod holder", "carafe", "water filter"),
        "negative_signals": ("filter", "descaling", "pod holder", "water filter"),
        "search_alias_templates": (
            "{brand} {model_token} coffee maker",
            "{brand} {required_tokens} coffee maker",
            "{raw}",
        ),
        "model_patterns": [
            re.compile(r"\bk[- ]?(?:express|mini|elite|compact|supreme)\b", re.I),
        ],
        "hard_block": re.compile(
            r"\bfilter\b|\bdescaling\b|\bcleaner\b|\bwater\s*filter\b|\bpod\s*holder\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("filter", "descaling", "pod holder", "water filter"),
        "require_brand_presence": True,
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
        "model_patterns": [
            re.compile(r"\b[a-z]{1,6}\d{2,4}[a-z]?\b", re.I),
        ],
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
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("router", "mesh", "wifi", "dual band", "tri band"),
        "accessory_signals": ("adapter", "antenna", "mount", "extender"),
        "negative_signals": ("adapter", "antenna", "mount", "extender"),
        "search_alias_templates": (
            "{brand} {model_token} router",
            "{model_token} router",
            "{raw}",
        ),
        "model_patterns": [
            re.compile(r"\barcher\s*(?:axe|ax|be|ac)\s*\d{1,4}\b", re.I),
            re.compile(r"\b(?:axe|ax|be|ac)\s*\d{1,4}\b", re.I),
        ],
        "hard_block": None,
        "other_brand_earbuds": None,
        "category_accessory_words": ("cable", "adapter", "mount", "antenna", "extender"),
    },
    {
        "id": "power_bank",
        "match_query": re.compile(
            r"\bpower\s*bank\b|\bportable\s*charger\b|\bbattery\s*pack\b|"
            r"\bpowercore\b|\b737\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\bpower\s*bank\b|\bportable\s*charger\b|\bbattery\s*pack\b|\bpowercore\b",
            re.I,
        ),
        "brand_tokens": ("anker", "ugreen", "baseus", "iniu", "ravpower"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("power bank", "portable charger", "battery pack", "mah", "usb-c"),
        "accessory_signals": ("cable", "wall charger", "charging brick", "adapter"),
        "negative_signals": ("cable", "wall charger", "charging brick", "adapter"),
        "search_alias_templates": (
            "{brand} {model_token} power bank",
            "{brand} {model_token} portable charger",
            "{brand} {model_token} powercore 24k",
            "{brand} {required_tokens} power bank",
            "{raw}",
        ),
        "model_patterns": [
            re.compile(r"\b737\b", re.I),
            re.compile(r"\bpowercore\s*\d+[a-z]?\b", re.I),
        ],
        "hard_block": re.compile(
            r"\bcable\b|\bwall\s*charger\b|\bcharging\s*brick\b|\badapter\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("cable", "wall charger", "charging brick", "adapter"),
        "require_brand_presence": True,
    },
    {
        "id": "building_set",
        "match_query": re.compile(
            r"\blego\b|\bbuilding\s*set\b|\bbotanicals\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\blego\b|\bbuilding\s*set\b|\bbotanicals\b|\bbrick\b",
            re.I,
        ),
        "brand_tokens": ("lego",),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("building set", "botanicals", "lego", "brick"),
        "accessory_signals": ("light kit", "display case", "instruction book", "compatible with lego"),
        "negative_signals": ("light kit", "display case", "instruction book", "compatible with lego"),
        "search_alias_templates": (
            "{brand} {required_tokens} botanicals building set",
            "{brand} {required_tokens} building set",
            "{raw}",
        ),
        "hard_block": re.compile(
            r"\blight\s*kit\b|\bdisplay\s*case\b|\bcompatible\s+with\s+lego\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("light kit", "display case", "instruction book"),
        "require_brand_presence": True,
    },
    {
        "id": "ps5",
        "match_query": re.compile(r"\bps5\b|\bplaystation\s*5\b", re.I),
        "title_core": re.compile(r"\bps5\b|\bplaystation\s*5\b", re.I),
        "brand_tokens": ("sony", "playstation"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": (
            "console", "slim", "digital", "disc", "disc edition", "1tb", "2tb",
            "playstation 5 console", "ps5 console",
        ),
        "accessory_signals": (
            "controller", "charger", "charging station", "stand", "skin",
            "cover", "headset", "faceplate",
        ),
        "negative_signals": (
            "digital code", "download code", "membership", "gift card", "voucher",
            "controller", "charger", "cover", "skin", "headset",
        ),
        "search_alias_templates": (
            "{raw}",
            "playstation 5 console",
            "ps5 console",
            "ps5 slim console",
            "ps5 digital edition console",
        ),
        "hard_block": re.compile(
            r"\b(?:digital\s+code|download\s+code|membership|gift\s*card|voucher)\b|"
            r"\bfor\s+(?:playstation\s*5|ps5)\b|"
            r"^(?!\s*(?:playstation\s*5|ps5)\b).+\-\s*(?:playstation\s*5|ps5)\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": (
            "controller", "charging", "stand", "skin", "case",
        ),
        "require_primary_signal": True,
    },
    {
        "id": "xbox",
        "match_query": re.compile(r"\bxbox\b|\bxbox\s+series\s+[xs]\b", re.I),
        "title_core": re.compile(r"\bxbox\b|\bxbox\s+series\s+[xs]\b", re.I),
        "brand_tokens": ("microsoft", "xbox"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": (
            "console", "series x", "series s", "1tb", "2tb", "digital",
            "all-digital", "xbox console",
        ),
        "accessory_signals": (
            "controller", "charger", "stand", "headset", "skin", "cover",
        ),
        "negative_signals": (
            "digital code", "download code", "membership", "gift card", "voucher",
            "controller", "charger", "headset", "cover", "skin",
        ),
        "search_alias_templates": (
            "{raw}",
            "xbox console",
            "xbox series x console",
            "xbox series s console",
        ),
        "hard_block": re.compile(
            r"\b(?:digital\s+code|download\s+code|membership|gift\s*card|voucher)\b|"
            r"\bfor\s+xbox\b|"
            r"^(?!\s*xbox\b).+\-\s*xbox(?:\s+series\s+[xs])?\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("controller", "charger", "stand"),
        "require_primary_signal": True,
    },
    {
        "id": "nintendo_switch",
        "match_query": re.compile(
            r"\bnintendo\s*switch\b|\bswitch\s*(?:lite|oled|2)\b",
            re.I,
        ),
        "title_core": re.compile(
            r"\bnintendo\s*switch\b|\bswitch\s*(?:lite|oled|2)\b",
            re.I,
        ),
        "brand_tokens": ("nintendo",),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": (
            "console", "system", "handheld", "joy-con", "joy con",
            "dock", "oled", "lite", "32gb", "64gb",
        ),
        "accessory_signals": (
            "controller", "carrying case", "travel case", "screen protector",
            "grip", "charger", "dock set", "shell",
        ),
        "negative_signals": (
            "digital code", "download code", "game card", "nintendo switch online",
            "membership", "amiibo", "carrying case", "travel case",
            "screen protector", "controller", "charger", "grip",
        ),
        "search_alias_templates": (
            "{raw}",
            "nintendo switch console",
            "nintendo switch oled console",
            "nintendo switch lite console",
            "nintendo switch 2 console",
        ),
        "model_patterns": [
            re.compile(r"\bnintendo\s*switch\s*(?:2|lite|oled)\b", re.I),
            re.compile(r"\bswitch\s*(?:2|lite|oled)\b", re.I),
        ],
        "hard_block": re.compile(
            r"\b(?:digital\s+code|download\s+code|game\s+card|amiibo|"
            r"membership|nintendo\s+switch\s+online|screen\s+protector|"
            r"travel\s+case|carrying\s+case)\b|"
            r"\bfor\s+nintendo\s+switch\b|"
            r"^(?!\s*nintendo\s+switch\b).+\-\s*nintendo\s+switch\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": (
            "controller", "joy-con charger", "carrying case", "travel case",
            "screen protector", "grip", "charger", "shell",
        ),
        "require_primary_signal": True,
    },
    {
        "id": "kindle_paperwhite",
        "match_query": re.compile(r"\bkindle\s+paperwhite\b", re.I),
        "title_core": re.compile(r"\bkindle\s+paperwhite\b", re.I),
        "brand_tokens": ("amazon", "kindle"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": (
            "e-reader", "ereader", "kindle paperwhite", "wifi", "ad-supported",
        ),
        "accessory_signals": (
            "case", "cover", "sleeve", "screen protector", "charger",
        ),
        "negative_signals": (
            "case", "cover", "sleeve", "screen protector", "charger",
        ),
        "search_alias_templates": (
            "{raw}",
            "amazon kindle paperwhite ereader",
            "kindle paperwhite ereader",
        ),
        "hard_block": re.compile(
            r"\bfor\s+kindle\s+paperwhite\b|\b(?:screen\s+protector|case|cover|sleeve)\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("case", "cover", "sleeve", "screen protector", "charger"),
        "require_primary_signal": True,
    },
    {
        "id": "roku_ultra",
        "match_query": re.compile(r"\broku\s+ultra\b", re.I),
        "title_core": re.compile(r"\broku\s+ultra\b", re.I),
        "brand_tokens": ("roku",),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("streaming player", "streaming", "4k", "hdr", "ultra"),
        "accessory_signals": ("remote", "cable", "adapter", "mount"),
        "negative_signals": ("remote", "cable", "adapter", "mount"),
        "search_alias_templates": (
            "{raw}",
            "roku ultra streaming player",
        ),
        "hard_block": re.compile(
            r"\bfor\s+roku\s+ultra\b|\b(?:replacement\s+remote|remote|adapter|mount)\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("remote", "cable", "adapter", "mount"),
        "require_primary_signal": True,
    },
    {
        "id": "steam_deck",
        "match_query": re.compile(r"\bsteam\s*deck\b", re.I),
        "title_core": re.compile(r"\bsteam\s*deck\b", re.I),
        "brand_tokens": ("valve", "steam"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("steam deck", "handheld", "gaming handheld", "oled", "lcd", "512gb", "1tb"),
        "accessory_signals": ("case", "dock", "screen protector", "grip", "charger", "skin"),
        "negative_signals": ("case", "dock", "screen protector", "grip", "charger", "skin"),
        "search_alias_templates": (
            "{raw}",
            "steam deck handheld",
            "steam deck oled handheld",
        ),
        "model_patterns": [
            re.compile(r"\bsteam\s*deck\s*oled\b", re.I),
            re.compile(r"\bsteam\s*deck\b", re.I),
        ],
        "hard_block": re.compile(
            r"\bfor\s+steam\s*deck\b|\b(?:screen\s*protector|dock|carrying\s*case|case|grip|skin)\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("screen protector", "dock", "case", "grip", "skin"),
        "require_primary_signal": True,
    },
    {
        "id": "meta_quest",
        "match_query": re.compile(r"\bmeta\s*quest\b|\bquest\s*3s?\b", re.I),
        "title_core": re.compile(r"\bmeta\s*quest\b|\bquest\s*3s?\b", re.I),
        "brand_tokens": ("meta", "oculus"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("vr headset", "mixed reality", "headset", "128gb", "512gb", "quest 3", "quest 3s"),
        "accessory_signals": ("case", "strap", "controller", "charging dock", "lens insert", "facial interface"),
        "negative_signals": ("case", "strap", "controller", "charging dock", "lens insert", "facial interface"),
        "search_alias_templates": (
            "{raw}",
            "meta quest 3 vr headset",
            "meta quest 3s vr headset",
        ),
        "model_patterns": [
            re.compile(r"\bmeta\s*quest\s*3s?\b", re.I),
            re.compile(r"\bquest\s*3s?\b", re.I),
        ],
        "hard_block": re.compile(
            r"\bfor\s+(?:meta\s*)?quest\s*3s?\b|\b(?:strap|case|controller|charging\s*dock|lens\s*insert|facial\s*interface)\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("strap", "case", "controller", "charging dock", "lens insert"),
        "require_primary_signal": True,
    },
    {
        "id": "office_chair",
        "match_query": re.compile(r"\boffice\s*chair\b|\bergonomic\s*chair\b|\bdesk\s*chair\b", re.I),
        "title_core": re.compile(r"\boffice\s*chair\b|\bergonomic\s*chair\b|\bdesk\s*chair\b|\bmesh\s*chair\b", re.I),
        "brand_tokens": ("steelcase", "herman miller", "branch", "autonomous", "sihoo", "hbada"),
        "brand_policy": "exact",
        "partner_brands": (),
        "primary_signals": ("office chair", "ergonomic chair", "desk chair", "lumbar", "adjustable arms", "mesh"),
        "accessory_signals": ("chair mat", "caster", "wheel", "cover", "armrest pad", "replacement cylinder"),
        "negative_signals": ("chair mat", "caster", "wheel", "cover", "armrest pad", "replacement cylinder"),
        "search_alias_templates": (
            "{raw}",
            "{brand} office chair",
            "{brand} ergonomic office chair",
        ),
        "model_patterns": [
            re.compile(r"\baeron\b", re.I),
            re.compile(r"\bleap\s*v?2\b", re.I),
            re.compile(r"\bgesture\b", re.I),
        ],
        "hard_block": re.compile(
            r"\b(?:chair\s*mat|caster|casters|wheel|wheels|cover|armrest\s*pad|replacement\s*cylinder)\b",
            re.I,
        ),
        "other_brand_earbuds": None,
        "category_accessory_words": ("chair mat", "caster", "wheel", "cover", "armrest pad", "replacement cylinder"),
        "require_primary_signal": True,
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
        merged.setdefault("require_primary_signal", False)
        merged.setdefault("require_brand_presence", False)
        out.append(merged)
    return out
