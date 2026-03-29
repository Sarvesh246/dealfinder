"""
Route package for PricePulse.

All feature modules register routes on a shared blueprint so the application can
keep existing endpoint names when the blueprint is mounted with an empty name
prefix.
"""

from flask import Blueprint


main_bp = Blueprint("main", __name__)


from . import admin  # noqa: E402,F401
from . import core  # noqa: E402,F401
from . import discovery  # noqa: E402,F401
from . import internal  # noqa: E402,F401
from . import settings  # noqa: E402,F401
from . import tracking  # noqa: E402,F401
