"""
Admin routes.
"""

from __future__ import annotations

from flask import flash, redirect, render_template, request, url_for

from database import add_category, get_all_categories, get_parent_categories, update_category

from . import main_bp


@main_bp.route("/admin/categories", methods=["GET", "POST"], endpoint="admin_categories")
def admin_categories():
    if request.method == "POST":
        action = request.form.get("action", "")

        if action == "add":
            name = request.form.get("name", "").strip()
            icon = request.form.get("icon", "📦").strip()
            parent_id = request.form.get("parent_id", "").strip() or None
            keywords = request.form.get("search_keywords", "").strip()
            slug = name.lower().replace(" ", "-").replace("&", "and")
            if name:
                add_category(name, slug, int(parent_id) if parent_id else None, keywords, icon)
                flash(f'Added category "{name}".', "success")

        elif action == "update":
            cat_id = request.form.get("category_id")
            if cat_id:
                fields = {}
                for field in ("name", "icon", "search_keywords"):
                    value = request.form.get(field, "").strip()
                    if value:
                        fields[field] = value
                parent_id = request.form.get("parent_id", "").strip()
                fields["parent_id"] = int(parent_id) if parent_id else None
                if "name" in fields:
                    fields["slug"] = fields["name"].lower().replace(" ", "-").replace("&", "and")
                update_category(int(cat_id), **fields)
                flash("Category updated.", "success")

        elif action == "toggle":
            cat_id = request.form.get("category_id")
            enabled = request.form.get("enabled", "0")
            if cat_id:
                update_category(int(cat_id), enabled=int(enabled))

        return redirect(url_for("admin_categories"))

    return render_template(
        "admin_categories.html",
        categories=[dict(category) for category in get_all_categories()],
        parent_categories=[dict(parent) for parent in get_parent_categories()],
    )
