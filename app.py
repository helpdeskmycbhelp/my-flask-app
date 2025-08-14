import os
import re
from flask import Flask, render_template, request, jsonify
from pymongo import MongoClient
from bson import ObjectId

app = Flask(__name__)

# -----------------------------
# MongoDB Connection
# -----------------------------
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb+srv://flaskuser:mypassword@cluster0.c971gqv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
)
DB_NAME = os.getenv("MONGO_DB", "property_db")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "properties")

client = MongoClient(MONGO_URI)
db = client.get_database(DB_NAME)
collection = db[COLLECTION_NAME]

# Optional: helpful indexes (safe to leave; they’ll no-op if they exist)
try:
    collection.create_index("city")
    collection.create_index("building_name")
    collection.create_index("community")
    collection.create_index("sub_community")
    collection.create_index("property_type")
    collection.create_index("sub_type")
    collection.create_index("area_sqft")
    collection.create_index("price")
    collection.create_index("beds")
except Exception:
    pass


# -----------------------------
# Helpers
# -----------------------------
def _to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _clean_text_list(vals):
    """Drop Nones/empties/nan-like values, return sorted unique strings."""
    out = []
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s.lower() in ("nan", "null", "none"):
            continue
        out.append(s)
    # unique + sorted (case-insensitive)
    return sorted(set(out), key=lambda x: x.lower())


def _clean_beds_list(vals):
    """Normalize beds for dropdown, keep numeric values and sort."""
    cleaned = []
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s.lower() in ("nan", "null", "none"):
            continue
        try:
            f = float(s)
            n = int(f) if float(f).is_integer() else f
            cleaned.append(n)
        except ValueError:
            pass
    return sorted(set(cleaned), key=lambda x: float(x))


def _regex_contains(text):
    """Case-insensitive 'contains' regex for Mongo, with escaping."""
    if text is None:
        return None
    safe = re.escape(str(text))
    return {"$regex": safe, "$options": "i"}


def _fallback_image_for(prop_id_str: str) -> str:
    """Pick a nice fallback image based on the property's _id."""
    images = [
        "https://images.unsplash.com/photo-1560185127-6ed189bf02f4?q=80&w=1200&auto=format&fit=crop",
        "https://images.unsplash.com/photo-1484154218962-a197022b5858?q=80&w=1200&auto=format&fit=crop",
        "https://images.unsplash.com/photo-1580587771525-78b9dba3b914?q=80&w=1200&auto=format&fit=crop",
        "https://images.unsplash.com/photo-1501183638710-841dd1904471?q=80&w=1200&auto=format&fit=crop",
        "https://images.unsplash.com/photo-1556020685-ae41abfc9365?q=80&w=1200&auto=format&fit=crop",
        "https://images.unsplash.com/photo-1460317442991-0ec209397118?q=80&w=1200&auto=format&fit=crop",
        "https://images.unsplash.com/photo-1505691938895-1758d7feb511?q=80&w=1200&auto=format&fit=crop",
        "https://images.unsplash.com/photo-1505691723518-36a5ac3be353?q=80&w=1200&auto=format&fit=crop",
    ]
    try:
        seed = int(str(prop_id_str)[-6:], 16)
    except Exception:
        seed = 0
    return images[seed % len(images)]


def _attach_hero_img(doc):
    """Add 'hero_img' using explicit image_url or a deterministic fallback."""
    if not isinstance(doc, dict):
        return doc
    img = (doc.get("image_url") or "").strip() if isinstance(doc.get("image_url"), str) else ""
    doc["hero_img"] = img if img else _fallback_image_for(str(doc.get("_id", "")))
    return doc


def _clean_opts(vals):
    """Small cleaner for cascade distincts."""
    out = []
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s.lower() in ("none", "null", "nan"):
            continue
        out.append(s)
    return sorted(set(out), key=lambda x: x.lower())


def distinct_any(names, q=None):
    """Return the first non-empty distinct list across possible field aliases."""
    for n in names:
        vals = collection.distinct(n, q or {})
        cleaned = _clean_text_list(vals)
        if cleaned:
            return cleaned
    return []


# -----------------------------
# Small APIs for cascading filters
# -----------------------------
@app.get("/api/cities")
def api_cities():
    vals = _clean_opts(collection.distinct("city"))
    return jsonify({"cities": vals})


@app.get("/api/cascade_options")
def api_cascade_options():
    """
    Returns options limited by current selections.
    Supported query params: city, building_name, community
    """
    city = (request.args.get("city") or "").strip()
    bldg = (request.args.get("building_name") or "").strip()
    comm = (request.args.get("community") or "").strip()

    q = {}
    if city:
        q["city"] = city
    if bldg:
        q["building_name"] = bldg
    if comm:
        q["community"] = comm

    cur = collection.find(q, {"building_name": 1, "community": 1, "sub_community": 1})
    buildings, communities, subcomms = set(), set(), set()
    for doc in cur:
        if doc.get("building_name"):
            buildings.add(str(doc["building_name"]).strip())
        if doc.get("community"):
            communities.add(str(doc["community"]).strip())
        if doc.get("sub_community"):
            subcomms.add(str(doc["sub_community"]).strip())

    return jsonify(
        {
            "buildings": sorted(buildings),
            "communities": sorted(communities),
            "sub_communities": sorted(subcomms),
        }
    )


# --- Optional type APIs (for future type/sub-type cascading on the client) ---
@app.get("/api/types")
def api_types():
    """
    Returns distinct property types. Optional filters:
    city, building_name, community, sub_community
    """
    q = {}
    for key in ("city", "building_name", "community", "sub_community"):
        val = (request.args.get(key) or "").strip()
        if val:
            q[key] = val
    types_ = _clean_opts(collection.distinct("property_type", q))
    return jsonify({"property_types": types_})


@app.get("/api/subtypes")
def api_subtypes():
    """
    Returns distinct sub types. Optional filters (and 'property_type' for linking):
    city, building_name, community, sub_community, property_type
    """
    q = {}
    for key in ("city", "building_name", "community", "sub_community", "property_type"):
        val = (request.args.get(key) or "").strip()
        if val:
            q[key] = val
    sub_types = _clean_opts(collection.distinct("sub_type", q))
    return jsonify({"sub_types": sub_types})


# --- tiny debug helper: see distinct values quickly in browser ---
@app.get("/debug/distinct/<field>")
def debug_distinct(field):
    vals = list(collection.distinct(field))
    return jsonify({"count": len(vals), "sample": vals[:50]})


# -----------------------------
# Pages
# -----------------------------
@app.route("/")
def home():
    query = {}

    # Free-text search across several fields
    search = request.args.get("building")
    if search:
        rx = _regex_contains(search)
        query["$or"] = [
            {"building_name": rx},
            {"community": rx},
            {"sub_community": rx},
            {"city": rx},
            {"owners.owner_name": rx},
            {"owners.contacts": rx},
        ]

    # Exact match filters (include building_name to work with cascade)
    for key in [
        "property_type",
        "community",
        "city",
        "sub_community",
        "sub_type",
        "land_number",
        "building_name",
    ]:
        val = request.args.get(key)
        if val not in (None, ""):
            query[key] = val

    # Beds (exact; change to {"$gte": f} for N+)
    beds_val = request.args.get("beds")
    if beds_val not in (None, ""):
        f = _to_float(beds_val)
        if f is not None:
            query["beds"] = f

    # Area range
    min_area = _to_float(request.args.get("min_area"))
    max_area = _to_float(request.args.get("max_area"))
    if min_area is not None or max_area is not None:
        rng = {}
        if min_area is not None:
            rng["$gte"] = min_area
        if max_area is not None:
            rng["$lte"] = max_area
        if rng:
            query["area_sqft"] = rng

    # Price range
    min_price = _to_float(request.args.get("min_price"))
    max_price = _to_float(request.args.get("max_price"))
    if min_price is not None or max_price is not None:
        rng = {}
        if min_price is not None:
            rng["$gte"] = min_price
        if max_price is not None:
            rng["$lte"] = max_price
        if rng:
            query["price"] = rng

    # Sorting (default area_sqft desc)
    sort_map = {
        "price": "price",
        "area_sqft": "area_sqft",
        "building_name": "building_name",
        "beds": "beds",
    }
    sort_req = request.args.get("sort_by") or "area_sqft"
    sort_field = sort_map.get(sort_req, "area_sqft")
    sort_order = request.args.get("order") or "desc"
    sort_dir = 1 if sort_order == "asc" else -1

    # Pagination
    try:
        page = int(request.args.get("page", 1) or 1)
        if page < 1:
            page = 1
    except ValueError:
        page = 1
    per_page = 12
    skip = (page - 1) * per_page

    # Counts
    total_properties = collection.count_documents(query)
    total_pages = max((total_properties + per_page - 1) // per_page, 1)
    if page > total_pages:
        page = total_pages
        skip = (page - 1) * per_page

    # Fetch page
    cursor = collection.find(query).sort(sort_field, sort_dir).skip(skip).limit(per_page)
    properties = [_attach_hero_img(p) for p in cursor]

    # Dropdown options (cleaned)
    property_types = distinct_any(["property_type", "propertyType", "Property Type"])
    sub_types = distinct_any(["sub_type", "subType", "Sub Type"])
    communities = _clean_text_list(collection.distinct("community"))
    sub_communities = _clean_text_list(collection.distinct("sub_community"))
    cities = _clean_text_list(collection.distinct("city"))
    land_numbers = _clean_text_list(collection.distinct("land_number"))
    beds_list = _clean_beds_list(collection.distinct("beds"))

    # Stats for hero
    total_communities = len(communities)
    total_cities = len(cities)
    total_count = total_properties

    # Preserve query args for pagination links
    query_args = request.args.to_dict()
    query_args.pop("page", None)

    return render_template(
        "index.html",
        properties=properties,
        dropdowns={
            "property_types": property_types,
            "communities": communities,
            "sub_communities": sub_communities,
            "cities": cities,
            "sub_types": sub_types,
            "land_numbers": land_numbers,
            "beds_list": beds_list,
        },
        property_types=property_types,
        communities=communities,
        sub_communities=sub_communities,
        cities=cities,
        sub_types=sub_types,
        beds_list=beds_list,
        land_numbers=land_numbers,
        total_pages=total_pages,
        current_page=page,
        query_args=query_args,
        total_count=total_count,
        total_communities=total_communities,
        total_cities=total_cities,
    )


@app.route("/property/<property_id>")
def property_detail(property_id):
    prop = None
    try:
        prop = collection.find_one({"_id": ObjectId(property_id)})
    except Exception:
        prop = None

    if prop:
        _attach_hero_img(prop)

    return render_template("detail.html", prop=prop)


# -----------------------------
# Entry
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
