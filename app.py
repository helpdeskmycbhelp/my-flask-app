from flask import Flask, render_template, request
from pymongo import MongoClient
from bson import ObjectId

app = Flask(__name__)

# MongoDB Atlas Connection
client = MongoClient(
    "mongodb+srv://flaskuser:mypassword@cluster0.c971gqv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
db = client.get_database("property_db")
collection = db["properties"]


def _to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _clean_text_list(vals):
    """Drop Nones/empties/nan-like values, return sorted strings."""
    out = []
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s.lower() in ("nan", "null", "none"):
            continue
        out.append(s)
    return sorted(out)


def _clean_beds_list(vals):
    """
    Normalize beds for dropdown:
    - keep only numeric values
    - cast to int when possible (so 2.0 -> 2)
    - sort ascending
    """
    cleaned = []
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if not s or s.lower() in ("nan", "null", "none"):
            continue
        try:
            f = float(s)
            n = int(f) if f.is_integer() else f
            cleaned.append(n)
        except ValueError:
            # ignore non-numeric entries
            pass
    return sorted(cleaned)


@app.route("/")
def home():
    query = {}

    # Search (partial) across building/community/master_project (+ owners/contacts)
    building = request.args.get("building")
    if building:
        rx = {"$regex": building, "$options": "i"}
        query["$or"] = [
            {"building_name": rx},
            {"community": rx},
            {"master_project": rx},
            {"owners.owner_name": rx},
            {"owners.contacts": rx},  # works because it's an array of strings
        ]

    # Exact match filters
    for key in ["property_type", "beds", "sub_type", "land_number"]:
        val = request.args.get(key)
        if val not in (None, ""):
            query[key] = _to_float(val) if key == "beds" else val

    # Area range
    min_area = _to_float(request.args.get("min_area"))
    max_area = _to_float(request.args.get("max_area"))
    if min_area is not None or max_area is not None:
        query.setdefault("area_sqft", {})
        if min_area is not None:
            query["area_sqft"]["$gte"] = min_area
        if max_area is not None:
            query["area_sqft"]["$lte"] = max_area

    # Price range
    min_price = _to_float(request.args.get("min_price"))
    max_price = _to_float(request.args.get("max_price"))
    if min_price is not None or max_price is not None:
        query.setdefault("price", {})
        if min_price is not None:
            query["price"]["$gte"] = min_price
        if max_price is not None:
            query["price"]["$lte"] = max_price

    # Sorting (default: area_sqft desc)
    sort_map = {"price": "price", "area_sqft": "area_sqft", "building_name": "building_name"}
    sort_req = request.args.get("sort_by") or "area_sqft"
    sort_field = sort_map.get(sort_req, "area_sqft")
    sort_order = request.args.get("order") or "desc"
    sort_dir = 1 if sort_order == "asc" else -1

    # Pagination
    page = int(request.args.get("page", 1) or 1)
    per_page = 12
    skip = (page - 1) * per_page
    total_properties = collection.count_documents(query)
    total_pages = (total_properties + per_page - 1) // per_page or 1

    # Fetch
    properties = list(
        collection.find(query).sort(sort_field, sort_dir).skip(skip).limit(per_page)
    )

    # Dropdown options (cleaned)
    dropdowns = {
        "property_types": _clean_text_list(collection.distinct("property_type")),
        "beds_list": _clean_beds_list(collection.distinct("beds")),
        "sub_types": _clean_text_list(collection.distinct("sub_type")),
        "land_numbers": _clean_text_list(collection.distinct("land_number")),
    }

    # pass query args for pagination links (template will add page=)
    query_args = request.args.to_dict()
    query_args.pop("page", None)

    return render_template(
        "index.html",
        properties=properties,
        dropdowns=dropdowns,
        total_pages=total_pages,
        current_page=page,
        query_args=query_args,
    )


@app.route("/property/<property_id>")
def property_detail(property_id):
    try:
        prop = collection.find_one({"_id": ObjectId(property_id)})
    except Exception:
        prop = None
    return render_template("detail.html", prop=prop)


if __name__ == "__main__":
    app.run(debug=True)
