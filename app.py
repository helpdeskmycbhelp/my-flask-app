from flask import Flask, render_template, request
from pymongo import MongoClient
from bson import ObjectId

app = Flask(__name__)

# MongoDB Atlas Connection
client = MongoClient("mongodb+srv://flaskuser:mypassword@cluster0.c971gqv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client.get_database("property_db")
collection = db["properties"]

@app.route("/")
def home():
    query = {}

    # Filters (partial match for building/community/master_project)
    building = request.args.get("building")
    if building:
        query["$or"] = [
            {"building_name": {"$regex": building, "$options": "i"}},
            {"community": {"$regex": building, "$options": "i"}},
            {"master_project": {"$regex": building, "$options": "i"}},
        ]

    # Exact match filters
    for key in ["property_type", "beds", "sub_type", "land_number"]:
        val = request.args.get(key)
        if val:
            query[key] = val

    # Area filter
    min_area = request.args.get("min_area")
    max_area = request.args.get("max_area")
    if min_area or max_area:
        query["area_sqft"] = {}
        if min_area:
            query["area_sqft"]["$gte"] = float(min_area)
        if max_area:
            query["area_sqft"]["$lte"] = float(max_area)

    # Price filter
    min_price = request.args.get("min_price")
    max_price = request.args.get("max_price")
    if min_price or max_price:
        query["price"] = {}
        if min_price:
           query["price"]["$gte"] = float(min_price)
        if max_price:
           query["price"]["$lte"] = float(max_price)
        

    # Sorting
    sort_field = request.args.get("sort_by", "price")
    sort_order = request.args.get("order", "asc")
    sort_dir = 1 if sort_order == "asc" else -1

    # Pagination
    page = int(request.args.get("page", 1))
    per_page = 12
    skip = (page - 1) * per_page
    total_properties = collection.count_documents(query)
    total_pages = (total_properties + per_page - 1) // per_page

    # Data fetch
    properties = list(collection.find(query).sort(sort_field, sort_dir).skip(skip).limit(per_page))

    # Dropdowns
    dropdowns = {
        "property_types": sorted(collection.distinct("property_type")),
        "beds_list": sorted(collection.distinct("beds")),
        "sub_types": sorted(collection.distinct("sub_type")),
        "land_numbers": sorted(collection.distinct("land_number")),
    }

    return render_template(
        "index.html",
        properties=properties,
        dropdowns=dropdowns,
        total_pages=total_pages,
        current_page=page,
        request=request
    )


@app.route("/property/<property_id>")
def property_detail(property_id):
    prop = collection.find_one({"_id": ObjectId(property_id)})
    return render_template("detail.html", prop=prop)

if __name__ == '__main__':
    app.run(debug=True)
