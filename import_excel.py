from pymongo import MongoClient
import pandas as pd

excel_file = pd.read_excel("Dubai Marina (Special 2025).xlsx", sheet_name=None)
df = pd.concat(excel_file.values(), ignore_index=True)


# Connect to MongoDB Atlas
client = MongoClient("mongodb+srv://flaskuser:mypassword@cluster0.c971gqv.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client.get_database("property_db")
collection = db["properties"]


# Ensure index for fast lookup
collection.create_index([("building_name", 1), ("unit_number", 1)])

# Cache existing data
existing_props = {
    (doc["building_name"], doc["unit_number"]): doc
    for doc in collection.find({}, {"building_name": 1, "unit_number": 1, "owners": 1})
}

count = 0
for _, row in df.iterrows():
    building = str(row.get("BuildingNameEn", "")).strip()
    unit = str(row.get("UnitNumber", "")).strip()
    area = row.get("Size")
    price = row.get("ProcedureValue")
    reg_date = row.get("Regis")
    property_type = str(row.get("PropertyTypeEn", "")).strip()
    master_project = str(row.get("Project", "")).strip()
    community = str(row.get("Project Lnd", "")).strip()
    land_number = str(row.get("Plot Pre Reg No", "")).strip()
    beds = row.get("Beds", None)
    sub_type = str(row.get("Sub Type", "")).strip()

    owner_name = str(row.get("NameEn", "")).strip()
    role = str(row.get("ProcedurePartyTypeNameEn", "")).strip()
    contact = str(row.get("Mobile", "")).strip() if pd.notna(row.get("Mobile")) else ""

    if not building or not unit or not owner_name:
        continue

    key = (building, unit)
    owner_data = {
        "owner_name": owner_name,
        "role": role,
        "contact": contact,
        "registration_date": str(reg_date) if reg_date else ""
    }

    if key in existing_props:
        existing_doc = existing_props[key]
        if not any(
            o["owner_name"] == owner_name and o["role"] == role and o.get("contact", "") == contact
            for o in existing_doc.get("owners", [])
        ):
            collection.update_one(
                {"_id": existing_doc["_id"]},
                {"$push": {"owners": owner_data}}
            )
    else:
        new_doc = {
            "building_name": building,
            "unit_number": unit,
            "area_sqft": area,
            "price": price,
            "property_type": property_type,
            "community": community,
            "master_project": master_project,
            "land_number": land_number,
            "beds": beds,
            "sub_type": sub_type,
            "owners": [owner_data]
        }
        result = collection.insert_one(new_doc)
        existing_props[key] = {**new_doc, "_id": result.inserted_id}

    count += 1
    if count % 500 == 0:
        print(f"Processed {count} rows...")

print(f"✅ Imported/Updated {count} records.")
