import re
from slugify import slugify
from pymongo import UpdateOne


def update_size_cloth(my_collection):
    my_collection.update_many(
        {"category": {"$in": ["одежда", "обувь", "сумки"]}},
        [{"$set": {
            "leftovers": {
                "$map": {
                    "input": "$leftovers",
                    "in": {
                        "size": "$$this.size",
                        "count": {"$sum": ["$$this.count", 1]},
                        "price": "$$this.price"
                    }
                }
            }
        }
        }])
    return "success"


def update_sku(my_collection):

    my_collection.update_many(
        {"category": {"$in": ["одежда", "обувь", "сумки"]}},
        [
            {
                "$set": {
                    "sku": {
                        "$regexFind": {
                            "input": "$sku",
                            "regex": "\\d+"
                        }
                    }
                }
            },
        ]
    )

    return {"message": "Цены и артиклы на все товары успешно присвоены"}


def remove_color_from_category(my_collection):
    my_collection.update_many(
        {"size_table_type": {
            "$in": ["парфюм", "парфюмерия", "Парфюм", "Парфюмерия"]},
            "size_table_type": {"$exists": True},
         },
        {"$unset": {"color": 1}}
    )
    my_collection.update_many({}, {"$unset": {"fashion_season": 1, "fashion_collection": 1,
                              "fashion_collection_inner": 1, "manufacture_country": 1, "size_table_type": 1, "category": 1}})
    return {"message": "Удалены все цвета из парфюмерии"}


def crud_update_brand(my_collection):
    products = my_collection.find({
        "brand": {"$exists": True},
        "color_name": {"$exists": True},
        "color_id": {"$exists": True}
    }).limit(3000)

    for product in products:
        if "slug" not in product["brand"]:
            name = product["title"]
            if product["color_name"] and product["color_id"]:
                color_code = product["color_id"]
                color_name = product["color_name"]
                sku = product["sku"]

                slug = slugify(f"{name} {color_code} {color_name} {sku}")

                my_collection.update_one(
                    {"_id": product["_id"]},
                    {"$set": {"brand_slug": {"name": name,
                                             "slug": slug, "color_name": color_name}}}
                )
        else:
            continue
    return {"message": "Бренды успешно обновлены"}


def change_color_product(my_collection):
    colors = my_collection.find(
        {"root_category": {"$nin": ["Парфюмерия с маркировкой", "Косметика", "Аксессуары", "Парфюмерия без маркировки"]},
         "color": {"$nin": ""},
         "color": {"$exists": True}}).limit(1000)

    for product in colors:
        if product["color"] is None:
            continue
        if "/" in product["color"]:
            color = product["color"].split("/")
            color_id = color[0]
            color_name = color[1]
            my_collection.update_one(
                {"_id": product["_id"]},
                {"$set": {"color_name": color_name, "color_id": color_id},
                 "$unset": {"color": True}}
            )
        else:
            my_collection.update_one(
                {"_id": product["_id"]},
                {"$set": {"color_name": None, "color_id": None},
                 "$unset": {"color": True}}
            )
    return {"message": "Цвета успешно обновлены"}


def set_data_price(my_collection):

    items = my_collection.find().limit(1000)

    for item in items:
        if item.get("discount_price", 0) > 0:
            item["price"] = item["discount_price"]
        else:
            item["discount_price"] = 0

        if item["discount_price"] > item["price"] or item["price"] == item["discount_price"]:
            item["price"] = item["discount_price"]
            item["discount_price"] = 0

        my_collection.update_one({"_id": item["_id"]}, {"$set": item})

    return {"message": "Цены на все товары успешно присвоены"}


def get_category_slug(category_name):
    return slugify(category_name)


def update_categories(my_collection):

    categories = my_collection.distinct("root_category")

    for category in categories:
        slug = slugify(category)
        category_entity = {"name": category, "slug": slug}
        my_collection.update_many({"root_category": category}, {
                                  "$set": {"category_entity": category_entity}})

    return {"message": "Categories updated successfully"}
