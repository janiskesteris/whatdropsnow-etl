from config import LOGGER
from local_cache import LocalCache
import wdn_api
import db

use_local_cache = False
brands = ["converse", "vans", "nike", "adidas", "jordan"]
# brands = ["converse"]

brands_data = []
products_data = []
offers_data = []

for brand in brands:
    brand_data = LocalCache(
        func=lambda: wdn_api.get_brand_by_name(brand),
        key="{}_brand".format(brand),
        use_local_cache=use_local_cache
    ).fetch()
    brands_data += [brand_data]

    products_data += LocalCache(
        func=lambda: wdn_api.get_products_by_brand_id(brand_data["id"], page_size=2000),
        key="{}_products".format(brand),
        use_local_cache=use_local_cache,
    ).fetch()
    product_ids = [int(product["id"]) for product in products_data]

    offers_data += LocalCache(
        func=lambda: wdn_api.get_products_offers(product_ids),
        key="{}_offers".format(brand),
        use_local_cache=use_local_cache
    ).fetch()
    LOGGER.info("Finished fetching for {}".format(brand))


retailer_ids = list({data["attributes"]["retailer_id"] for data in offers_data})
retailer_data = LocalCache(
    func=lambda: wdn_api.get_retailers(retailer_ids),
    key="all_retailers",
    use_local_cache=use_local_cache
).fetch()

session = db.connect_db()
db.upsert(db.Brands, session, db.Brands.format_data(brands_data))
db.upsert(db.Products, session, db.Products.format_data(products_data))
db.upsert(db.Retailers, session, db.Retailers.format_data(retailer_data))
db.upsert(db.Offers, session, db.Offers.format_data(offers_data))
