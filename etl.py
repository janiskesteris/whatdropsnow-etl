from multiprocessing import set_start_method
from functools import partial
from config import LOGGER
import wdn_api
import db
import datetime

engine = db.connect_db()
session = db.session()

def persist_data(meta_class, data):
    db.upsert(meta_class, engine, meta_class.format_data(data))

def filter_out_recently_added(meta_class, data, match_field='id'):
    recently_updated_records = session.query(meta_class).filter(meta_class.updated_at > one_day_ago).all()
    recent_data = [getattr(record, match_field) for record in recently_updated_records]
    if data and isinstance(data[0], str):
        data = [elem.lower() for elem in data]
        recent_data = [elem.lower() for elem in recent_data]
    return list(set(data) - set(recent_data))

if __name__ == '__main__':
    set_start_method("spawn")
    brands = ["Converse", "Vans", "Nike", "Adidas", "Jordan"]

    current_time = datetime.datetime.utcnow()
    one_day_ago = current_time - datetime.timedelta(days=1)

    LOGGER.info("Creating DB schemas (if not exist)")
    for meta in [db.Brand, db.Product, db.Retailer, db.Offer]:
        meta.__table__.create(bind=engine, checkfirst=True)

    LOGGER.info("Starting ETL")
    for brand in brands:
        LOGGER.info("Started fetching data for {}".format(brand))
        brand_filtered = filter_out_recently_added(db.Brand, [brand], match_field='name')
        if brand_filtered:
            wdn_api.get_brand(brand_filtered[0], request_callback=partial(persist_data, db.Brand))

        brand_id = session.query(db.Brand).filter(db.Brand.name.ilike(brand)).first().id
        brand_filtered = filter_out_recently_added(db.Product, [brand_id], match_field='brand_id')
        if brand_filtered:
            wdn_api.get_products(brand_filtered[0], page_size=2000, request_callback=partial(persist_data, db.Product))

        products = session.query(db.Product).filter(db.Product.brand_id == brand_id).all()
        product_ids = [product.id for product in products]
        product_ids_filtered = filter_out_recently_added(db.Offer, product_ids, match_field='product_id')
        if product_ids_filtered:
            wdn_api.get_products_offers(product_ids_filtered, request_callback=partial(persist_data, db.Offer))

        offers = session.query(db.Offer).filter(db.Offer.product_id.in_(product_ids_filtered)).all()
        retailer_ids = list({offer.retailer_id for offer in offers})
        retailer_ids_filtered = filter_out_recently_added(db.Retailer, retailer_ids, match_field='id')
        if retailer_ids_filtered:
            wdn_api.get_retailers(retailer_ids, request_callback=partial(persist_data, db.Retailer))

        LOGGER.info("Finished fetching data for {}".format(brand))