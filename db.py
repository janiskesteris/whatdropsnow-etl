from sqlalchemy import (
    UniqueConstraint,
    inspect,
    create_engine,
    Column,
    Integer,
    Numeric,
    String,
    DateTime,
    Sequence,
    Float,
    PrimaryKeyConstraint,
    ForeignKey,
)
from decimal import Decimal
from sqlalchemy.types import ARRAY as Array
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.sql import *
from retry import retry
from prettyprinter import cpprint as pprint
import yaml
from sqlalchemy.dialects.postgresql import insert


@retry(delay=1, backoff=2, max_delay=60, tries=5)
def connect_db():
    with open("db_config.yml", "r") as stream:
        db_config = yaml.safe_load(stream)
    if "password" in db_config:
        return create_engine(
            "postgresql://{user}:{password}@{host}:{port}/{database}".format(
                **db_config
            )
        )
    else:
        return create_engine(
            "postgresql://{user}@{host}:{port}/{database}".format(**db_config)
        )


def upsert(model, session, rows):
    table = model.__table__
    table.create(bind=session, checkfirst=True)
    stmt = insert(table)
    primary_keys = [key.name for key in inspect(table).primary_key]
    update_dict = {c.name: c for c in stmt.excluded if not c.primary_key}

    if not update_dict:
        raise ValueError("insert_or_update resulted in an empty update_dict")

    stmt = stmt.on_conflict_do_update(index_elements=primary_keys, set_=update_dict)
    session.execute(stmt, rows)


Base = declarative_base()

class Retailers(Base):
    __tablename__ = "retailers"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    website = Column(String)

    def format_data(data):
        return [
            {
                "id": int(record["data"]["id"]),
                "name": record["data"]["attributes"]["name"],
                "website": record["data"]["attributes"]["website"],
            }
            for record in data
        ]


class Offers(Base):
    __tablename__ = "offers"
    id = Column(String, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.id", onupdate="CASCADE"), nullable=False)
    retailer_id = Column(Integer, ForeignKey("retailers.id"), nullable=False)
    original_currency = Column(String)
    price_usd = Column(Numeric)
    price_gbp = Column(Numeric)
    price_eur = Column(Numeric)

    def format_data(data):
        return [
            {
                "id": record["id"],
                "product_id": int(record["attributes"]["product_id"]),
                "retailer_id": int(record["attributes"]["retailer_id"]),
                "original_currency": record["attributes"]["original_currency"],
                "price_usd": Decimal(record["attributes"]["price_usd"]),
                "price_gbp": Decimal(record["attributes"]["price_gbp"]),
                "price_eur": Decimal(record["attributes"]["price_eur"]),
            }
            for record in data
        ]


class Brands(Base):
    __tablename__ = "brands"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    website = Column(String)

    def format_data(data):
        return [
            {
                "id": int(record["id"]),
                "name": record["attributes"]["name"],
                "description": record["attributes"]["description"],
                "website": record["attributes"]["website"],
            }
            for record in data
        ]


class Products(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    brand_id = Column(Integer, ForeignKey("brands.id"), nullable=False)
    name = Column(String)
    description = Column(String)
    release_at = Column(DateTime)
    brand_names = Column(String)

    def format_data(data):
        return [
            {
                "id": int(record["id"]),
                "brand_id": int(record["brand_id"]),
                "name": record["attributes"]["name"],
                "description": record["attributes"]["description"],
                "release_at": record["attributes"]["release_at"],
                "brand_names": record["attributes"]["brand_names"],
            }
            for record in data
        ]