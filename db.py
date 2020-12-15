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
from psycopg2.errors import ForeignKeyViolation
from sqlalchemy.exc import IntegrityError
from retry import retry
from prettyprinter import cpprint as pprint
import yaml
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
from config import LOGGER


@retry(delay=10, tries=10)
def connect_db():
    with open("db_config.yml", "r") as stream:
        db_config = yaml.safe_load(stream)
    if "password" in db_config:
        engine = create_engine(
            "postgresql://{user}:{password}@{host}:{port}/{database}".format(
                **db_config
            )
        )
    else:
        engine = create_engine(
            "postgresql://{user}@{host}:{port}/{database}".format(**db_config)
        )
    engine.connect()
    return engine

def session():
    Session = sessionmaker(bind=connect_db())
    return Session()

def upsert(model, engine, rows):
    table = model.__table__
    stmt = insert(table)
    primary_keys = [key.name for key in inspect(table).primary_key]
    update_dict = {c.name: c for c in stmt.excluded if not c.primary_key}

    if not update_dict:
        raise ValueError("insert_or_update resulted in an empty update_dict")

    stmt = stmt.on_conflict_do_update(index_elements=primary_keys, set_=update_dict)
    try:
        engine.execute(stmt, rows)
    except IntegrityError as err:
        if "is not present in table" in err._message():
            LOGGER.error(err)
            return
        raise


Base = declarative_base()

class Retailer(Base):
    __tablename__ = "retailers"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    website = Column(String)
    updated_at = Column(DateTime)

    def parse_data(data):
        return {
            "id": int(data["id"]),
            "name": data["attributes"]["name"],
            "website": data["attributes"]["website"],
            "updated_at": datetime.now()
        }


class Offer(Base):
    __tablename__ = "offers"
    id = Column(String, primary_key=True)
    product_id = Column(Integer, ForeignKey("products.id", onupdate="CASCADE"), nullable=False)
    retailer_id = Column(Integer)
    original_currency = Column(String)
    price_usd = Column(Numeric)
    price_gbp = Column(Numeric)
    price_eur = Column(Numeric)
    updated_at = Column(DateTime)

    def parse_data(data):
        return [
            {
                "id": record["id"],
                "product_id": int(record["attributes"]["product_id"]),
                "retailer_id": int(record["attributes"]["retailer_id"]),
                "original_currency": record["attributes"]["original_currency"],
                "price_usd": Decimal(record["attributes"]["price_usd"]),
                "price_gbp": Decimal(record["attributes"]["price_gbp"]),
                "price_eur": Decimal(record["attributes"]["price_eur"]),
                "updated_at": datetime.now()
            }
            for record in data
        ]


class Brand(Base):
    __tablename__ = "brands"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    website = Column(String)
    updated_at = Column(DateTime)

    def parse_data(data):
        return [
            {
                "id": int(record["id"]),
                "name": record["attributes"]["name"],
                "description": record["attributes"]["description"],
                "website": record["attributes"]["website"],
                "updated_at": datetime.now()
            }
            for record in data
        ]


class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    brand_id = Column(Integer, ForeignKey("brands.id"), nullable=False)
    name = Column(String)
    description = Column(String)
    release_at = Column(DateTime)
    brand_names = Column(String)
    updated_at = Column(DateTime)

    def parse_data(data):
        return [
            {
                "id": int(record["id"]),
                "brand_id": int(record["brand_id"]),
                "name": record["attributes"]["name"],
                "description": record["attributes"]["description"],
                "release_at": record["attributes"]["release_at"],
                "brand_names": record["attributes"]["brand_names"],
                "updated_at": datetime.now()
            }
            for record in data
        ]