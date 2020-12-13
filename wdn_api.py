from prettyprinter import cpprint as pprint
import requests
from retry import retry
import re
from config import LOGGER


DEFAULT_PAGE_SIZE = 20

API_URL = "https://www.whatdropsnow.com/api/v1"


def get_brand_by_name(brand_name, page_size=DEFAULT_PAGE_SIZE):
    resource_uri = "/searches/brand_search?q={}".format(brand_name)
    brands = paginate_request(resource_uri, page_size)
    brands = [
        brand
        for brand in brands
        if brand["attributes"]["name"].lower() == brand_name.lower()
    ]
    if not brands:
        raise Exception("brand {} not found!".format(brand_name))
    return brands[0]


def get_products_by_brand_id(brand_id, page_size=DEFAULT_PAGE_SIZE):
    resource_uri = "/brands/{}/products".format(brand_id)
    products = paginate_request(resource_uri, page_size)
    products_with_brand_id = [
        dict(product, **{"brand_id": brand_id}) for product in products
    ]
    return products_with_brand_id


def get_retailers(retailer_ids):
    return iteration_request("/retailers/{}", retailer_ids)


def get_products_offers(product_ids):
    offers = iteration_request("/products/{}/offers", product_ids)
    flat_offers = [
        offer
        for offer_data in offers
        for offer in offer_data["data"]
        if offer["attributes"]["product_id"] in product_ids
    ]
    return flat_offers


def match(string, substr):
    re.match(string, substr, re.IGNORECASE)


@retry(delay=1, backoff=2, max_delay=60, tries=5)
def request(api_uri, ignore_404=False):
    request = requests.get("{}{}".format(API_URL, api_uri))
    if request.status_code == 404 and ignore_404:
        LOGGER.error("request failed 404 - {}".format(api_uri))
        return
    if not request.ok:
        raise Exception(
            "request failed with status: {} for url: {}".format(
                request.status_code, request.url
            )
        )
    return request.json()


def iteration_request(uri_templete, ids, ignore_404=True):
    records = []
    for id in ids:
        resource_uri = uri_templete.format(id)
        response_data = request(resource_uri, ignore_404=ignore_404)
        if response_data:
            records += [response_data]
            LOGGER.info(
                "{resource}:\t {done_count}/{total_count}".format(
                    resource=resource_uri,
                    done_count=ids.index(id) + 1,
                    total_count=len(ids),
                )
            )
    return records


def paginate_request(resource_uri, page_size):
    page = 1
    data = []
    while True:
        param_prefix = "&" if "?" in resource_uri else "?"
        pagination_params = "{}page={}&page_size={}".format(
            param_prefix, page, page_size
        )
        api_uri = "{resource_uri}{pagination_params}".format(
            resource_uri=resource_uri, pagination_params=pagination_params
        )
        response = request(api_uri)
        data += response["data"]
        if not data:
            break
        total_count = response["meta"]["page"]["total_count"]
        page = response["meta"]["page"]["current_page"]
        records_fetched_count = page_size * page
        LOGGER.info(
            "paginate_request: {}\t records fetched: {}/{}".format(
                resource_uri, records_fetched_count, total_count
            )
        )
        if records_fetched_count >= total_count:
            break
        page += 1

    return data