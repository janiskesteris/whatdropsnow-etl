from multiprocessing import get_context
import multiprocessing

from prettyprinter import cpprint as pprint
import requests
from retry import retry
from config import LOGGER
from functools import partial
import timeout_decorator


DEFAULT_PAGE_SIZE = 20
PARALLEL_PROCESS_COUNT = 5
REQUEST_TIMEOUT = 40
ITERATION_CHUNK_SIZE = 50

API_URL = "https://www.whatdropsnow.com/api/v1"


def get_brand(brand_name, page_size=DEFAULT_PAGE_SIZE, request_callback=None):
    resource_uri = "/searches/brand_search?q={}".format(brand_name)
    brands = paginate_request(resource_uri, page_size, request_callback)
    brands = [
        brand
        for brand in brands
        if brand["attributes"]["name"].lower() == brand_name.lower()
    ]
    if not brands:
        raise Exception("brand {} not found!".format(brand_name))
    return brands[0]

def products_add_brand_id(response_data, brand_id):
    products_with_brand_id = [
        dict(product, **{"brand_id": brand_id}) for product in response_data
    ]
    return products_with_brand_id


def get_products(brand_id, page_size=DEFAULT_PAGE_SIZE, request_callback=None):
    resource_uri = "/brands/{}/products".format(brand_id)
    modified_callback = (lambda resp: request_callback(products_add_brand_id(resp, brand_id)))
    products = paginate_request(resource_uri, page_size, modified_callback)
    products_with_brand_id = [
        dict(product, **{"brand_id": brand_id}) for product in products
    ]
    return products_with_brand_id


def get_retailers(retailer_ids, request_callback=None):
    return iteration_request_multithread("/retailers/{}", retailer_ids, request_callback, request_callback=request_callback, processes=PARALLEL_PROCESS_COUNT)


def get_products_offers(product_ids, request_callback=None):
    offers = iteration_request_multithread("/products/{}/offers", product_ids, request_callback=request_callback, processes=PARALLEL_PROCESS_COUNT)
    flat_offers = [
        offer
        for offer_data in offers
        for offer in offer_data
        if offer["attributes"]["product_id"] in product_ids
    ]
    return flat_offers

@timeout_decorator.timeout(REQUEST_TIMEOUT)
@retry(delay=1, backoff=2, max_delay=60, tries=10)
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

def iteration_request(id, uri_templete, ignore_404, request_callback):
    resource_uri = uri_templete.format(id)
    response = request(resource_uri, ignore_404=ignore_404)
    if response and response['data']:
        if request_callback:
            request_callback(response['data'])
        LOGGER.info("{} - done".format(resource_uri))
        return response['data']
    LOGGER.info("{} - response empty".format(resource_uri))
    return {}

def chunks(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        chunk = lst[i:i + chunk_size]
        yield chunk, i+chunk_size

def iteration_request_multithread(uri_templete, ids, ignore_404=True, request_callback=None, processes=1):
    iteration_request_=partial(iteration_request, uri_templete=uri_templete, ignore_404=ignore_404, request_callback=request_callback)
    records = []
    for ids_chunk, i in chunks(ids, chunk_size=ITERATION_CHUNK_SIZE):
        with get_context("spawn").Pool(processes=processes) as pool:
            records += pool.map(iteration_request_, ids_chunk)
            records = [record for record in records if record]
            LOGGER.info(
                "Processed: {}/{}, records with values: {}".format(
                    i, len(ids), len(records)
                )
            )
    return records

def paginate_request(resource_uri, page_size, request_callback=None):
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
        response_data = response["data"]
        if request_callback:
            request_callback(response_data)
        data += response_data
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