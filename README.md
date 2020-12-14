## General notes

- Packaged with docker compose for both etl script and postgres service
- Persists Brands, Products, Offers and Retailers in PostgresDB
- API calls are parellelized for offers, since can only be made for 1 product at the time
- Implementations is generic, easily new resources can be added for both fetching and persisting

## Run
After runnin docker-compose it will trigger ETL and after it's completed `sql.py` that will run the query and print out result table for analysis. ETL might take a while to finish **~30min** depending on parallization and API response times
```
docker-compose build && docker-compose up
```

To start a fresh run delete `pgdata` folder and rebuilt and rerun
```
rm -rf ./pgdata
docker-compose build && docker-compose up
```

## Implementation details

### architecture diagram
![highsnobiety-Page-1](https://user-images.githubusercontent.com/2915290/102107992-a7d56880-3e32-11eb-92e1-0e79094e8ff6.png)

### etl.py
Main execution script file. Orchestrates the ETL, fetchign data from API and persisting it. Skips recently ETLed data.

- Creates db tables if they don't exists already
- Iterates over brands and fetches all downstream resources for each:
1. get brand data
2. using brand_id get products
3. using product_ids fetch offers
4. using offers.retailer_id fetch retailers

- Implements generic callback method persist_data that gets passed to the API calls and triggered as soon as data is fetched from API.
- Implements recency checking and filtering out records that have already been fetched and persisted in last day. This ensures that even after failure and restart ETL picks up from the place it left of.

### wdn_api.py
Module with functions related to What Drops Now API handling. Implements both generic request methods and pagination as well as business logic abstractions for fetching specific resources.

- Supports parallelization
- Implements retry logic with exponensial backoff for request errors
- Implements callback logic for generic use (in this case used for persisting data in DB)
  - For many Offers API endpoint returns empty results, these get refetched every time ETL is retriggered

#### Constants
**DEFAULT_PAGE_SIZE** - default page size for API
**PARALLEL_PROCESS_COUNT** - defaul parallalization, wouldn't go higher than 5 API seems to throttle responses after a while
**REQUEST_TIMEOUT** - time before request times out and is retried, in case connection chokes
**ITERATION_CHUNK_SIZE** - purly to report back the status requests for offers and retailers are chunked and after each chunk is finished status report is printed

### db.py
Using sqlalchemy defines ORM declarative metaclasses for each API resource. Expand metaclases to include the parsing method `format_data()` for each resource. As well as implements `upsert` method used in callback function for persisting data adding new records otherwise updating if they already exists.

### sql.py
Implements query to cluster retailers and print them out.
