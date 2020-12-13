import db
from prettytable import PrettyTable

sql = """
select
	retailer_name,
	segment,
	RANK() OVER (PARTITION BY segment ORDER BY avg_product_price desc) as retailer_rank,
    ROUND(avg_product_price, 2) as avg_product_price_usd
from (
	select
		retailers."name" as retailer_name,
		AVG(offers.price_usd) as avg_product_price,
		case when AVG(offers.price_usd) < 100 then 'Normal'
			 when AVG(offers.price_usd) between 100 and 200 then 'High End'
			 else 'Very High End' end as segment
	from offers
	join retailers on retailers.id = offers.retailer_id
	join products on products.id = offers.product_id
	where lower(products.brand_names) similar to '%%(nike|adidas|vans)%%'
	group by retailers."name", retailers.id
) as base
where segment != 'Very High End'
order by segment, retailer_rank
"""

session = db.connect_db()


with session.connect() as con:
    rs = con.execute(sql)
    table = PrettyTable(rs.keys())
    table.align['retailer_name'] = 'l'
    for row in rs:
        table.add_row(row)

print(table.get_string(border=True))