select product_description,count(1) as sum_operations,sum(unit_price*quantity) as total_revenue,avg(unit_price*quantity) as daily_revenue
from transcations
group by product_description