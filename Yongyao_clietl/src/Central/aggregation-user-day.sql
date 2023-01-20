select customer_id,count(1) as operation_num,datetime,sum(quantity*unit_price) as revenue
from transcations
group by customer_id,datetime