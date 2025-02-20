create table anna as
(select * from order_details o join menu_items m 
on o.item_id=m.menu_item_id);

select * from anna;

-- 1.duplicates

with cte7 as(
select * , 
row_number() over(partition by order_details_id,order_id,item_id) as row_num from anna
)
select * from cte7 ;

-- No duplicates in the anna table 
-- if any duplicates, need to create staging table and delete duplicates

-- 2.standardize
-- check all names in proper format

-- 3.checking nulls

select * from menu_items
where menu_item_id is null or item_name is null or category is null or price is null;

select * from order_details
where order_details_id is null or order_id is null or
 order_date is null or order_time is null or item_id is null;
 
 -- here delete null rows by joining by item_id the null row is eliminated
 
 select * from anna where menu_item_id is null or item_name is null or category is null or price is null or
 order_details_id is null or order_id is null or
 order_date is null or order_time is null or item_id is null;
 
 -- ANALYSIS
 
  select category,avg(price),sum(price),count(price) available_varieties from menu_items
  group by category;
  
  -- max price , min price ?
  
  -- overall max,min price
  
  with cte2 as
  (select max(price) max_price from menu_items)
  select * from cte2 join menu_items m
  on cte2.max_price=m.price;
  
    with cte4 as
  (select min(price) min_price from menu_items)
  select * from cte4 join menu_items m
  on cte4.min_price=m.price;
  
  select * from menu_items;
 
   with cte3 as
  (select max(price) max_price,min(price) min_price from menu_items
  where category = 'Asian')
  select menu_item_id,item_name,category,price from cte3 join menu_items m
  on cte3.max_price=m.price or cte3.min_price=m.price
  where category="Asian";
  
  with cte5 as
 (select max(price) max_price,min(price) min_price from menu_items
  group by category),
  cte6 as
  (
   select * from cte5 join menu_items m
  on cte5.max_price=m.price or cte5.min_price=m.price)
  select * from cte6;
  
  -- CRCT max,min price item by category
  
  with cte8 as
  (select distinct category,
  max(price) over(partition by category) as max_price,
  min(price) over(partition by category) as min_price
  from anna)
  select distinct a.category,c.max_price,c.min_price,
  a.item_id,a.item_name,a.price from cte8 c join anna a
  on c.category=a.category and c.max_price=a.price
  or c.category=a.category and c.min_price=a.price
  order by a.category,a.price desc;
  
  select distinct item_id,item_name,category,sum(price) over(partition by item_id) total_revenue,
  count(item_id) over(partition by item_id) total_sales_count from anna
  order by total_revenue desc;
  
  select category,sum(price),count(category) from anna
  group by category
  order by sum(price) desc;
  
  with cte10 as(
select order_id,sum(price) sales,count(order_details_id) no_of_dishes
 from anna1
group by order_id
order by sum(price) desc
limit 5)
select distinct a.order_date,a.order_time,c.order_id,c.sales,c.no_of_dishes
from cte10 c join anna1 a on
c.order_id=a.order_id;
  
select * ,week(order_date),dayname(order_date) from anna;

create table anna1 like anna;

alter table anna1
add week smallint,
add day varchar(45);

select * from anna1;

insert anna1
select *,week(order_date),dayname(order_date) from anna;

alter table anna1
modify week smallint
after order_date;

alter table anna1
modify day varchar(45)
after week;

select * from anna1
order by order_details_id;

select *, hour(order_time) as hr from anna1;

-- making time interval for EDA

alter table anna1
add time_interval varchar(45);
update anna1
set time_interval  =
case
when hour(order_time) = 10 then '10-11'
when hour(order_time) = 11 then '11-12'
when hour(order_time) = 12 then '12-13'
when hour(order_time) = 13 then '13-14'
when hour(order_time) = 14 then '14-15'
when hour(order_time) = 15 then '15-16'
when hour(order_time) = 16 then '16-17'
when hour(order_time) = 17 then '17-18'
when hour(order_time) = 18 then '18-19'
when hour(order_time) = 19 then '19-20'
when hour(order_time) = 20 then '20-21'
when hour(order_time) = 21 then '21-22'
when hour(order_time) = 22 then '22-23'
when hour(order_time) = 23 then '23-24'
end;

alter table anna1
modify time_interval varchar(45)
after order_time;

select * from anna1
order by time_interval;

select time_interval,sum(price) sales from anna1
group by time_interval
order by sales desc;

select week,sum(price) sales from anna1
group by week
order by sales desc;

select day,sum(price) sales from anna1
group by day
order by sales desc;

select day,category,sum(price) sales from anna1
group by day,category
order by sales desc;

select time_interval,category,sum(price) sales from anna1
group by time_interval,category
order by time_interval,category;