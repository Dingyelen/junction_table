drop table if exists hive.mushroom2_tw_w.dws_token_info_mf;

create table if not exists hive.mushroom2_tw_w.dws_token_info_mf(
cal_month date,
role_id varchar, 
money decimal(32, 4),
token_get_by_pay bigint,
token_get bigint,
token_cost bigint,
token_end bigint,
token_end_lastmonth bigint, 
price decimal(32, 4)
);

insert into hive.mushroom2_tw_w.dws_token_info_mf(
cal_month, role_id, 
money, token_get_by_pay,
token_get, token_cost, token_end, token_end_lastmonth, price
)

with event_pay_token_web as(
select part_date, event_name, event_time, 
date(event_time) as date, 
date_trunc('month', date(event_time)) as month, 
role_id, payment_itemid, 
currency, money, cast(split_part(payment_itemid, ';', 2) as bigint) as token_num, part_date
from hive.mushroom2_tw_r.dwd_gserver_payment_live
where part_date >= '2024-05-01' and part_date <= cast(current_date as varchar)
and pay_source = 'web'
),

good_config_app as(
select *
from (values
('gold_100001', 660),
('gold_100002', 320),
('gold_100003', 120),
('gold_100004', 60),
('gold_100005', 32),
('gold_100006', 4)) as good_config(payment_itemid, token_num)
),

event_pay_token_app as(
select part_date, event_name, event_time, 
date(event_time) as date, 
date_trunc('month', date(event_time)) as month, 
role_id, a.payment_itemid, 
a.currency, money, c.token_num, part_date
from hive.mushroom2_tw_r.dwd_gserver_payment_live a
left join good_config_app c
on a.payment_itemid = c.payment_itemid
where part_date >= '2024-05-01' and part_date <= cast(current_date as varchar)
and a.payment_itemid in ('gold_100001', 'gold_100002', 'gold_100003', 'gold_100004', 'gold_100005', 'gold_100006')
),

event_pay_token as(
select * from event_pay_token_web
union all
select * from event_pay_token_app
),

user_daily_info_pay as(
select month, role_id, 
sum(money) as money, 
count(money) as pay_count,
sum(token_num) as token_get_by_pay
from event_pay_token
group by 1, 2
),

event_token_item as(
select part_date, event_name, event_time, 
date(event_time) as date,
date_trunc('month', date(event_time)) as month,  
role_id, event_type,
reason, reason_id, reason_subid,
item_id, item_num, item_end as token_end, 
row_number() over (partition by role_id, date_trunc('month', date(event_time)) order by event_time desc, item_end) as rn, 
part_date
from hive.mushroom2_tw_r.dwd_gserver_itemchange_live
where part_date >= '2024-05-01' and part_date <= cast(current_date as varchar)
and item_id = '2'
and reason !='638'
),

user_daily_info_token1 as
(select month, role_id, 
sum(case when event_type = 'gain' then item_num else null end) as token_get,
sum(case when event_type = 'cost' then item_num else null end) as token_cost
from event_token_item
group by 1, 2
),

user_daily_info_token2 as
(select month, role_id, token_end
from event_token_item
where rn = 1
), 

user_daily_info_token as
(select a.month, a.role_id, 
a.token_get, a.token_cost, b.token_end
from user_daily_info_token1 a
left join user_daily_info_token2 b
on a.month = b.month and a.role_id = b.role_id
),

start_month_info as(
select role_id, 
min(month) as start_month
from user_daily_info_pay
group by 1
),

month_cube as
(select a.start_month, a.role_id, months.cal_month
from start_month_info a
cross join unnest(sequence(a.start_month, date_trunc('month', current_date), interval '1' month)) as months(cal_month)
),

user_month_info as
(select a.cal_month, a.role_id, 
c.money, c.token_get_by_pay,
b.token_get, b.token_cost, 
coalesce(b.token_end, lag(b.token_end, 1, null) ignore nulls over(partition by a.role_id order by a.cal_month)) as token_end 
from month_cube a
left join user_daily_info_token b
on a.cal_month = b.month and a.role_id = b.role_id
left join user_daily_info_pay c
on a.cal_month = c.month and a.role_id = c.role_id
)

select cal_month, role_id, 
money, token_get_by_pay,
token_get, token_cost, token_end, 
lag(token_end, 1) over (order by cal_month) as token_end_lastmonth,
money / (token_get + lag(token_end, 1, 0) over (order by cal_month)) as price
from user_month_info