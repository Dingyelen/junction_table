drop table if exists hive.dow_jpnew_w.dws_token_monthly_mf;

create table if not exists hive.dow_jpnew_w.dws_token_monthly_mf(
cal_month date,
money_rmb decimal(32, 4),
users bigint,
pay_users bigint,
token_users bigint,
token_get_by_pay bigint,
token_get bigint,
token_cost bigint,
token_remain bigint,
token_remain_lastmonth bigint, 
price decimal(32, 4), 
diff bigint
);

insert into hive.dow_jpnew_w.dws_token_monthly_mf(
cal_month, money_rmb, users,
pay_users, token_users, token_get_by_pay,
token_get, token_cost, token_remain, 
token_remain_lastmonth, price, diff
)

with currency_rate as(
select currency, currency_time, rate as exchange_rate
from mysql_bi_r."gbsp-bi-bigdata".t_currency_rate
where currency = 'JPY'
), 

event_pay_token_web as
(select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, payment_itemid, 
a.currency, money, b.exchange_rate, 
cast(money * b.exchange_rate * 0.01 as decimal(32, 4)) money_rmb,
cast(split_part(payment_itemid, ';', 2) as bigint) as token_num,
part_date
from hive.dow_jpnew_r.dwd_gserver_payment_live a
left join currency_rate b
on date_format(a.event_time, '%Y-%m') = b.currency_time 
where part_date >= '2024-05-01' and part_date <= cast(current_date as varchar)
and pay_source = 'web'
),

good_config_app as
(select *
from (values
('gold_100001', 660),
('gold_100002', 320),
('gold_100003', 120),
('gold_100004', 60),
('gold_100005', 32),
('gold_100006', 4)) as good_config(payment_itemid, token_num)
),

event_pay_token_app as
(select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, a.payment_itemid, 
a.currency, money, b.exchange_rate, 
cast(money * b.exchange_rate / 100 as decimal(32, 4)) money_rmb,
c.token_num,
part_date
from hive.dow_jpnew_r.dwd_gserver_payment_live a
left join currency_rate b
on date_format(a.event_time, '%Y-%m') = b.currency_time 
left join good_config_app c
on a.payment_itemid = c.payment_itemid
where part_date >= '2024-05-01' and part_date <= cast(current_date as varchar)
and a.payment_itemid in ('gold_100001', 'gold_100002', 'gold_100003', 'gold_100004', 'gold_100005', 'gold_100006')
),

event_pay_token as
(select * from event_pay_token_web
union all
select * from event_pay_token_app
),

user_daily_info_pay as
(select date, role_id, 
sum(money_rmb) as money_rmb, 
count(money_rmb) as pay_count,
sum(token_num) as token_get_by_pay
from event_pay_token
group by 1, 2
),

event_token_item as
(select row_number() over (partition by role_id, part_date order by event_time desc, item_end) as rn,
part_date, event_name, event_time, 
date(event_time) as date, 
role_id, event_type,
reason, reason_id, reason_subid,
item_id, item_num as change_num,
item_end as change_after,
part_date
from hive.dow_jpnew_r.dwd_gserver_itemchange_live
where part_date >= '2024-05-01' and part_date <= cast(current_date as varchar)
and item_id = '2'
and reason !='638'
),

user_daily_info_token1 as
(select date, role_id, 
sum(case when event_type = 'gain' then change_num else null end) as token_get,
sum(case when event_type = 'cost' then change_num else null end) as token_cost
from event_token_item
group by 1, 2
),

user_daily_info_token2 as
(select *
from event_token_item
where rn = 1
),

user_daily_info_token as
(select a.*, b.change_after
from user_daily_info_token1 a
left join user_daily_info_token2 b
on a.date = b.date and a.role_id = b.role_id
),

user_daily_info as
(select row_number() over (partition by a.role_id, date_trunc('month', a.date) order by a.date desc) as rn,
a.*, b.token_get, b.token_cost, b.change_after as token_remain
from user_daily_info_pay a
left join user_daily_info_token b
on a.date = b.date and a.role_id = b.role_id
),

user_month_info1 as
(select date_trunc('month', date) as month, role_id, 
sum(money_rmb) as money_rmb, 
sum(token_get_by_pay) as token_get_by_pay, 
sum(token_get) as token_get, 
sum(token_cost) as token_cost
from user_daily_info
group by 1, 2
),

user_month_info2_1 as
(select date_trunc('month', date) as month, role_id, token_remain
from user_daily_info
where rn = 1
),

user_month_info2_2 as
(select row_number() over (partition by a.role_id, months.cal_month order by a.month desc) as rn,
a.*, months.cal_month
from user_month_info2_1 a
cross join unnest(sequence(a.month, date_trunc('month', current_date), interval '1' month)) as months(cal_month)
),

user_month_info2 as
(select cal_month, role_id, token_remain
from user_month_info2_2
where rn = 1
),

user_month_info as
(select a.*,
b.money_rmb,
b.token_get_by_pay,
b.token_get,
b.token_cost
from user_month_info2 a
left join user_month_info1 b
on a.cal_month = b.month and a.role_id = b.role_id
),

month_info as
(select cal_month, 
sum(money_rmb) as money_rmb,
count(role_id) as users,
count(case when money_rmb > 0 then role_id else null end) as pay_users,
count(case when token_get > 0 or token_cost > 0 then role_id else null end) as token_users,
sum(token_get_by_pay) as token_get_by_pay,
sum(token_get) as token_get,
sum(token_cost) as token_cost,
sum(token_remain) as token_remain
from user_month_info
group by 1
),

month_info_pirce as
(select *,
lag(token_remain, 1) over (order by cal_month) as token_remain_lastmonth,
money_rmb / (token_get + lag(token_remain, 1, 0) over (order by cal_month)) as price
from month_info
)

select cal_month, money_rmb, users,
pay_users, token_users, token_get_by_pay,
token_get, token_cost, token_remain, 
token_remain_lastmonth, price, 
token_get + coalesce(token_remain_lastmonth, 0) - token_cost - token_remain as diff
from month_info_pirce
order by 1