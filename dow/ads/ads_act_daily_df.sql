drop table if exists hive.dow_jpnew_w.ads_act_daily_df;

create table if not exists hive.dow_jpnew_w.ads_act_daily_df(
date date, 
part_date varchar, 
zone_id varchar, 
act_tag varchar, 
retention_day bigint, 
active_users bigint, 
money_rmb decimal(36, 2), 
act_rmb decimal(36, 2), 
item_cost bigint, 
is_act bigint
);

insert into hive.dow_jpnew_w.ads_act_daily_df(
date, part_date, zone_id, act_tag, retention_day, 
active_users, money_rmb, act_rmb, item_cost, is_act
)

with daily_info as(
select date, part_date, a.role_id, b.zone_id, 
money_rmb
from hive.dow_jpnew_w.dws_user_daily_di a
left join hive.dow_jpnew_w.dws_user_info_di b
on a.role_id = b.role_id
where b.is_test is null
and (part_date between '2024-01-13' and '2024-01-19'
or part_date between '2024-04-21' and '2024-04-25'
or part_date between '2024-05-06' and '2024-05-09')
), 

costitem_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
(case when part_date between '2024-05-06' and '2024-05-09' then '0506-0509'
when part_date between '2024-04-21' and '2024-04-25' then '0421-0425' 
when part_date between '2024-01-13' and '2024-01-19' then '0113-0119' 
else 'unknow' end) as act_tag, 
cast(zone_id as varchar) as zone_id, 
role_id, open_id, null as adid, 
item_id, item_num as item_cost
from hive.dow_jpnew_r.dwd_gserver_itemchange_live
where (part_date between '2024-01-13' and '2024-01-19'
or part_date between '2024-04-21' and '2024-04-25'
or part_date between '2024-05-06' and '2024-05-09')
and event_type = 'cost'
), 

cost_info as(
select date, part_date, act_tag, role_id, zone_id, 
sum(item_cost) as item_cost
from costitem_log
where item_id = '11437'
group by 1, 2, 3, 4, 5
), 

currency_rate as(
select currency, currency_time, rate as exchange_rate
from mysql_bi_r."gbsp-bi-bigdata".t_currency_rate
where currency = 'JPY'
), 

payment_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
(case when part_date between '2024-05-06' and '2024-05-09' then '0506-0509'
when part_date between '2024-04-21' and '2024-04-25' then '0421-0425' 
when part_date between '2024-01-13' and '2024-01-19' then '0113-0119' 
else 'unknow' end) as act_tag, 
role_id, open_id, null as adid, 
zone_id, 
vip_level, level, 
payment_itemid, a.money * b.exchange_rate as money_rmb
from hive.dow_jpnew_r.dwd_gserver_payment_live a
left join currency_rate b
on date_format(a.event_time, '%Y-%m') = b.currency_time 
where payment_itemid in ('gold_350', 'gold_351', 'gold_352', 'gold_353')
and (part_date between '2024-01-13' and '2024-01-19'
or part_date between '2024-04-21' and '2024-04-25'
or part_date between '2024-05-06' and '2024-05-09'
)), 

payment_info as(
select date, part_date, act_tag, role_id, zone_id, 
sum(money_rmb) as act_rmb
from payment_log
group by 1, 2, 3, 4, 5
), 

res as(
select a.date, a.role_id, a.zone_id, b.act_tag, 
a.money_rmb, c.act_rmb, b.item_cost, 
(case when b.item_cost > 0 then 1 else null end) as is_act, 
a.part_date
from daily_info a
left join cost_info b
on a.role_id = b.role_id and a.date = b.date
left join payment_info c
on a.role_id = c.role_id and a.date = c.date
), 

res_group as(
select date, part_date, zone_id, act_tag, 
count(distinct role_id) as active_users, 
sum(money_rmb) as money_rmb, 
sum(act_rmb) as act_rmb, 
sum(item_cost) as item_cost, 
sum(is_act) as is_act
from res
group by 1, 2, 3, 4
)

select date, part_date, zone_id, act_tag, 
row_number() over(partition by act_tag, zone_id order by date) as retention_day, 
active_users, money_rmb, act_rmb, item_cost, is_act
from res_group