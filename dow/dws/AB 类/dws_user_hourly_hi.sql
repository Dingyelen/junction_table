###
create table if not exists hive.dow_jpnew_w.dws_user_hourly_hi
(date date,
hour timestamp,
role_id varchar,
zone_id varchar,
channel varchar,
exchange_rate double,
pay_count bigint,
money double, 
money_rmb double,
events array(varchar),
last_event varchar,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.dow_jpnew_w.dws_user_hourly_hi 
where part_date >= $start_date
and part_date <= $end_date;

insert into  hive.dow_jpnew_w.dws_user_hourly_hi
(date, hour, role_id, 
zone_id, channel, exchange_rate, 
pay_count, money, money_rmb,
events, last_event,
part_date)

with currency_rate as(
select currency, currency_time, rate * 0.01 as exchange_rate
from mysql_bi_r."gbsp-bi-bigdata".t_currency_rate
where currency = 'JPY'
), 

base_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, device_id, 
channel, zone_id, alliance_id,  
'dow_jp' as app_id, 
vip_level, level, rank_level, power, 
payment_itemid, a.currency, a.money, b.exchange_rate, 
a.money * b.exchange_rate as money_rmb, 
online_time
from hive.dow_jpnew_r.dwd_merge_base_live a
left join currency_rate b
on date_format(a.event_time, '%Y-%m') = b.currency_time 
where part_date >= $start_date
and part_date <= $end_date
), 

exchange_info as(
select part_date, 
min(exchange_rate) as exchange_rate
from base_log
where event_name = 'Payment'
group by 1
), 

daily_gserver_info as(
select part_date, 
date(part_date) as date, 
date_trunc('hour', event_time) as hour,
role_id, app_id, 
zone_id, channel,
sum(money) as money, 
sum(money_rmb) as money_rmb, 
sum(case when event_name = 'Payment' then 1 else null end) as pay_count,
array_agg(event_name order by event_time) as events,
element_at(array_agg(event_name order by event_time), -1) as last_event
from base_log
group by 1, 2, 3, 4, 5, 6, 7
)

select a.date, a.hour, a.role_id, a.zone_id, a.channel, b.exchange_rate, 
a.pay_count, a.money, a.money_rmb, a.events, a.last_event, a.part_date
from daily_gserver_info a
left join exchange_info b
on a.part_date = b.part_date
;
###
