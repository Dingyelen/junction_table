-- a_live_dwdmergebaselive
select count(distinct a.role_id) as active_user, 
count(distinct case when event_name = 'Payment' then a.role_id else null end) as pay_user, 
sum(a.money * 0.052102) as moneyrmb_fixed, 
sum(a.money * z.rate) as money_rmb 
from hive.dow_jpnew_r.dwd_merge_base_live a
left join hive.dow_jpnew_w.dim_gserver_base_roleid b
on a.role_id = b.role_id
left join mysql_bi_r."gbsp-bi-bigdata".t_currency_rate z
on a.currency = z.currency and date_format(date(a.part_date), '%Y-%m') = z.currency_time 
where part_date = date_format(current_date, '%Y-%m-%d')
and b.role_id is null;

-- a_live_dwduserinfolive
with log as(
select 
concat(date_format(install_ts, '%H:'), lpad(cast(cast(date_format(install_ts, '%i') as bigint)/10*10 as varchar), 2, '0')) as install_hour, 
count(distinct a.role_id) as new_user
from kudu.dow_jpnew_r.dwd_user_info_live a
left join hive.dow_jpnew_w.dim_gserver_base_roleid b
on a.role_id = b.role_id
where install_date = date_format(current_date, '%Y-%m-%d')
and b.role_id is null
group by 1
), 

data_cube as(
select concat(hour,':', min) as time 
from(select lpad(cast(t.hour as varchar), 2, '0') as hour
from unnest(sequence(0, 23, 1)) as t(hour))
cross join(
select lpad(cast(t.min as varchar), 2, '0') as min
from unnest(sequence(0, 50, 10)) as t(min))
)

select a.time as time, new_user, 
coalesce(sum(new_user) over(order by time), 0) as newuser_ac, 
row_number() over(order by time) as rank
from data_cube a 
left join log b
on a.time = b.install_hour;

-- a_live_dwdgserverpaymentlive
with log as(
select 
concat(date_format(event_time, '%H:'), lpad(cast(cast(date_format(event_time, '%i') as bigint)/10*10 as varchar), 2, '0')) as event_time, 
sum(a.money * 0.052102) as moneyrmb_fixed, 
sum(a.money * z.rate) as money_rmb 
from hive.dow_jpnew_r.dwd_gserver_payment_live a
left join hive.dow_jpnew_w.dim_gserver_base_roleid b
on a.role_id = b.role_id
left join mysql_bi_r."gbsp-bi-bigdata".t_currency_rate z
on a.currency = z.currency and date_format(date(a.part_date), '%Y-%m') = z.currency_time 
where part_date = date_format(current_date, '%Y-%m-%d')
and b.role_id is null
group by 1
), 

data_cube as(
select concat(hour,':', min) as time 
from(select lpad(cast(t.hour as varchar), 2, '0') as hour
from unnest(sequence(0, 23, 1)) as t(hour))
cross join(
select lpad(cast(t.min as varchar), 2, '0') as min
from unnest(sequence(0, 50, 10)) as t(min))
)

select a.time as time, money_rmb, moneyrmb_fixed, 
coalesce(sum(money_rmb) over(order by time), 0) as moneyrmb_ac, 
coalesce(sum(moneyrmb_fixed) over(order by time), 0) as moneyrmbfixed_ac, 
row_number() over(order by time) as rank
from data_cube a 
left join log b
on a.time = b.event_time;

-- a_live_dwdgserverpaymentlive_2
select 
c.payment_cate, c.payment_name, 
sum(a.money * 0.052102) as moneyrmb_fixed, 
sum(a.money * z.rate) as money_rmb,  
count(distinct a.role_id) as pay_user, 
count(*) as pay_count 
from hive.dow_jpnew_r.dwd_gserver_payment_live a
left join hive.dow_jpnew_w.dim_gserver_base_roleid b
on a.role_id = b.role_id
left join hive.dow_jpnew_w.dim_gserver_payment_paymentitemid c
on a.payment_itemid = c.payment_itemid
left join mysql_bi_r."gbsp-bi-bigdata".t_currency_rate z
on a.currency = z.currency and date_format(date(a.part_date), '%Y-%m') = z.currency_time 
where part_date = date_format(current_date, '%Y-%m-%d')
and b.role_id is null
group by 1, 2;

select count(distinct a.role_id) as active_user, 
count(distinct case when event_name = 'Payment' then a.role_id else null end) as pay_user, 
sum(a.money * 0.052102) as moneyrmb_fixed, 
sum(a.money * z.rate) as money_rmb 
from hive.dow_jpnew_r.dwd_merge_base_live a
left join hive.dow_jpnew_w.dim_gserver_base_roleid b
on a.role_id = b.role_id
left join mysql_bi_r."gbsp-bi-bigdata".t_currency_rate z
on a.currency = z.currency and date_format(date(a.part_date), '%Y-%m') = z.currency_time 
where part_date = date_format(current_date, '%Y-%m-%d')
and b.role_id is null;

-- a_live_dwdmergebaselive_2
with base_log as(
select distinct a.part_date, date(a.event_time) as date, a.role_id
from hive.dow_jpnew_r.dwd_merge_base_live a
left join hive.dow_jpnew_w.dim_gserver_base_roleid b
on a.role_id = b.role_id
where part_date >= date_format(date_add('day', -10, current_date), '%Y-%m-%d')
and b.role_id is null), 

res as(
select part_date, date, role_id, 
-- lag(date, 1, date_add('day', -8, current_date)) over(partition by role_id order by date), 
date_diff('day', lag(date, 1, date_add('day', -8, current_date)) over(partition by role_id order by date), date) as return_days
from base_log
)

select count(distinct role_id) as return_users
from res
where part_date >= date_format(current_date, '%Y-%m-%d')
and return_days > 7;


-- a_live_dwdmergebaselive_3
with base_log as(
select part_date, event_name, event_time, 
-- date_trunc('hour', event_time) as hour,
concat(date_format(event_time, '%H:'), lpad(cast(cast(date_format(event_time, '%i') as bigint)/10*10 as varchar), 2, '0')) as time, 
cast(date_format(event_time, concat('%Y-%m-%d %H:', lpad(cast(cast(date_format(event_time, '%i') as bigint)/10*10 as varchar), 2, '0'), ':00')) as timestamp) as datetime, 
date(event_time) as date, a.role_id
from hive.dow_jpnew_r.dwd_merge_base_live a
left join hive.dow_jpnew_w.dim_gserver_base_roleid b
on a.role_id = b.role_id
where part_date >= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
and b.role_id is null
), 

base_agg as(
select datetime, role_id, 
array_agg(event_name order by event_time) as events,
element_at(array_agg(event_name order by event_time), -1) as last_event
from base_log
group by 1, 2
), 

data_cube01 as(
select role_id, datetime, date_add('minute', t.hour * 60 + s.min* 10, datetime) as time_agg
from base_agg
cross join unnest(sequence(0, 47, 1)) as t(hour), unnest(sequence(0, 5, 1)) as s(min)
), 

data_log as(
select a.datetime, a.role_id, a.last_event, date(b.time_agg) as date_agg, b.time_agg, 
(case when b.time_agg != a.datetime and a.last_event = 'logout' then 'nonact' else 'act' end) as isact
from base_agg a
left join data_cube01 b
on a.role_id = b.role_id and a.datetime = b.datetime
), 

data_log_last as
(select date_agg, time_agg, role_id, 
map_agg(datetime, last_event) as last_events,
element_at(array_agg(isact order by datetime), -1) as isact_final
from data_log
group by 1, 2, 3
),

active_select as(
select date_agg, time_agg, role_id
from data_log_last
where isact_final = 'act'
and date_agg = current_date
), 

data_cube02 as(
select date_add('minute', t.hour * 60 + s.min * 10, cast(current_date as timestamp)) as time_agg
from unnest(sequence(0, 23, 1)) as t(hour), unnest(sequence(0, 5, 1)) as s(min)
), 

activeac_log as(
select a.time_agg, b.role_id
from data_cube02 a
left join active_select b
on a.time_agg >= b.time_agg
), 

data_agg as(
select time_agg, 
count(distinct role_id) as active_users
from active_select
group by 1
), 

activeac_agg as(
select time_agg, 
count(distinct role_id) as activeusers_ac
from activeac_log
group by 1
), 

res as(
select a.time_agg, date_diff('minute', a.time_agg, cast(date_format(current_timestamp, '%Y-%m-%d %H:%i:%s') as timestamp)) as min_diff, 
concat(date_format(a.time_agg, '%H:'), lpad(cast(cast(date_format(a.time_agg, '%i') as bigint)/10*10 as varchar), 2, '0')) as time, 
a.activeusers_ac, b.active_users
from activeac_agg a
left join data_agg b
on a.time_agg = b.time_agg
)

select time_agg, time, 
activeusers_ac, 
(case when min_diff<0 then null else active_users end) as active_users
from res