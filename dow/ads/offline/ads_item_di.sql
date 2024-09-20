###
create table if not exists hive.dow_jpnew_w.ads_item_di
(date date,
zone_id varchar,
channel varchar,
vip_level bigint, 
item_id varchar,
item_add bigint,
item_cost bigint,
users bigint,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.dow_jpnew_w.ads_item_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.dow_jpnew_w.ads_item_di
(date, zone_id, channel, vip_level, 
item_id, item_add, item_cost, users,
part_date)

with dws_item_daily as(
select a.date, a.role_id, item_detail, part_date
from hive.dow_jpnew_w.dws_item_snapshot_di a
left join hive.dow_jpnew_w.dws_user_info_di b
on a.role_id = b.role_id
where part_date>=$start_date
and part_date<=$end_date
and b.is_test is null
),

dws_unnest as(
select date, part_date, role_id, item_detail_t
from dws_item_daily
cross join unnest(item_detail) as addinfo(item_detail_t)
),

dws_detail as(
select date, part_date, role_id, 
cast(json_extract(item_detail_t, '$.item_id') as varchar) as item_id, 
cast(json_extract(item_detail_t, '$.item_add') as bigint) as item_add, 
cast(json_extract(item_detail_t, '$.item_cost') as bigint) as item_cost, 
cast(json_extract(item_detail_t, '$.item_end') as bigint) as item_end
from dws_unnest
), 

dws_item_daily_join as(
select a.date, a.part_date, a.role_id, 
a.item_id, a.item_add, a.item_cost, a.item_end, 
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.moneyrmb_ac, b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel, b.vip_level, 
date_diff('day', b.install_date, a.date) as retention_day,
date_diff('day', b.firstpay_date, a.date) as pay_retention_day,
date_diff('day', b.install_date, b.firstpay_date) as firstpay_interval_days
from dws_detail a
left join hive.dow_jpnew_w.dws_user_info_di b
on a.role_id = b.role_id
),

core_daily_agg as(
select date, part_date, zone_id, channel, 
vip_level, item_id,
sum(item_add) as item_add,
sum(item_cost) as item_cost,
count(distinct role_id) as users
from dws_item_daily_join
group by 1, 2, 3, 4, 5, 6
)

select date, zone_id, channel, vip_level, 
item_id, item_add, item_cost, users,
part_date
from core_daily_agg
;
###