###
drop table if exists hive.mushroom2_tw_w.ads_item_end_df;

create table if not exists hive.mushroom2_tw_w.ads_item_end_df
(role_id varchar,
item_end bigint,
item_id varchar
)
with(partitioned_by = array['item_id']);

insert into hive.mushroom2_tw_w.ads_item_end_df
(role_id, item_end, item_id)

with dws_item_daily as(
select date, role_id, item_detail, part_date
from hive.mushroom2_tw_w.dws_item_snapshot_di
),

dws_unnest as(
select date, role_id, item_detail_t
from dws_item_daily
cross join unnest(item_detail) as addinfo(item_detail_t)
),

dws_detail as(
select date, role_id, 
cast(json_extract(item_detail_t, '$.item_id') as varchar) as item_id, 
-- cast(json_extract(item_detail_t, '$.item_add') as bigint) as item_add, 
-- cast(json_extract(item_detail_t, '$.item_cost') as bigint) as item_cost, 
cast(json_extract(item_detail_t, '$.item_end') as bigint) as item_end
from dws_unnest
), 

dws_select as(
select *
from dws_detail
where item_id >= '13000'
and item_id <= '14000'
), 

dws_item_daily_join as(
select a.date, a.role_id, 
a.item_id, 
-- a.item_add, a.item_cost, 
a.item_end, 
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.moneyrmb_ac, b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id, b.channel, b.vip_level, 
date_diff('day', b.install_date, a.date) as retention_day,
date_diff('day', b.firstpay_date, a.date) as pay_retention_day,
date_diff('day', b.install_date, b.firstpay_date) as firstpay_interval_days
from dws_select a
left join hive.mushroom2_tw_w.dws_user_info_di b
on a.role_id = b.role_id
),

core_daily_agg as(
select role_id, item_id, 
last_value(item_end) over(partition by role_id, item_id order by date) as item_end
from dws_item_daily_join
)

select role_id, item_end, item_id
from core_daily_agg
;
###