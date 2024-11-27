create table if not exists hive.dow_jpnew_w.dws_hero_snapshot_di(
date date, 
role_id varchar, 
hero_detail array(varchar), 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.dow_jpnew_w.dws_hero_snapshot_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.dow_jpnew_w.dws_hero_snapshot_di(
date, role_id, hero_detail, part_date
)

with base_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, device_id, 
channel, zone_id, alliance_id,  
'dow_jp' as app_id, 
vip_level, level, rank_level, power
from hive.dow_jpnew_r.dwd_merge_base_live
where part_date >= $start_date
and part_date <= $end_date
), 

daily_gserver_info as(
select part_date, date, role_id
from base_log
group by 1, 2, 3
), 

upgraderare_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
cast(hero as varchar) as hero_id, 
cast(substring(cast(fromrare as varchar), 3, 1) as bigint) as hero_star, 
costchip as chip_cost, remainchip as chip_end, newskill as new_skill
from hive.dow_jpnew_r.dwd_gserver_upgraderare_live
where part_date >= $start_date
and part_date <= $end_date
), 

upgraderare_cal as(
select date, part_date, role_id, hero_id, 
max(hero_star) as hero_star,
max(chip_cost) as chip_cost
from upgraderare_log
group by 1, 2, 3, 4
),

upgradehero_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
cast(hero as varchar) as hero_id, 
fromlv as original_level, tolv as hero_level
from hive.dow_jpnew_r.dwd_gserver_upgradehero_live
where part_date >= $start_date
and part_date <= $end_date
), 

upgradehero_cal as(
select date, part_date, role_id, hero_id, 
max(hero_level) as hero_level
from upgradehero_log
group by 1, 2, 3, 4
), 

cal_info as(
select 
coalesce(a.date, b.date) as date, 
coalesce(a.part_date, b.part_date) as part_date, 
coalesce(a.role_id, b.role_id) as role_id, 
coalesce(a.hero_id, b.hero_id) as hero_id, 
b.hero_level, a.hero_star, a.chip_cost
from upgraderare_cal a
full join upgradehero_cal b
on a.part_date = b.part_date 
and a.role_id = b.role_id 
and a.hero_id = b.hero_id
), 

daily_hero_daily as(
select part_date, role_id, 
json_object('hero_id': hero_id, 'hero_level': hero_level, 'hero_star': hero_star, 'chip_cost': chip_cost) as hero_detail
from cal_info
), 

daily_hero_array_info as(
select part_date, role_id, 
array_agg(hero_detail) as hero_detail
from daily_hero_daily 
group by 1, 2
)

select a.date, a.role_id, b.hero_detail, a.part_date
from daily_gserver_info a
left join daily_hero_array_info b
on a.part_date = b.part_date
and a.role_id = b.role_id;