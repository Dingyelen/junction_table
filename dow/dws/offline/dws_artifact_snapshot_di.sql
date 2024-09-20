###
create table if not exists hive.dow_jpnew_w.dws_artifact_snapshot_di(
date date, 
role_id varchar, 
artifact1_detail array(varchar), 
artifact2_detail array(varchar), 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.dow_jpnew_w.dws_artifact_snapshot_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.dow_jpnew_w.dws_artifact_snapshot_di(
date, role_id, artifact1_detail, artifact2_detail, part_date
)

with base_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, null as adid, 
cast(channel as varchar) as channel, 
cast(zone_id as varchar) as zone_id, 
cast(guild_id as varchar) as alliance_id,  
'dow_jp' as app_id, 
vip_level, level, rank, 
payment_itemid as good_id, 
currency, money, exchange_rate, money_rmb, 
online_time
from hive.dow_jpnew_r.dwd_merge_base_live
where part_date >= $start_date
and part_date <= $end_date
), 

hero_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, null as adid, 
zone_id, guild_id as alliance_id, 
vip_level, level, rank, 
artifact1_id, artifact1_level, 
artifact2_id, artifact2_level
from hive.dow_jpnew_r.dwd_gserver_herosnap_live
where part_date >= $start_date
and part_date <= $end_date
and (artifact1_id != 0 or artifact2_id != 0)
), 

daily_gserver_info as(
select part_date, date, role_id
from base_log
group by 1, 2, 3
), 

daily_hero_change_log_1 as(
select part_date, role_id, artifact1_id, 
max(artifact1_level) as artifact1_level
from hero_log
where artifact1_id != 0 
group by 1, 2, 3
), 

daily_hero_end_turn_array_1 as(
select part_date, role_id, 
json_object('artifact1_id': artifact1_id, 'artifact1_level': artifact1_level) as artifact1_detail
from daily_hero_change_log_1
), 

daily_hero_array_info_1 as(
select part_date, role_id, 
array_agg(artifact1_detail) as artifact1_detail
from daily_hero_end_turn_array_1 
group by 1, 2
), 

daily_hero_change_log_2 as(
select part_date, role_id, artifact2_id, 
max(artifact2_level) as artifact2_level
from hero_log
where artifact2_id != 0 
group by 1, 2, 3
), 

daily_hero_end_turn_array_2 as(
select part_date, role_id, 
json_object('artifact2_id': artifact2_id, 'artifact2_level': artifact2_level) as artifact2_detail
from daily_hero_change_log_2
), 

daily_hero_array_info_2 as(
select part_date, role_id, 
array_agg(artifact2_detail) as artifact2_detail
from daily_hero_end_turn_array_2 
group by 1, 2
)

select a.date, a.role_id, b.artifact1_detail, c.artifact2_detail, a.part_date
from daily_gserver_info a
left join daily_hero_array_info_1 b
on a.part_date = b.part_date
and a.role_id = b.role_id
left join daily_hero_array_info_2 c
on a.part_date = c.part_date
and a.role_id = c.role_id;
###