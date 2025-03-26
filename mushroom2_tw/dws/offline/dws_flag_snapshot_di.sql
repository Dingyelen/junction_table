###
create table if not exists hive.mushroom2_tw_w.dws_flag_snapshot_di(
date date, 
role_id varchar, 
flag_detail array(varchar), 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.mushroom2_tw_w.dws_flag_snapshot_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.mushroom2_tw_w.dws_flag_snapshot_di(
date, role_id, flag_detail, part_date
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
from hive.mushroom2_tw_r.dwd_merge_base_live
where part_date >= $start_date
and part_date <= $end_date
), 

hero_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, null as adid, 
zone_id, guild_id as alliance_id, 
vip_level, level, rank, 
flag_id, flag_level, 
flag_star, flag_starran, flag_awake
from hive.mushroom2_tw_r.dwd_gserver_herosnap_live
where part_date >= $start_date
and part_date <= $end_date
and flag_id != 0
), 

daily_gserver_info as(
select part_date, date, role_id
from base_log
group by 1, 2, 3
), 

daily_hero_change_log as(
select part_date, role_id, flag_id, 
max(flag_level) as flag_level, 
max(flag_star) as flag_star, 
max(flag_awake) as flag_awake
from hero_log
group by 1, 2, 3
), 

daily_hero_end_turn_array as(
select part_date, role_id, 
json_object('flag_id': flag_id, 'flag_level': flag_level, 'flag_star': flag_star, 'flag_awake': flag_awake) as flag_detail
from daily_hero_change_log
), 

daily_hero_array_info as(
select part_date, role_id, 
array_agg(flag_detail) as flag_detail
from daily_hero_end_turn_array 
group by 1, 2
)

select a.date, a.role_id, b.flag_detail, a.part_date
from daily_gserver_info a
left join daily_hero_array_info b
on a.part_date = b.part_date
and a.role_id = b.role_id;
###