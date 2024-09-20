###
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
hero_id, hero_level, 
herostar as hero_star, 
power as hero_power
from hive.dow_jpnew_r.dwd_gserver_herosnap_live
where part_date >= $start_date
and part_date <= $end_date
), 

daily_gserver_info as(
select part_date, date, role_id
from base_log
group by 1, 2, 3
), 

daily_hero_change_log as(
select part_date, role_id, hero_id, 
max(hero_level) as hero_level, 
max(hero_star) as hero_star, 
max(hero_power) as hero_power
from hero_log
group by 1, 2, 3
), 

daily_hero_end_turn_array as(
select part_date, role_id, 
json_object('hero_id': hero_id, 'hero_level': hero_level, 'hero_star': hero_star, 'hero_power': hero_power) as hero_detail
from daily_hero_change_log
), 

daily_hero_array_info as(
select part_date, role_id, 
array_agg(hero_detail) as hero_detail
from daily_hero_end_turn_array 
group by 1, 2
)

select a.date, a.role_id, b.hero_detail, a.part_date
from daily_gserver_info a
left join daily_hero_array_info b
on a.part_date = b.part_date
and a.role_id = b.role_id;
###