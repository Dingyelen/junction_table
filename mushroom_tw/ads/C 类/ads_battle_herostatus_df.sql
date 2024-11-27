drop table if exists hive.mushroom_tw_w.ads_battle_herostatus_df;

create table if not exists hive.mushroom_tw_w.ads_battle_herostatus_df(
zone_id varchar,
vip_level bigint, 
hero_status array(varchar),
users bigint,
pvp_count bigint
);

insert into hive.mushroom_tw_w.ads_battle_herostatus_df
(zone_id, vip_level, hero_status, users, pvp_count)

with data_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
opponent_uid, 
score_change, rank_change, battle_result, 
team1_status, team3_status, team2_status, 
oppoteam1_status, oppoteam2_status, oppoteam3_status, aiteam1
from hive.mushroom_tw_r.dwd_gserver_3v3battleinfo_live
where part_date >= date_format(date_add('day', -15, date($end_date)), '%Y-%m-%d')
and part_date <= $end_date
), 

union_log as(
select *, row_number() over(order by event_time) as rn from (
select date, part_date, role_id, zone_id, vip_level, event_time, team1_status as team_status from data_log
union all
select date, part_date, role_id, zone_id, vip_level, event_time, team2_status as team_status from data_log
union all
select date, part_date, role_id, zone_id, vip_level, event_time, team3_status as team_status from data_log
)), 

team_hero_res as(
select date, part_date, role_id, zone_id, vip_level, event_time, rn, 
team_status, json_value(hero_detail, 'lax $.hero_id') as hero_id
from union_log
cross join unnest(team_status) t(hero_detail)
), 

trans_log as(
select date, part_date, role_id, zone_id, vip_level, event_time, team_status, rn, 
a.hero_id, b.hero_cn, 
concat(a.hero_id, '_', b.hero_cn) as hero_status
from team_hero_res a
left join hive.mushroom_tw_w.dim_gserver_levelup_heroid b
on a.hero_id = b.hero_id
), 

array_log as(
select role_id, zone_id, vip_level, rn, 
array_agg(hero_status) as hero_status
from trans_log
group by 1, 2, 3, 4
), 

array_sort as(
select role_id, zone_id, vip_level, rn, 
array_sort(hero_status, (x, y) -> if(x < y, 1, if(x = y, 0, -1))) as hero_status
from array_log
)

select zone_id, vip_level, hero_status, 
count(distinct role_id) as users, 
count(*) as pvp_count
from array_sort
group by 1, 2, 3;