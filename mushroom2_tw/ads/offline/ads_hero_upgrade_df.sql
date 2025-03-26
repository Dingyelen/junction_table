drop table if exists hive.mushroom2_tw_w.ads_hero_upgrade_df;

create table if not exists hive.mushroom2_tw_w.ads_hero_upgrade_df(
hero_id varchar,
zone_id varchar,
start_date date,
active_users bigint,
hero_star bigint,
hold_users bigint,
upgrade_users bigint,
upgrade_count bigint,
chip_cost bigint
);

insert into hive.mushroom2_tw_w.ads_hero_upgrade_df
(hero_id, zone_id, start_date, active_users, hero_star, 
hold_users, upgrade_users, upgrade_count, chip_cost)

with data_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
cast(hero as varchar) as hero_id, 
fromrare as original_level, torare as target_level, 
cast(substring(cast(fromrare as varchar), 3, 1) as bigint) as hero_star, 
cast(substring(cast(torare as varchar), 3, 1) as bigint) as target_star, 
costchip as chip_cost, remainchip as chip_end, newskill as new_skill
from hive.mushroom2_tw_r.dwd_gserver_upgraderare_live
where part_date >= date_format(date_add('day', -15, date($end_date)), '%Y-%m-%d')
and part_date <= $end_date
), 

data_agg as(
select hero_id, cast(zone_id as varchar) as zone_id, hero_star, 
count(distinct role_id) as upgrade_users, 
count(*) as upgrade_count, 
sum(chip_cost) as chip_cost
from data_log
group by 1, 2, 3
), 

role_info as(
select role_id, cast(hero_id as varchar) as hero_id, cast(zone_id as varchar) as zone_id, 
max(herostar) as hero_star
from hive.mushroom2_tw_r.dwd_gserver_herosnap_live
where part_date >= date_format(date_add('day', -15, date($end_date)), '%Y-%m-%d')
and part_date <= $end_date
group by 1, 2, 3
), 

hero_group as(
select hero_id, zone_id, hero_star, 
count(distinct role_id) as hold_users
from role_info
group by 1, 2, 3
), 

hero_info as(
select cast(hero_id as varchar) as hero_id, cast(zone_id as varchar) as zone_id, 
min(date(event_time)) as start_date 
from hive.mushroom2_tw_r.dwd_gserver_herosnap_live
group by 1, 2
), 

daily_agg as(
select b.zone_id, count(distinct a.role_id) as active_users
from hive.mushroom2_tw_w.dws_user_daily_di a
left join hive.mushroom2_tw_w.dws_user_info_di b
on a.role_id = b.role_id
where part_date >= date_format(date_add('day', -15, date($end_date)), '%Y-%m-%d')
and part_date <= $end_date
group by 1
)

select a.hero_id, a.zone_id, c.start_date, d.active_users, a.hero_star, 
b.hold_users, a.upgrade_users, a.upgrade_count, a.chip_cost
from data_agg a
left join hero_group b
on a.hero_id = b.hero_id and a.zone_id = b.zone_id
and a.hero_star = b.hero_star
left join hero_info c
on a.hero_id = c.hero_id and a.zone_id = c.zone_id
left join daily_agg d
on a.zone_id = d.zone_id
