drop table if exists hive.mushroom_tw_w.ads_hero_df;

create table if not exists hive.mushroom_tw_w.ads_hero_df(
role_id varchar, 
hero_level bigint, 
hero_power bigint, 
hero_star bigint, 
hero_id varchar
) 
with(partitioned_by = array['hero_id']);

insert into hive.mushroom_tw_w.ads_hero_df
(role_id, 
hero_level, hero_power, hero_star, 
hero_id)

with dws_log as(
select date, role_id, hero_detail, part_date
from hive.mushroom_tw_w.dws_hero_snapshot_di
),

dws_unnest as(
select date, role_id, json_parse(hero_detail_t) as hero_detail_t
from dws_log, unnest(hero_detail) as t(hero_detail_t)
), 

dws_detail as(
select date, role_id, 
cast(json_extract(hero_detail_t, '$.hero_id') as varchar) as hero_id, 
cast(json_extract(hero_detail_t, '$.hero_level') as bigint) as hero_level, 
cast(json_extract(hero_detail_t, '$.hero_power') as bigint) as hero_power, 
cast(json_extract(hero_detail_t, '$.hero_star') as bigint) as hero_star
from dws_unnest
), 

dau_info as(
select date, count(distinct role_id) as dau
from dws_log
group by 1
), 

report_info as(
select role_id, hero_id, 
max(hero_level) as hero_level, 
max(hero_power) as hero_power, 
max(hero_star) as hero_star
from dws_detail
group by 1, 2
)

select role_id, 
hero_level, hero_power, hero_star, 
hero_id
from report_info