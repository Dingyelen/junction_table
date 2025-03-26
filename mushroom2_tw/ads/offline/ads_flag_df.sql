drop table if exists hive.mushroom2_tw_w.ads_flag_df;

create table if not exists hive.mushroom2_tw_w.ads_flag_df(
role_id varchar, 
flag_level bigint, 
flag_star bigint, 
flag_awake bigint, 
flag_id varchar
) 
with(partitioned_by = array['flag_id']);

insert into hive.mushroom2_tw_w.ads_flag_df
(role_id, 
flag_level, flag_star, flag_awake, 
flag_id)

with dws_log as(
select date, role_id, flag_detail, part_date
from hive.mushroom2_tw_w.dws_flag_snapshot_di
),

dws_unnest as(
select date, role_id, json_parse(flag_detail_t) as flag_detail_t
from dws_log, unnest(flag_detail) as t(flag_detail_t)
), 

dws_detail as(
select date, role_id, 
cast(json_extract(flag_detail_t, '$.flag_id') as varchar) as flag_id, 
cast(json_extract(flag_detail_t, '$.flag_level') as bigint) as flag_level, 
cast(json_extract(flag_detail_t, '$.flag_star') as bigint) as flag_star, 
cast(json_extract(flag_detail_t, '$.flag_awake') as bigint) as flag_awake
from dws_unnest
), 

dau_info as(
select date, count(distinct role_id) as dau
from dws_log
group by 1
), 

report_info as(
select role_id, flag_id, 
max(flag_level) as flag_level, 
max(flag_star) as flag_star, 
max(flag_awake) as flag_awake
from dws_detail
group by 1, 2
)

select role_id, 
flag_level, flag_star, flag_awake, 
flag_id
from report_info