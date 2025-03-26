drop table if exists hive.mushroom2_tw_w.ads_artifact_df;

create table if not exists hive.mushroom2_tw_w.ads_artifact_df(
role_id varchar, 
artifact_level bigint, 
artifact_id varchar
) 
with(partitioned_by = array['artifact_id']);

insert into hive.mushroom2_tw_w.ads_artifact_df
(role_id, artifact_level, artifact_id)

with dws_log as(
select date, role_id, artifact1_detail, artifact2_detail, part_date
from hive.mushroom2_tw_w.dws_artifact_snapshot_di
),

dws_unnest as(
select date, role_id, 
json_parse(artifact1_detail_t) as artifact1_detail_t, 
json_parse(artifact2_detail_t) as artifact2_detail_t
from dws_log, unnest(artifact1_detail, artifact2_detail) as t(artifact1_detail_t, artifact2_detail_t)
), 

dws_detail_01 as(
select date, role_id, 
cast(json_extract(artifact1_detail_t, '$.artifact1_id') as varchar) as artifact_id, 
cast(json_extract(artifact1_detail_t, '$.artifact1_level') as bigint) as artifact_level
from dws_unnest
), 

dws_detail_02 as(
select date, role_id, 
cast(json_extract(artifact2_detail_t, '$.artifact2_id') as varchar) as artifact_id, 
cast(json_extract(artifact2_detail_t, '$.artifact2_level') as bigint) as artifact_level
from dws_unnest
), 

dws_detail as(
select * from dws_detail_01
union all 
select * from dws_detail_02
), 

report_info as(
select role_id, artifact_id, 
max(artifact_level) as artifact_level
from dws_detail
group by 1, 2
)

select role_id, artifact_level, artifact_id
from report_info