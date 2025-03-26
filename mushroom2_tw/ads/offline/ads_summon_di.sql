create table if not exists hive.mushroom2_tw_w.ads_summon_di(
date date, 
dau bigint,  
summon_id varchar, 
currency_id varchar, 
users bigint, 
currency_num bigint, 
summon_free bigint, 
summon_valid bigint, 
summon_continue bigint, 
summon_count bigint, 
part_date varchar
) 
with(partitioned_by = array['part_date']);

delete from hive.mushroom2_tw_w.ads_summon_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.mushroom2_tw_w.ads_summon_di
(date, dau, 
summon_id, currency_id, 
users, currency_num, summon_free, summon_valid, 
summon_continue, summon_count, part_date)

with dws_log as(
select a.date, a.role_id, summon_detail, part_date
from hive.mushroom2_tw_w.dws_summon_snapshot_di a
left join hive.mushroom2_tw_w.dws_user_info_di b
on a.role_id = b.role_id
where part_date>=$start_date
and part_date<=$end_date
and b.is_test is null
),

dws_unnest as(
select date, part_date, role_id, json_parse(summon_detail_t) as summon_detail_t
from dws_log, unnest(summon_detail) as t(summon_detail_t)
), 

dws_detail as(
select date, part_date, role_id, 
cast(json_extract(summon_detail_t, '$.summon_id') as varchar) as summon_id, 
cast(json_extract(summon_detail_t, '$.currency_id') as varchar) as currency_id, 
cast(json_extract(summon_detail_t, '$.currency_num') as bigint) as currency_num, 
cast(json_extract(summon_detail_t, '$.summon_free') as bigint) as summon_free, 
cast(json_extract(summon_detail_t, '$.summon_valid') as bigint) as summon_valid, 
cast(json_extract(summon_detail_t, '$.summon_continue') as bigint) as summon_continue, 
cast(json_extract(summon_detail_t, '$.summon_count') as bigint) as summon_count
from dws_unnest
), 

dau_info as(
select date, count(distinct role_id) as dau
from dws_log
group by 1
), 

report_info as(
select date, part_date, summon_id, currency_id, 
count(distinct role_id) users, 
sum(currency_num) as currency_num, 
sum(summon_free) as summon_free, 
sum(summon_valid) as summon_valid, 
sum(summon_continue) as summon_continue, 
sum(summon_count) as summon_count
from dws_detail
group by 1, 2, 3, 4
)

select a.date, b.dau, 
a.summon_id, a.currency_id, 
a.users, a.currency_num, a.summon_free, a.summon_valid, 
a.summon_continue, a.summon_count, 
a.part_date
from report_info a
left join dau_info b
on a.date = b.date