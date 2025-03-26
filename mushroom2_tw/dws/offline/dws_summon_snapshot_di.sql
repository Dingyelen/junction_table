###
create table if not exists hive.mushroom2_tw_w.dws_summon_snapshot_di(
date date, 
role_id varchar, 
summon_detail array(varchar), 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.mushroom2_tw_w.dws_summon_snapshot_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.mushroom2_tw_w.dws_summon_snapshot_di(
date, role_id, summon_detail, part_date
)

with base_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, 
null as adid, 
cast(channel as varchar) as channel, 
cast(zone_id as varchar) as zone_id, 
cast(guild_id as varchar) as alliance_id,  
'dow_jp' as app_id, 
vip_level, level, rank, 
payment_itemid as good_id, 
currency, money, exchange_rate, money_rmb, 
online_time
from hive.mushroom2_tw_w.dwd_merge_base_live
where part_date >= $start_date
and part_date <= $end_date
), 

summon_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, guild_id as alliance_id, 
vip_level, level, rank, 
recruitid as summon_id, 
count as summon_count, 
is_free, 
currency_id, currency_num, 
null as core_summon, 
null as item_summon, 
null as unknow_summon, 
null as core_summonvalid, 
null as item_summonvalid, 
null as unknow_summonvalid
from hive.mushroom2_tw_r.dwd_gserver_recruitcard_live
where part_date >= $start_date
and part_date <= $end_date
), 

daily_gserver_info as(
select part_date, date, role_id
from base_log
group by 1, 2, 3
), 

daily_item_change_log as(
select part_date, role_id, summon_id, currency_id, 
sum(case when is_free = 1 then summon_count else null end) as summon_free,
sum(case when is_free = 0 then summon_count else null end) as summon_valid, 
sum(summon_count) as summon_count, 
sum(case when summon_count = 10 then 10 else null end) as summon_continue, 
sum(currency_num) as currency_num
from summon_log
group by 1, 2, 3, 4
), 

daily_item_end_turn_array as(
select part_date, role_id, 
json_object('summon_id': summon_id, 'currency_id': currency_id, 'currency_num': currency_num, 'summon_free': summon_free, 'summon_valid': summon_valid, 'summon_count': summon_count, 'summon_continue': summon_continue) as summon_detail
from daily_item_change_log
), 

daily_item_array_info as(
select part_date, role_id, 
array_agg(summon_detail) as summon_detail
from daily_item_end_turn_array 
group by 1, 2
)

select a.date, a.role_id, b.summon_detail, 
a.part_date as part_date
from daily_gserver_info a
left join daily_item_array_info b
on a.part_date = b.part_date
and a.role_id = b.role_id;
###