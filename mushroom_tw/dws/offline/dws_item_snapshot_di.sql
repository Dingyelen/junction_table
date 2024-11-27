###
create table if not exists hive.mushroom_tw_w.dws_item_snapshot_di(
date date, 
role_id varchar, 
item_detail array(varchar), 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.mushroom_tw_w.dws_item_snapshot_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.mushroom_tw_w.dws_item_snapshot_di(
date, role_id, item_detail, part_date
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
from hive.mushroom_tw_r.dwd_merge_base_live
where part_date >= $start_date
and part_date <= $end_date
), 

item_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, null as adid, 
zone_id, guild_id as alliance_id, 
vip_level, level, rank, 
item_id, item_add, item_cost, 
item_remain as item_end
from hive.mushroom_tw_r.dwd_merge_item_live
where part_date >= $start_date
and part_date <= $end_date
), 

daily_gserver_info as(
select part_date, date, role_id
from base_log
group by 1, 2, 3
), 

daily_item_change_log as(
select part_date, role_id, item_id, 
sum(case when event_name = 'AddItem' then item_add else null end) as item_add,
sum(case when event_name = 'CostItem' then item_cost else null end) as item_cost
from item_log
group by 1, 2, 3
), 

daily_item_end_log as(
select distinct part_date, role_id, item_id, 
last_value(item_end) ignore nulls over(partition by role_id, part_date, item_id order by event_time, item_end
rows between unbounded preceding and unbounded following) as item_end
from item_log
), 

daily_item_end_turn_array as(
select a.part_date, a.role_id, 
json_object('item_id': a.item_id, 'item_add': a.item_add, 'item_cost': a.item_cost, 'item_end': b.item_end) as item_detail
from daily_item_change_log a
left join daily_item_end_log b
on a.part_date = b.part_date
and a.role_id = b.role_id
and a.item_id = b.item_id
), 

daily_item_array_info as(
select part_date, role_id, 
array_agg(item_detail) as item_detail
from daily_item_end_turn_array 
group by 1, 2
)

select a.date, a.role_id, b.item_detail, a.part_date
from daily_gserver_info a
left join daily_item_array_info b
on a.part_date = b.part_date
and a.role_id = b.role_id;
###