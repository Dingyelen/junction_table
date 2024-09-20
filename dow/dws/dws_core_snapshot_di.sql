###
create table if not exists hive.dow_jpnew_w.dws_core_snapshot_di(
date date, 
role_id varchar, 
coregain_detail varchar, 
freegain_detail varchar, 
paidgain_detail varchar, 
corecost_detail varchar, 
freecost_detail varchar, 
paidcost_detail varchar, 
core_end bigint, 
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.dow_jpnew_w.dws_core_snapshot_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.dow_jpnew_w.dws_core_snapshot_di(
date, role_id, 
coregain_detail, freegain_detail, paidgain_detail, 
corecost_detail, freecost_detail, paidcost_detail, 
core_end, part_date
)

with base_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, device_id, 
channel, zone_id, alliance_id,  
'dow_jp' as app_id, 
vip_level, level, rank_level, power, 
payment_itemid, currency, money, 
-- b.exchange_rate, 
-- a.money * b.exchange_rate as money_rmb, 
online_time, 
row_number() over(partition by role_id, part_date, event_name order by event_time) as partevent_rn, 
row_number() over(partition by role_id, part_date, event_name order by event_time desc) as partevent_descrn
from hive.dow_jpnew_r.dwd_merge_base_live
where part_date >= $start_date
and part_date <= $end_date
), 

core_log_base as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
'dow_jp' as app_id, 
vip_level, level, rank_level, 
reason, event_type, 
free_num, paid_num, free_num + paid_num as core_num, 
free_end, paid_end, free_end + paid_end as core_end
from hive.dow_jpnew_r.dwd_gserver_corechange_live
where part_date >= $start_date
and part_date <= $end_date
and reason != '638'
), 

core_log as(
select part_date, event_name, event_time, 
role_id, open_id, adid, 
zone_id, alliance_id, app_id, 
vip_level, level, rank_level, reason, 
(case when event_type = 'gain' then free_num else null end) as free_gain, 
(case when event_type = 'gain' then paid_num else null end) as paid_gain, 
(case when event_type = 'gain' then core_num else null end) as core_gain, 
(case when event_type = 'cost' then free_num else null end) as free_cost, 
(case when event_type = 'cost' then paid_num else null end) as paid_cost, 
(case when event_type = 'cost' then core_num else null end) as core_cost, 
free_end, paid_end, core_end
from core_log_base
), 

core_cal_log as(
select part_date, event_name, 
role_id, open_id, adid, 
zone_id, alliance_id, app_id, 
vip_level, level, rank_level, reason, 
sum(free_gain) as free_gain,
sum(paid_gain) as paid_gain,
sum(core_gain) as core_gain,
sum(free_cost) as free_cost,
sum(paid_cost) as paid_cost,
sum(core_cost) as core_cost
from core_log
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
), 

daily_gserver_info as(
select part_date, date, role_id
from base_log
group by 1, 2, 3
), 

daily_turn_array as(
select part_date, role_id, 
json_format(cast(map_agg(reason, core_gain) filter (where core_gain > 0) as json)) as coregain_detail, 
json_format(cast(map_agg(reason, free_gain) filter (where free_gain > 0) as json)) as freegain_detail, 
json_format(cast(map_agg(reason, paid_gain) filter (where paid_gain > 0) as json)) as paidgain_detail, 
json_format(cast(map_agg(reason, core_cost) filter (where core_cost > 0) as json)) as corecost_detail, 
json_format(cast(map_agg(reason, free_cost) filter (where free_cost > 0) as json)) as freecost_detail, 
json_format(cast(map_agg(reason, paid_cost) filter (where paid_cost > 0) as json)) as paidcost_detail
from core_cal_log
group by 1, 2
), 

daily_core_last as
(select distinct part_date, role_id,
last_value(core_end) ignore nulls over (partition by part_date, role_id order by event_time, core_end
rows between unbounded preceding and unbounded following) as core_end
from core_log
)

select a.date, a.role_id, 
b.coregain_detail, 
b.freegain_detail, 
b.paidgain_detail, 
b.corecost_detail, 
b.freecost_detail, 
b.paidcost_detail, 
c.core_end, 
a.part_date
from daily_gserver_info a
left join daily_turn_array b
on a.part_date = b.part_date
and a.role_id = b.role_id
left join daily_core_last c
on a.part_date = c.part_date
and a.role_id = c.role_id;
###