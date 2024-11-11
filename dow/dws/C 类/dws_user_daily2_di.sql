###
create table if not exists hive.dow_jpnew_w.dws_user_daily2_di(
date date, 
role_id varchar,
money decimal(36, 2),
is_pay bigint,
summon_list array(varchar), 
summon_free bigint,
summon_valid bigint,
summon_count bigint,
summon_continue bigint,
core_cost bigint,
is_summon bigint,
is_both_summonpay bigint,
is_battlefield bigint,
pvp_count bigint,
pvp_win bigint,
is_pvp bigint,
pvp_alliance bigint,
is_pvpalliance bigint,
huodong_count bigint,
huodong_win bigint,
pve_count bigint,
is_pve bigint,
is_both_pvppve bigint,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.dow_jpnew_w.dws_user_daily2_di 
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.dow_jpnew_w.dws_user_daily2_di
(date, role_id, 
money, is_pay, 
summon_list, summon_free, summon_valid, summon_count, 
summon_continue, core_cost, 
is_summon, is_both_summonpay, 
is_battlefield, 
pvp_count, pvp_win, is_pvp, 
pvp_alliance, is_pvpalliance, huodong_count, huodong_win, 
pve_count, is_pve, is_both_pvppve, 
part_date)
 
with daily_info as(
select date, part_date, role_id, 
money
from hive.dow_jpnew_w.dws_user_daily_di
where part_date >= $start_date
and part_date <= $end_date
), 

summon_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
cast(recruitid as varchar) as summon_id, 
count as summon_count, 
free as is_free, costid as currency_id, costcount as currency_num
from hive.dow_jpnew_r.dwd_gserver_recruitcard_live
where part_date >= $start_date
and part_date <= $end_date
), 

summon_cal as(
select date, part_date, role_id, 
sum(case when is_free = 1 then summon_count else null end) as summon_free,
sum(case when is_free = 0 then summon_count else null end) as summon_valid, 
sum(summon_count) as summon_count, 
sum(case when summon_count = 10 then 10 else null end) as summon_continue, 
sum(case when currency_id = 1 then currency_num else null end) as core_cost
from summon_log
group by 1, 2, 3
), 

summon_group as(
select date, part_date, role_id, summon_id
from summon_log
group by 1, 2, 3, 4
), 

summon_cal02 as(
select date, part_date, role_id, 
array_agg(summon_id) as summon_list
from summon_group
group by 1, 2, 3
), 

battle_log as(
select part_date, event_name, event_time, 
date(event_time) as date, 
role_id, open_id, adid, 
zone_id, alliance_id, 
vip_level, level, rank_level, 
cast((case when event_name = 'StartBattle' then battle_id else matchid end) as varchar) as battlefield_id, 
isalliance as is_alliance, result
from hive.dow_jpnew_r.dwd_merge_battle_live
where part_date >= $start_date
and part_date <= $end_date
and event_name in('FinishBattle', 'StartBattle')
), 

battle_cal as(
select date, part_date, role_id, 
sum(case when event_name = 'FinishBattle' and length(battlefield_id)=7 then 1 else null end) as pvp_count, 
sum(case when event_name = 'FinishBattle' and length(battlefield_id)=7 and result=1 then 1 else null end) as pvp_win,
sum(case when event_name = 'FinishBattle' and length(battlefield_id)=7 and is_alliance=1 then 1 else null end) as pvp_alliance,
sum(case when event_name = 'FinishBattle' and length(battlefield_id)=10 then 1 else null end) as huodong_count, 
sum(case when event_name = 'FinishBattle' and length(battlefield_id)=10 and result=1 then 1 else null end) as huodong_win, 
sum(case when event_name = 'StartBattle' then 1 else null end) as is_battlefield
from battle_log
group by 1, 2, 3
), 

instance_log as(
select date, part_date, role_id from(
select date(event_time) as date, part_date, role_id
from hive.dow_jpnew_r.dwd_gserver_challengechallenge_live
where part_date >= $start_date
and part_date <= $end_date)
union all(
select date(event_time) as date, part_date, role_id
from hive.dow_jpnew_r.dwd_gserver_instancebattle_live
where part_date >= $start_date
and part_date <= $end_date)
), 

instance_cal as(
select date, part_date, role_id, 
count(*) as pve_count
from instance_log
group by 1, 2, 3
),

daily_res as(
select a.date, a.part_date, a.role_id, 
a.money, (case when a.money > 0 then 1 else null end) as is_pay, 
c.summon_list, 
b.summon_free, b.summon_valid, b.summon_count, b.summon_continue, b.core_cost, 
(case when b.summon_count > 0 then 1 else null end) as is_summon, 
(case when a.money > 0 and b.summon_count > 0 then 1 else null end) as is_both_summonpay, 
(case when d.is_battlefield>0 then 1 else null end) as is_battlefield, 
d.pvp_count, d.pvp_win, (case when d.pvp_count > 0 then 1 else null end) as is_pvp, 
d.pvp_alliance, (case when d.pvp_count > 0 then 1 else null end) as is_pvpalliance,
d.huodong_count, d.huodong_win, 
e.pve_count, (case when e.pve_count > 0 then 1 else null end) as is_pve, 
(case when d.pvp_count > 0 and e.pve_count > 0 then 1 else null end) as is_both_pvppve
from daily_info a
left join summon_cal b
on a.role_id = b.role_id and a.date = b.date
left join summon_cal02 c
on a.role_id = c.role_id and a.date = c.date
left join battle_cal d
on a.role_id = d.role_id and a.date = d.date
left join instance_cal e
on a.role_id = e.role_id and a.date = e.date
)

select date, role_id, 
money, is_pay, 
summon_list, summon_free, summon_valid, summon_count, 
summon_continue, core_cost, 
is_summon, is_both_summonpay, 
is_battlefield, 
pvp_count, pvp_win, is_pvp, 
pvp_alliance, is_pvpalliance, huodong_count, huodong_win, 
pve_count, is_pve, is_both_pvppve, 
part_date
from daily_res
;
###