drop table if exists hive.mushroom2_tw_w.ads_act_battle_df;

create table if not exists hive.mushroom2_tw_w.ads_act_battle_df(
start_date date, 
end_date date, 
act_days bigint, 
active_users bigint, 
apply_users bigint, 
battle_users bigint
);

insert into hive.mushroom2_tw_w.ads_act_battle_df(
start_date, end_date, act_days, active_users, apply_users, battle_users
)

with user_daily as(
select a.date, a.part_date, day_of_week(a.date) as weekday, 
a.role_id, b.huodong_count
from hive.mushroom2_tw_w.dws_user_daily_di a
left join hive.mushroom2_tw_w.dws_user_daily2_di b
on a.role_id = b.role_id and a.date = b.date
where a.part_date >= '2023-12-29'
), 

weekday_cal as(
select date, part_date, weekday, 
(case when part_date < '2024-05-24' and weekday in(2, 5) then date 
when part_date < '2024-05-24' and weekday in(3, 6) then date_add('day', -1, date)
when part_date < '2024-05-24' and weekday in(4, 7) then date_add('day', -2, date)
when part_date < '2024-05-24' and weekday = 1 then date_add('day', -3, date)
when part_date >= '2024-05-24' and weekday in(1, 3, 5) then date 
when part_date >= '2024-05-24' and weekday in(2, 4, 6) then date_add('day', -1, date)
when part_date >= '2024-05-24' and weekday = 7 then date_add('day', -2, date)
else null end) as start_date, 
(case when part_date < '2024-05-24' and weekday in(4, 1) then date 
when part_date < '2024-05-24' and weekday in(3, 7) then date_add('day', 1, date)
when part_date < '2024-05-24' and weekday in(2, 6) then date_add('day', 2, date)
when part_date < '2024-05-24' and weekday = 5 then date_add('day', 3, date)
when part_date >= '2024-05-24' and weekday in(2, 4, 7) then date 
when part_date >= '2024-05-24' and weekday in(1, 3, 6) then date_add('day', 1, date)
when part_date >= '2024-05-24' and weekday = 5 then date_add('day', 2, date)
else null end) as end_date, 
role_id, huodong_count
from user_daily 
), 

apply_log as(
select date(event_time) as date, part_date, role_id, 1 as is_apply
from hive.mushroom2_tw_r.dwd_gserver_activityapply_live
where part_date >= '2023-12-29'
group by 1, 2, 3, 4
), 

daily_info as(
select a.date, a.part_date, a.weekday, 
a.start_date, a.end_date, 
a.role_id, a.huodong_count, b.is_apply
from weekday_cal a
left join apply_log b
on a.role_id = b.role_id and a.date = b.date
)

select start_date, end_date, 
count(distinct date) as act_days, 
count(distinct role_id) as active_users, 
count(distinct case when is_apply>0 then role_id else null end) as apply_users, 
count(distinct case when huodong_count>0 then role_id else null end) as battle_users
from daily_info
group by 1, 2