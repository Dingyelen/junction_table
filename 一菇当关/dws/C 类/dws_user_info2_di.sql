create table if not exists hive.mushroom_tw_w.dws_user_info2_di(
role_id varchar, 
normal_count bigint,
normal_win bigint,
adv_count bigint,
adv_win bigint,
adv_click bigint,
adv_success bigint,
adv_duration bigint,
android_id varchar,
gaid varchar,
device_id varchar,
device_modelid varchar,
device_detail varchar,
is_second varchar
)
with(
format = 'ORC',
transactional = true
);

delete from hive.mushroom_tw_w.dws_user_info2_di 
where exists(
select 1
from hive.mushroom_tw_w.dws_user_daily2_di
where dws_user_daily2_di.role_id = dws_user_info2_di.role_id
and dws_user_daily2_di.part_date >= '$start_date'
and dws_user_daily2_di.part_date <= '$end_date'
);

insert into hive.mushroom_tw_w.dws_user_info2_di(
role_id, 
normal_count, normal_win, 
adv_count, adv_win, 
adv_click, adv_success, adv_duration, 
android_id, gaid, device_id, 
device_modelid, device_detail, is_second
)

with user_daily as(
select *
from hive.mushroom_tw_w.dws_user_daily2_di
where role_id in 
(select distinct role_id 
from hive.mushroom_tw_w.dws_user_daily2_di 
where part_date >= $start_date
and  part_date <= $end_date)
), 

user_info as(
select role_id, 
sum(normal_count) as normal_count,
sum(normal_win) as normal_win,
sum(adv_count) as adv_count,
sum(adv_win) as adv_win,
sum(adv_click) as adv_click,
sum(adv_success) as adv_success,
sum(adv_duration) as adv_duration, 
min(date) as install_date
from user_daily 
group by 1
), 

second_info as(
select a.role_id, '1' as is_second
from user_daily a
left join user_info b
on a.role_id = b.role_id
where a.date = date_add('day', 1, b.install_date)
), 

user_first_info as(
select distinct role_id, 
first_value(android_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as android_id,
first_value(gaid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as gaid, 
first_value(device_id) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as device_id, 
first_value(device_modelid) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as device_modelid, 
first_value(device_detail) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as device_detail
from user_daily 
)

select 
a.role_id, 
a.normal_count, a.normal_win, 
a.adv_count, a.adv_win, 
a.adv_click, a.adv_success, a.adv_duration, 
b.android_id, b.gaid, b.device_id, 
b.device_modelid, b.device_detail, coalesce(c.is_second, '') as is_second
from user_info a
left join user_first_info b
on a.role_id = b.role_id
left join second_info c
on a.role_id = c.role_id
;