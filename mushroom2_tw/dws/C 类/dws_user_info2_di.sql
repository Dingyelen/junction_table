create table if not exists hive.mushroom2_tw_w.dws_user_info2_di(
role_id varchar, 
android_id varchar,
gaid varchar,
device_id varchar,
device_modelid varchar,
device_detail varchar,
is_second varchar, 
summon_num bigint, 
summon_continue bigint, 
summon_corecost bigint, 
normal_count bigint,
normal_win bigint,
adv_count bigint,
adv_win bigint,
ad_click bigint,
ad_success bigint,
ad_duration bigint,
tech_upgrade bigint,
equip_upgrade bigint,
equip_exchange bigint,
gem_exchange bigint
)
with(
format = 'ORC',
transactional = true
);

delete from hive.mushroom2_tw_w.dws_user_info2_di 
where exists(
select 1
from hive.mushroom2_tw_w.dws_user_daily2_di
where dws_user_daily2_di.role_id = dws_user_info2_di.role_id
and dws_user_daily2_di.part_date >= '$start_date'
and dws_user_daily2_di.part_date <= '$end_date'
);

insert into hive.mushroom2_tw_w.dws_user_info2_di(
role_id, android_id, gaid, device_id, 
device_modelid, device_detail, is_second, 
summon_num, summon_continue, summon_corecost, 
normal_count, normal_win, 
adv_count, adv_win, 
ad_click, ad_success, ad_duration, 
tech_upgrade, equip_upgrade, equip_exchange, gem_exchange
)

with user_daily as(
select *
from hive.mushroom2_tw_w.dws_user_daily2_di
where role_id in 
(select distinct role_id 
from hive.mushroom2_tw_w.dws_user_daily2_di 
where part_date >= $start_date
and  part_date <= $end_date)
), 

user_info as(
select role_id, 
sum(summon_num) as summon_num, 
sum(summon_continue) as summon_continue, 
sum(summon_corecost) as summon_corecost, 
sum(normal_count) as normal_count,
sum(normal_win) as normal_win,
sum(adv_count) as adv_count,
sum(adv_win) as adv_win,
sum(ad_click) as ad_click,
sum(ad_success) as ad_success,
sum(ad_duration) as ad_duration, 
sum(tech_upgrade) as tech_upgrade,  
sum(equip_upgrade) as equip_upgrade,  
sum(equip_exchange) as equip_exchange,  
sum(gem_exchange) as gem_exchange,  
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
a.role_id, b.android_id, b.gaid, b.device_id, 
b.device_modelid, b.device_detail, coalesce(c.is_second, '') as is_second, 
a.summon_num, a.summon_continue, a.summon_corecost, 
a.normal_count, a.normal_win, 
a.adv_count, a.adv_win, 
a.ad_click, a.ad_success, a.ad_duration, 
a.tech_upgrade, a.equip_upgrade, a.equip_exchange, a.gem_exchange
from user_info a
left join user_first_info b
on a.role_id = b.role_id
left join second_info c
on a.role_id = c.role_id
;