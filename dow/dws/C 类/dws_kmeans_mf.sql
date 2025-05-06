drop table if exists hive.dow_jpnew_w.dws_kmeans_mf;

create table if not exists hive.dow_jpnew_w.dws_kmeans_mf(
role_id varchar, 
mau_tag varchar, 
paydau_tag varchar, 
moneyrmb_tag varchar
)
with(format = 'PARQUET'
);

insert into hive.dow_jpnew_w.dws_kmeans_mf(
role_id, mau_tag, paydau_tag, moneyrmb_tag
)

###
with user_daily as(
select part_date, date(part_date) as date, 
role_id, level_min, level_max, 
money, money * 0.052102 as money_rmb, app_money, web_money,
pay_count, app_count, web_count,  
sincetimes_add, sincetimes_cost, sincetimes_end, 
(case when money>0 or sincetimes_cost>0 then 1 else null end) as is_paynew, 
(case when money>0 then 1 else null end) as is_pay, 
1 as dau
from hive.dow_jpnew_w.dws_user_daily_di
where part_date >= date_format(date_add('day', -30*10, current_date), '%Y-%m-%d')
and part_date <= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
), 

data_cube as(
select date, 
floor((row_number() over(order by date)-1)/30) as group_id
from unnest(sequence(date_add('day', -30*10, current_date), date_add('day', -1, current_date), interval '1' day)) as t(date)
), 

user_info as(
select distinct role_id
from user_daily
), 

data_cube_info as(
select distinct role_id, date, group_id
from user_info
cross join data_cube
), 

data_cube_agg as(
select a.group_id, a.role_id, 
sum(dau) as mau, 
sum(is_pay) as pay_dau, 
sum(money_rmb) as money_rmb 
from data_cube_info a
left join user_daily b
on a.role_id = b.role_id
and a.date = b.date
group by 1, 2
), 

user_agg as(
select role_id, sum(money_rmb) as money_rmb
from user_daily
group by 1
), 

data_cube_agg2 as(
select role_id, 
listagg(cast(coalesce(mau, 0) as varchar), '-') within group(order by group_id) as mau_tag, 
listagg(cast(coalesce(pay_dau, 0) as varchar), '-') within group(order by group_id) as paydau_tag, 
listagg(cast(coalesce(money_rmb, 0) as varchar), '-') within group(order by group_id) as moneyrmb_tag
from data_cube_agg
group by 1
)

select a.role_id, mau_tag, paydau_tag, moneyrmb_tag
from data_cube_agg2 a
left join user_agg b
on a.role_id = b.role_id
where b.money_rmb>0
###
