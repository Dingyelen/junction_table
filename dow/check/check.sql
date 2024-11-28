/*
* @Author: dingyelen
* @Date:   2024-11-20 10:25:48
* @Last Modified by:   dingyelen
* @Last Modified time: 2024-11-27 17:46:03
*/

-- 4. dws_user_daily_di
-- 4.1 验证昨天 user_daily 和原始支付表的金额差异
with money_daily_in_payment as (
select cast(sum(money) as bigint) money
from hive.dow_jpnew_r.dwd_gserver_payment_live
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')),

money_daily_in_user_daily as (
select cast(sum(money) as bigint) money
from hive.dow_jpnew_w.dws_user_daily_di
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d'))

select abs(money_daily_in_payment.money - money_daily_in_user_daily.money)
from money_daily_in_payment, money_daily_in_user_daily

-- 4.2 验证user_daily是否重复
select role_id, count(1) FROM hive.dow_jpnew_w.dws_user_daily_di
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d'))
group by 1
HAVING count(1) > 1

-- 4.3 验证昨天 user_daily 每个人支付金额和原始支付表的金额差异
with money_daily_in_payment as (
select role_id, cast(sum(money) as bigint) money
from hive.dow_jpnew_r.dwd_gserver_payment_live
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
group by 1),

money_daily_in_user_daily as (
select role_id, cast(sum(money) as bigint) money
from hive.dow_jpnew_w.dws_user_daily_di
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
group by 1)

select a.role_id, abs(coalesce(a.money, 0) - coalesce(b.money, 0))
-- from money_daily_in_payment a
left join money_daily_in_user_daily b
on a.role_id = b.role_id

-- 5. dws_user_info_di
-- 5.1 验证全表 user_info 和 user_daily 的误差
with user_info_count as (
select count(1) as count
from hive.dow_jpnew_w.dws_user_info_di
),
merge_base_count as (
select count(distinct role_id) as count
from hive.dow_jpnew_r.dwd_merge_base_live
)
select cast((abs(user_info_count.count - merge_base_count.count) / user_info_count.count ) * 100 as bigint) as result
from user_info_count, merge_base_count

-- 5.2 验证全表 user_info 是否有重复数据
select role_id, count(1) 
from hive.dow_jpnew_w.dws_user_info_di
group by 1
having count(1) > 1

-- 6. dws_user_daily_derive_di
-- 6.1 验证 user_daily_derive 金额差异
with daily_derive as(
select date, role_id, money_ac
from hive.dow_jpnew_w.dws_user_daily_derive_di
where date < current_date
), 

max_money as(
select role_id, max(money_ac) as money_ac
from daily_derive
group by 1
), 

sum_money as(
select sum(money_ac) as money_ac
from max_money
), 

payment_log as(
select sum(money) as money
from hive.dow_jpnew_r.dwd_gserver_payment_live
where part_date < cast(current_date as varchar)
)

select cast((abs(coalesce(money_ac, 0) - coalesce(money, 0))) as bigint) 
from sum_money, payment_log

-- 6.2 验证user_daily_derive是否有重复用户
select part_date, role_id, count(1) 
from hive.dow_jpnew_w.dws_user_daily_derive_di
group by 1, 2
having count(1) > 1

-- 6.3 验证 user_daily_derive 每个用户累计金额是否和 user_daily 一致
with base_log as(
select part_date, role_id, 
sum(money) over(partition by role_id order by part_date rows between unbounded preceding and current row) as money_ac
from hive.dow_jpnew_w.dws_user_daily_di
), 

base_cal as(
select part_date, role_id, money_ac
from base_log
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
), 

check_log as(
select part_date, role_id, money_ac
from hive.dow_jpnew_w.dws_user_daily_derive_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
)

select a.*, b.money_ac
from base_cal a
full join check_log b
on a.part_date = b.part_date and a.role_id = b.role_id
where coalesce(a.money_ac, 0) != coalesce(b.money_ac, 0)

-- 7. dws_user_hourly_hi
-- 7.1 验证昨天到现在前一小时用户数是否与原始日志一致
with base_log as (
select count(distinct role_id) as users
from hive.dow_jpnew_r.dwd_merge_base_live
where part_date >= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
and part_date <= date_format(current_date, '%Y-%m-%d')
and event_time < cast(date_format(localtimestamp, '%Y-%m-%d %h:00:00') as timestamp)),

check_data as (
select count(distinct role_id) as users
from hive.dow_jpnew_w.dws_user_hourly_hi
where part_date >= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
and part_date <= date_format(current_date, '%Y-%m-%d')
and hour < cast(date_format(localtimestamp, '%Y-%m-%d %h:00:00') as timestamp))

select abs(base_log.users - check_data.users)
from base_log, check_data

-- 7.2 验证昨天到现在前一小时每个用户数付费金额是否与原始支付日志一致
with base_log as (
select role_id, sum(money) as money
from hive.dow_jpnew_r.dwd_gserver_payment_live
where part_date >= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
and part_date <= date_format(current_date, '%Y-%m-%d')
and event_time < cast(date_format(localtimestamp, '%Y-%m-%d %h:00:00') as timestamp)       
group by 1),

check_data as (
select role_id, sum(money) as money
from hive.dow_jpnew_w.dws_user_hourly_hi
where part_date >= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
and part_date <= date_format(current_date, '%Y-%m-%d')
and hour < cast(date_format(localtimestamp, '%Y-%m-%d %h:00:00') as timestamp)
group by 1)

select a.role_id, abs(coalesce(a.money, 0) - coalesce(b.money, 0))
from base_log a 
left join check_data b
on a.role_id = b.role_id

-- 8. dws_core_snapshot_di
-- 8.1 验证 core_gain 和 core_cost 是否和原始日志 dwd_gserver_corechange_live 一致
with base_log as (
select role_id, event_type, coalesce(free_num, 0) as free_num, coalesce(paid_num, 0) as paid_num
from hive.dow_jpnew_r.dwd_gserver_corechange_live
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
and reason != '638'),

base_cal as(
select role_id, sum(case when event_type = 'gain' then coalesce(free_num, 0) + coalesce(paid_num, 0) else null end) as core_add
from base_log
group by 1), 

check_data as (
select role_id, sum(core_add) as core_add
from hive.dow_jpnew_w.dws_core_snapshot_di
cross join unnest(cast(json_parse(coreadd_detail) as map(varchar, bigint))) as addinfo(reason, core_add)
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
group by 1)

select a.role_id, abs(coalesce(a.core_add, 0) - coalesce(b.core_add, 0))
from base_cal a 
left join check_data b
on a.role_id = b.role_id;

with base_log as (
select role_id, event_type, coalesce(free_num, 0) as free_num, coalesce(paid_num, 0) as paid_num
from hive.dow_jpnew_r.dwd_gserver_corechange_live
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
and reason != '638'),

base_cal as(
select role_id, sum(case when event_type = 'cost' then coalesce(free_num, 0) + coalesce(paid_num, 0) else null end) as core_cost
from base_log
group by 1), 

check_data as (
select role_id, sum(core_cost) as core_cost
from hive.dow_jpnew_w.dws_core_snapshot_di
cross join unnest(cast(json_parse(corecost_detail) as map(varchar, bigint))) as addinfo(reason, core_cost)
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
group by 1)

select a.role_id, abs(coalesce(a.core_cost, 0) - coalesce(b.core_cost, 0))
from base_cal a 
left join check_data b
on a.role_id = b.role_id;

-- 9. ads_active_daily_di
-- 9.1 验证近 30 日 dau 是否与 user_daily 一致
with base_log as(
select part_date, count(distinct role_id) as users
from hive.dow_jpnew_w.dws_user_daily_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
group by 1),

check_data as(
select part_date, sum(dau) as users
from hive.dow_jpnew_w.ads_active_daily_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
group by 1)

select a.part_date, abs(a.users- b.users)
from base_log a 
left join check_data b
on a.part_date = b.part_date;

-- 10. ads_core_addreason_di
-- 10.1 验证近 30 日 core_add 是否与 dws_core_snapshot_di 一致
with base_log as(
select a.*
from hive.dow_jpnew_w.dws_core_snapshot_di a
left join hive.dow_jpnew_w.dim_gserver_base_roleid b
on a.role_id = b.role_id
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
and b.role_id is null
),

base_cal as(
select part_date, sum(core_add) as core_add
from base_log
cross join unnest(cast(json_parse(coreadd_detail) as map(varchar, bigint))) as addinfo(reason, core_add)
group by 1
), 

check_data as(
select part_date, sum(core_add) as core_add
from hive.dow_jpnew_w.ads_core_addreason_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
group by 1)

select a.part_date, abs(coalesce(a.core_add, 0) - coalesce(b.core_add, 0))
from base_cal a 
left join check_data b
on a.part_date = b.part_date;

-- 11. ads_core_costreason_di
-- 11.1 验证每日 core_cost 是否与 dws_core_snapshot_di 一致
with base_log as(
select a.*
from hive.dow_jpnew_w.dws_core_snapshot_di a
left join hive.dow_jpnew_w.dim_gserver_base_roleid b
on a.role_id = b.role_id
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
and b.role_id is null
),

base_cal as(
select part_date, sum(core_cost) as core_cost
from base_log
cross join unnest(cast(json_parse(corecost_detail) as map(varchar, bigint))) as addinfo(reason, core_cost)
group by 1
), 

check_data as(
select part_date, sum(core_cost) as core_cost
from hive.dow_jpnew_w.ads_core_costreason_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
group by 1)

select a.part_date, abs(a.core_cost - b.core_cost)
from base_cal a 
left join check_data b
on a.part_date = b.part_date;

-- 12. ads_retention_daily_di
-- 12.1 验证近 30 日 dau 是否与 user_daily 一致
with base_log as(
select part_date, count(distinct role_id) as users       
from hive.dow_jpnew_w.dws_user_daily_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
and is_test is null
group by 1),

check_data as(
select part_date, sum(dau) as users
from hive.dow_jpnew_w.ads_retention_daily_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
and retention_day = 0
group by 1)

select a.part_date, coalesce(a.users, 0)- coalesce(b.users, 0)
from base_log a 
left join check_data b
on a.part_date = b.part_date
where coalesce(a.users, 0) != coalesce(b.users, 0)

-- 13. ads_user_retention_di
-- 13.1 验证近 30 日新增用户是否与 user_info 一致
with base_log as(
select install_date, count(distinct role_id) as users       
from hive.dow_jpnew_w.dws_user_info_di
where install_date >= date_add('day', -30, current_date)
and is_test is null
group by 1),

check_data as(
select date, sum(active_users) as users
from hive.dow_jpnew_w.ads_user_retention_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
and retention_day = 0
group by 1)

select a.install_date, abs(coalesce(a.users, 0) - coalesce(b.users, 0))
from base_log a 
left join check_data b
on a.install_date = b.date
where coalesce(a.users, 0) != coalesce(b.users, 0)

-- 14. ads_kpi_daily_di
-- 14.1 验证近 30 日每日付费金额是否与 user_daily 一致
with base_log as(
select part_date, cast(sum(money) as bigint) as money    
from hive.dow_jpnew_w.dws_user_daily_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
group by 1),

check_data as(
select part_date, cast(sum(money) as bigint) as money
from hive.dow_jpnew_w.ads_kpi_daily_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
group by 1)

select a.part_date as base_date, abs(coalesce(a.money, 0) - coalesce(b.money, 0))
from base_log a 
left join check_data b
on a.part_date = b.part_date
order by 1, 2;

-- 14.2 验证最大累计金额是否与 user_daily 付费总和一致
with base_log as(
select sum(money) as money_ac
from hive.dow_jpnew_w.dws_user_daily_di
where part_date <= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
and is_test is null
),

check_data as(
select sum(money_ac) as money_ac
from hive.dow_jpnew_w.ads_kpi_daily_di
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
)

select abs(base_log.money_ac - check_data.money_ac)
from base_log, check_data;

-- 14.3 验证最大新增用户是否与user_daily总用户数一致
with base_log as(
select count(distinct role_id) as users
from hive.dow_jpnew_w.dws_user_daily_di
where part_date <= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
and is_test is null),

check_data as(
select sum(newuser_ac) as newuser_ac
from hive.dow_jpnew_w.ads_kpi_daily_di
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
)

select abs(base_log.users - check_data.newuser_ac)
from base_log, check_data;

-- 15. ads_kpi_hourly_hi
-- 15.1 验证近 30 日 dau 是否与 user_daily 一致
with base_log as(
select part_date, count(distinct role_id) as users       
from hive.dow_jpnew_w.dws_user_daily_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
and part_date < date_format(current_date, '%Y-%m-%d')
and is_test is null
group by 1),

check_data as(
select part_date, sum(dau) as users
from hive.dow_jpnew_w.ads_kpi_hourly_hi
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
and part_date < date_format(current_date, '%Y-%m-%d')
group by 1)

select a.part_date, abs(coalesce(a.users, 0)- coalesce(b.users, 0))
from base_log a 
left join check_data b
on a.part_date = b.part_date
where coalesce(a.users, 0) != coalesce(b.users, 0);

-- 15.2 验证近 30 日付费金额是否与 user_daily 一致
with base_log as(
select part_date, sum(money) as money    
from hive.dow_jpnew_w.dws_user_daily_di
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
and part_date < date_format(current_date, '%Y-%m-%d')
and is_test is null
group by 1),

check_data as(
select part_date, sum(money_hourly) as money
from hive.dow_jpnew_w.ads_kpi_hourly_hi
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
and part_date < date_format(current_date, '%Y-%m-%d')
group by 1)

select a.part_date, abs(coalesce(a.money, 0) - coalesce(b.money, 0))
from base_log a 
left join check_data b
on a.part_date = b.part_date
where coalesce(a.money, 0) != coalesce(b.money, 0);

-- 15.3 验证近 30 日新增用户是否与 user_info 一致
with base_log as(
select cast(install_date as varchar) as install_date, count(distinct role_id) as new_users
from hive.dow_jpnew_w.dws_user_info_di
where install_date >= date_add('day', -30, current_date)
and install_date < current_date
and is_test is null
group by 1),

check_data as(
select part_date, sum(new_users) as new_users    
from hive.dow_jpnew_w.ads_kpi_hourly_hi
where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
and part_date < date_format(current_date, '%Y-%m-%d')
group by 1)

select a.install_date, abs(coalesce(a.new_users, 0) - coalesce(b.new_users, 0))
from base_log a 
left join check_data b
on a.install_date = b.part_date
where coalesce(a.new_users, 0) != coalesce(b.new_users, 0);