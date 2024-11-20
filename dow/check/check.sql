/*
* @Author: dingyelen
* @Date:   2024-11-20 10:25:48
* @Last Modified by:   dingyelen
* @Last Modified time: 2024-11-20 11:48:48
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
SELECT role_id, count(1) FROM hive.dow_jpnew_w.dws_user_daily_di
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d'))
GROUP BY 1
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
from money_daily_in_payment a
left join money_daily_in_user_daily b
on a.role_id = b.role_id

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

select cast((abs(COALESCE(money_ac, 0) - COALESCE(money, 0))) as bigint) 
from sum_money, payment_log

-- 6.2 验证user_daily_derive是否有重复用户
SELECT role_id, count(1) FROM hive.dow_jpnew_w.dws_user_daily_derive_di
group by 1
having count(1) > 1


-- 14. ads_kpi_daily_di
-- 14.1 验证近 30 日每日付费金额是否与 user_daily 一致
with base_log as(
       select part_date, sum(money) as money    
       from hive.dow_jpnew_w.dws_user_daily_di
       where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
       group by 1),
       
     check_data as(
       select part_date, sum(money) as money
       from hive.dow_jpnew_w.ads_kpi_daily_di
       where part_date >= date_format(date_add('day', -30, current_date), '%Y-%m-%d')
       group by 1)

select a.part_date as base_date, b.part_date as check_date, abs(a.money - b.money)
from base_log a 
full join check_data b
on a.part_date = b.part_date
order by 1, 2;

-- 14.2 验证最大累计金额是否与 user_daily 付费总和一致
with base_log as(
       select sum(money) as money_ac
       from hive.dow_jpnew_w.dws_user_daily_di
       where part_date <= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
       ),
       
     check_data as(
       select sum(money_ac) as money_ac
       from hive.dow_jpnew_w.ads_kpi_daily_di
       where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
       )

select abs(base_log.money_ac - check_data.money_ac)
from base_log, check_data