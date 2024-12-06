with payment_log as(
select role_id, sum(money) as money_ac
from hive.dow_jpnew_r.dwd_gserver_payment_live
where part_date < date_format(current_date, '%Y-%m-%d')
group by 1
), 

data_log as(
select a.event_time, a.part_date, b.install_date, 
a.role_id, 
(case when c.money_ac > 0 then 1 else 0 end) as is_paid, 
a.pay_source, a.money
from hive.dow_jpnew_r.dwd_merge_base_live a
left join hive.dow_jpnew_w.dws_user_info_di b
on a.role_id = b.role_id
left join payment_log c
on a.role_id = c.role_id
where a.part_date >= date_format(date_add('day', -1, current_date), '%Y-%m-%d')
and b.is_test is null
), 

today_agg as (
select 
count(distinct case when install_date = current_date or install_date is null then role_id else null end) as new_users, 
count(distinct role_id) as dau, 
count(distinct case when install_date = date_add('day', -1, current_date) then role_id else null end) as retention1_users, 
count(distinct case when money > 0 then role_id else null end) as pay_users, 
count(distinct case when money > 0 and is_paid = 0 then role_id else null end) as newpay_users, 
count(distinct case when money > 0 and (install_date = current_date or install_date is null) then role_id else null end) as install_pay, 
sum(case when money > 0 and is_paid = 0 then money else null end) as newpay_money, 
sum(case when money > 0 and (install_date = current_date or install_date is null) then money else null end) as install_money, 
sum(money) as money, 
sum(case when pay_source = 'web' then money else null end) as web_money 
from data_log
where part_date = date_format(current_date, '%Y-%m-%d')
), 

yesterday_agg as(
select 
count(distinct case when install_date = date_add('day', -1, current_date) then role_id else null end) as yesterday_newusers
from data_log
where part_date = date_format(date_add('day', -1, current_date), '%Y-%m-%d')
), 

log_agg as(
select max(event_time) as event_time
from data_log
)

select event_time, new_users, dau, retention1_users, retention1_users/yesterday_newusers as retention1_rate, 
pay_users, newpay_users, pay_users/dau as pay_rate, money/pay_users as arppu, money/dau as arpu, 
install_pay, install_money/install_pay as new_arppu, install_money/new_users as new_arpu, 
money, web_money

from today_agg, yesterday_agg, log_agg