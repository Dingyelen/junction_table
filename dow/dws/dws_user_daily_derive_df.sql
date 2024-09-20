###
drop table if exists hive.dow_jpnew_w.dws_user_daily_derive_df;

create table if not exists hive.dow_jpnew_w.dws_user_daily_derive_df(
date date, 
role_id varchar, 
login_days bigint, 
is_firstpay bigint, 
is_pay bigint, 
is_paid bigint,
is_new bigint,
money_ac decimal(36, 2), 
moneyrmb_ac decimal(36, 2),
webrmb_ac decimal(36, 2), 
sincetimes_end bigint, 
core_end bigint, 
free_end bigint, 
paid_end bigint, 
newuser_ac bigint, 
before_date date, 
after_date date, 
part_date varchar
);

insert into hive.dow_jpnew_w.dws_user_daily_derive_df(
date, role_id, login_days, 
is_firstpay, is_pay, is_paid, is_new, 
money_ac, moneyrmb_ac, webrmb_ac, 
sincetimes_end, core_end, free_end, paid_end, 
newuser_ac, before_date, after_date, 
part_date
)

with user_daily as(
select date, role_id, 
row_number() over(partition by role_id order by part_date) as login_days, 
firstpay_ts, money, money_rmb, web_rmb, 
sincetimes_end, core_end, free_end, paid_end, 
part_date
from hive.dow_jpnew_w.dws_user_daily_di
), 

daily_log as(
select a.date, a.role_id, 
b.install_date, 
a.login_days, a.firstpay_ts, a.money, a.money_rmb, a.web_rmb,
a.sincetimes_end, a.core_end, a.free_end, a.paid_end, 
a.part_date
from user_daily a
left join hive.dow_jpnew_w.dws_user_info_di b
on a.role_id = b.role_id
), 

daily_cal as(
select date, role_id, login_days, 
money, money_rmb, 
min(firstpay_ts) over(partition by role_id order by part_date rows between unbounded preceding and unbounded following) as firstpay_ts, 
sum(money) over(partition by role_id order by part_date rows between unbounded preceding and current row) as money_ac, 
sum(money_rmb) over(partition by role_id order by part_date rows between unbounded preceding and current row) as moneyrmb_ac, 
sum(web_rmb) over(partition by role_id order by part_date rows between unbounded preceding and current row) as webrmb_ac, 
last_value(sincetimes_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and current row) as sincetimes_end, 
last_value(core_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and current row) as core_end, 
last_value(free_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and current row) as free_end, 
last_value(paid_end) ignore nulls over(partition by role_id order by part_date rows between unbounded preceding and current row) as paid_end, 
lag(date, 1, install_date) over(partition by role_id order by date) as before_date,
lead(date, 1, current_date) over(partition by role_id order by date) as after_date, 
part_date
from daily_log
), 

daily_boolean_cal as(
select date, role_id, login_days, 
(case when date = install_date then 1 else 0 end) as is_new, 
(case when date(firstpay_ts) = date(part_date) then 1 else 0 end) as is_firstpay, 
(case when money > 0 then 1 else 0 end) as is_pay, 
(case when money_ac > 0 then 1 else 0 end) as is_paid, 
money_ac, moneyrmb_ac,
sincetimes_end, core_end, free_end, paid_end, 
before_date, after_date, 
part_date
from daily_cal
)

select
date, role_id, login_days, 
is_firstpay, is_pay, is_paid, is_new, 
money_ac, moneyrmb_ac, webrmb_ac, 
sincetimes_end, core_end, free_end, paid_end, 
sum(is_new) over(order by date) as newuser_ac, 
before_date, after_date, 
part_date
from daily_boolean_cal;
###