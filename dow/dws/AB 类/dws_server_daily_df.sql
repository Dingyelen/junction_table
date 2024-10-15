drop table if exists hive.dow_jpnew_w.dws_server_daily_df;

create table if not exists hive.dow_jpnew_w.dws_server_daily_df
(date date,
zone_id varchar,
open_date date, 
online_time bigint,
new_users bigint,
active_users bigint,
paid_users bigint,
pay_users bigint,
web_users bigint,
firstpay_users bigint,
firstdaypay_users bigint,
pay_count bigint,
money_rmb decimal(36, 2),
web_rmb decimal(36, 2),
firstpay_rmb decimal(36, 2),
firstdaypay_rmb decimal(36, 2),
newusers_ac bigint, 
payusers_ac bigint, 
moneyrmb_ac decimal(36, 2), 
leveltop_roleid varchar,
leveltop_level bigint,
leveltop_moneyrmb decimal(36, 2),
paytop_roleid varchar,
paytop_level bigint,
paytop_moneyrmb decimal(36, 2),
retention_day bigint,
part_date varchar
)
with(format = 'ORC',
transactional = true,
partitioned_by = array['part_date']
);

insert into hive.dow_jpnew_w.dws_server_daily_df
(date, zone_id, open_date, 
online_time, 
new_users, active_users, 
paid_users, pay_users, web_users, 
firstpay_users, firstdaypay_users, 
pay_count, money_rmb, web_rmb, firstpay_rmb, firstdaypay_rmb, 
newusers_ac, payusers_ac, moneyrmb_ac,
leveltop_roleid, leveltop_level, leveltop_moneyrmb, 
paytop_roleid, paytop_level, paytop_moneyrmb, retention_day, 
part_date)

with user_daily as(
select date, part_date, role_id, 
level_min, level_max,
viplevel_min, viplevel_max, 
online_time, 
exchange_rate, 
pay_count, money, money_rmb, web_rmb
from hive.dow_jpnew_w.dws_user_daily_di
), 

user_daily_derive as(
select date, part_date, role_id, 
is_new, is_firstpay, is_pay, is_paid
from hive.dow_jpnew_w.dws_user_daily_derive_di
),

user_daily_join as
(select a.date, a.part_date, a.role_id, 
a.level_min, a.level_max,
a.viplevel_min, a.viplevel_max, 
b.is_new, 
a.online_time, 
a.exchange_rate, a.pay_count, 
a.money, a.money_rmb, a.web_rmb, b.is_firstpay, b.is_pay, b.is_paid, 
-- z.install_date, date(z.lastlogin_ts) as lastlogin_date, 
-- z.moneyrmb_ac, z.firstpay_date, z.firstpay_goodid, z.firstpay_level,
z.zone_id 
-- z.channel, z.os, 
-- date_diff('day', z.install_date, a.date) as retention_day,
-- date_diff('day', z.firstpay_date, a.date) as pay_retention_day,
-- date_diff('day', z.install_date, z.firstpay_date) as firstpay_interval_days
from user_daily a
left join user_daily_derive b
on a.role_id = b.role_id and a.part_date = b.part_date
left join hive.dow_jpnew_w.dws_user_info_di z
on a.role_id = z.role_id
where z.is_test is null
),

daily_info as
(select date, part_date, zone_id, 
sum(online_time) as online_time,    
sum(is_new) as new_users, 
count(distinct role_id) as active_users, 
sum(is_paid) as paid_users, 
sum(is_pay) as pay_users, 
count(distinct case when web_rmb > 0 then role_id else null end) as web_users, 
sum(is_firstpay) as firstpay_users, 
count(distinct case when is_firstpay = 1 and is_new = 1 then role_id else null end) as firstdaypay_users, 
sum(pay_count) as pay_count, 
sum(money_rmb) as money_rmb, 
sum(web_rmb) as web_rmb, 
sum(case when is_firstpay = 1 then money_rmb else null end) as firstpay_rmb, 
sum(case when is_firstpay = 1 and is_new = 1 then money_rmb else null end) as firstdaypay_rmb
from user_daily_join
group by 1, 2, 3
),

user_daily_rn as(
select date, part_date, role_id, 
zone_id, level_max, money_rmb, 
row_number() over(partition by date, zone_id order by level_max desc, money_rmb desc) as level_desc, 
row_number() over(partition by date, zone_id order by money_rmb desc, level_max desc) as money_desc
from user_daily_join
), 

server_info as
(select a.date, a.part_date, a.zone_id, 
a.online_time, 
a.new_users, a.active_users, 
a.paid_users, a.pay_users, a.web_users, a.firstpay_users, a.firstdaypay_users, 
a.pay_count, a.money_rmb, a.web_rmb, a.firstpay_rmb, a.firstdaypay_rmb, 
b.role_id as leveltop_roleid, b.level_max as leveltop_level, b.money_rmb as leveltop_moneyrmb, 
c.role_id as paytop_roleid, c.level_max as paytop_level, c.money_rmb as paytop_moneyrmb
from daily_info a
left join user_daily_rn b
on a.date = b.date and a.zone_id = b.zone_id 
left join user_daily_rn c
on a.date = c.date and a.zone_id = c.zone_id 
where b.level_desc = 1
and c.money_desc = 1
), 

server_win_info as(
select date, part_date, zone_id,
online_time, 
new_users, active_users, 
paid_users, pay_users, web_users, firstpay_users, firstdaypay_users, 
pay_count, money_rmb, web_rmb, firstpay_rmb, firstdaypay_rmb, 
sum(new_users) over(partition by zone_id order by part_date rows between unbounded preceding and current row) as newusers_ac, 
sum(firstpay_users) over(partition by zone_id order by part_date rows between unbounded preceding and current row) as payusers_ac, 
sum(money_rmb) over(partition by zone_id order by part_date rows between unbounded preceding and current row) as moneyrmb_ac, 
leveltop_roleid, leveltop_level, leveltop_moneyrmb, 
paytop_roleid, paytop_level, paytop_moneyrmb
from server_info
), 

open_date as(
select min(date) as open_date
from server_win_info
where newusers_ac >= 0
), 

server_daily_res as(
select date, zone_id, (select open_date from open_date) as open_date, 
online_time, 
new_users, active_users, 
paid_users, pay_users, web_users, 
firstpay_users, firstdaypay_users, 
pay_count, money_rmb, web_rmb, firstpay_rmb, firstdaypay_rmb, 
newusers_ac, payusers_ac, moneyrmb_ac,
leveltop_roleid, leveltop_level, leveltop_moneyrmb, 
paytop_roleid, paytop_level, paytop_moneyrmb, 
part_date
from server_win_info 
)

select date, zone_id, open_date, 
online_time, 
new_users, active_users, 
paid_users, pay_users, web_users, 
firstpay_users, firstdaypay_users, 
pay_count, money_rmb, web_rmb, firstpay_rmb, firstdaypay_rmb, 
newusers_ac, payusers_ac, moneyrmb_ac,
leveltop_roleid, leveltop_level, leveltop_moneyrmb, 
paytop_roleid, paytop_level, paytop_moneyrmb, 
date_diff('day', open_date, date) as retention_day, 
part_date
from server_daily_res
;