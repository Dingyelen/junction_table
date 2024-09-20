drop table if exists hive.dow_jpnew_w.dws_server_daily_df;

create table if not exists hive.dow_jpnew_w.dws_server_daily_df
(date date,
zone_id varchar,
open_date date, 
online_time bigint,
new_users bigint,
active_users bigint,
pay_users bigint,
firstpay_users bigint,
firstdaypay_users bigint,
money_rmb decimal(36, 2),
firstpay_rmb decimal(36, 2),
firstdaypay_rmb decimal(36, 2),
pay_count bigint,
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
transactional = true);

insert into hive.dow_jpnew_w.dws_server_daily_df
(date, zone_id, open_date, 
online_time, 
new_users, active_users, 
pay_users, firstpay_users, firstdaypay_users, 
money_rmb, firstpay_rmb, firstdaypay_rmb, pay_count, 
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
pay_count, money, money_rmb
from hive.dow_jpnew_w.dws_user_daily_di 
-- where part_date >= $start_date
-- and part_date <= $end_date
), 

user_daily_join as
(select a.date, a.part_date, a.role_id, 
a.level_min, a.level_max,
a.viplevel_min, a.viplevel_max, 
a.online_time, 
a.exchange_rate, a.pay_count, 
a.money, a.money_rmb, 
z.install_date, date(z.lastlogin_ts) as lastlogin_date, 
z.moneyrmb_ac, z.firstpay_date, z.firstpay_goodid, z.firstpay_level,
z.zone_id, z.channel, z.os, 
date_diff('day', z.install_date, a.date) as retention_day,
date_diff('day', z.firstpay_date, a.date) as pay_retention_day,
date_diff('day', z.install_date, z.firstpay_date) as firstpay_interval_days
from user_daily a
left join hive.dow_jpnew_w.dws_user_info_di z
on a.role_id = z.role_id
where z.is_test is null
),

daily_info as
(select date, part_date, zone_id, 
sum(online_time) as online_time,    
count(distinct case when retention_day = 0 then role_id else null end) as new_users, 
count(distinct role_id) as active_users, 
count(distinct case when money_rmb > 0 then role_id else null end) as pay_users, 
count(distinct case when money_rmb > 0 and firstpay_date = date then role_id else null end) as firstpay_users, 
count(distinct case when firstpay_date = date and retention_day = 0 then role_id else null end) as firstdaypay_users, 
sum(money_rmb) as money_rmb, 
sum(case when firstpay_date = date then money_rmb else null end) as firstpay_rmb, 
sum(case when firstpay_date = date and retention_day = 0 then money_rmb else null end) as firstdaypay_rmb, 
sum(pay_count) as pay_count
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
a.pay_users, a.firstpay_users, a.firstdaypay_users, 
a.money_rmb, a.firstpay_rmb, a.firstdaypay_rmb, a.pay_count, 
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
min(date) over(partition by zone_id order by part_date rows between unbounded preceding and unbounded following) as open_date, 
online_time, 
new_users, active_users, 
pay_users, firstpay_users, firstdaypay_users, 
money_rmb, firstpay_rmb, firstdaypay_rmb, pay_count, 
sum(new_users) over(partition by zone_id order by part_date rows between unbounded preceding and current row) as newusers_ac, 
sum(firstpay_users) over(partition by zone_id order by part_date rows between unbounded preceding and current row) as payusers_ac, 
sum(money_rmb) over(partition by zone_id order by part_date rows between unbounded preceding and current row) as moneyrmb_ac, 
leveltop_roleid, leveltop_level, leveltop_moneyrmb, 
paytop_roleid, paytop_level, paytop_moneyrmb
from server_info
)

select date, zone_id, open_date, 
online_time, 
new_users, active_users, 
pay_users, firstpay_users, firstdaypay_users, 
money_rmb, firstpay_rmb, firstdaypay_rmb, pay_count, 
newusers_ac, payusers_ac, moneyrmb_ac,
leveltop_roleid, leveltop_level, leveltop_moneyrmb, 
paytop_roleid, paytop_level, paytop_moneyrmb, 
date_diff('day', open_date, date) as retention_day, 
part_date
from server_win_info
;