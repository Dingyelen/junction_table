create table if not exists hive.mushroom2_tw_w.ads_core_daily_di
(date date,
zone_id varchar,
channel varchar,
core_add bigint,
core_cost bigint,
core_end bigint,
users bigint,
users_add bigint,
users_cost bigint,
part_date varchar
)
with(partitioned_by = array['part_date']);

delete from hive.mushroom2_tw_w.ads_core_daily_di
where part_date >= $start_date
and part_date <= $end_date;

insert into hive.mushroom2_tw_w.ads_core_daily_di
(date, zone_id, channel,
core_add, core_cost, core_end,
users, users_add, users_cost,
part_date)

with dws_core_daily as
(select date, role_id, coregain_detail, corecost_detail, core_end, part_date,
reduce(map_values(cast(json_parse(coregain_detail) as map(varchar, bigint))), 0, (s, x) -> s + x, s -> s) as core_add,
reduce(map_values(cast(json_parse(corecost_detail) as map(varchar, bigint))), 0, (s, x) -> s + x, s -> s) as core_cost
from hive.mushroom2_tw_w.dws_core_snapshot_di
where part_date>=$start_date
and part_date<=$end_date
),

dws_core_daily_join as
(select a.date, a.part_date, a.role_id, a.core_add, a.core_cost, a.core_end,
b.install_date, date(b.lastlogin_ts) as lastlogin_date, 
b.moneyrmb_ac, b.firstpay_date, b.firstpay_goodid, b.firstpay_level,
b.zone_id as zone_id, 
b.channel as channel,
date_diff('day', b.install_date, a.date) as retention_day,
date_diff('day', b.firstpay_date, a.date) as pay_retention_day,
date_diff('day', b.install_date, b.firstpay_date) as firstpay_interval_days
from dws_core_daily a
left join hive.mushroom2_tw_w.dws_user_info_di b
on a.role_id = b.role_id
where b.is_test is null
),

core_daily_agg as
(select date, part_date, zone_id, channel, 
sum(core_add) as core_add, sum(core_cost) as core_cost, sum(core_end) as core_end,
count(distinct role_id) as users,
count(distinct (case when core_add > 0 then role_id else null end)) as users_add,
count(distinct (case when core_cost > 0 then role_id else null end)) as users_cost
from dws_core_daily_join
group by 1, 2, 3, 4
)

select date, zone_id, channel,
core_add, core_cost, core_end,
users, users_add, users_cost,
part_date
from core_daily_agg
;