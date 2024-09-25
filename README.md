# 1. 目的
- 梳理数仓 dws，ads 通用规范
- 保证所有项目数据处理一致性、规范性
- 满足新项目上线，快速接入数据，构建高复用性数据模型
- 为可视化打好统一规范的基础数据

# 2. 模型版本现状与改动
## 2.1 现状
- 目前 DWS 有 10 个表模型，包含了日活，小时活跃，用户属性，主题快照等内容。ADS 有 15 个表模型，包含了核心货币产销存、核心 KPI 指标、同期群指标等内容。

# 3. 数据模型规划
## 3.1 DWS（Data Warehouse）
1. 必要性说明：
  - A类：所有项目必须，通用看板依赖项
  - B类：非必要，根据日常分析，或者非通用看板增加或者改写
  - C类：不必要，单纯 DOW 独有内容
2. 根据自己更新频率调整表名
3. 表血缘

|表名|必要性|依赖情况|备注|
|--------|--------|--------|--------|
|dws_user_daily_di|A类|独立||
|dws_user_info_di|A类|dws_user_daily_di||
|dws_user_daily_derive_df|A类|dws_user_daily_di、dws_user_info_di||
|dws_user_hourly_hi|A类|独立||
|dws_core_snapshot_di|A类|独立||
|dws_token_info_mf|A类|独立||
|dws_server_daily_df|B类|dws_user_daily_di||
|dws_summon_daily_di|B类|独立|停运，分区待优化|
|dws_user_daily2_di|C类|dws_user_daily_di||
|dws_user_info_mi|C类|独立||


## 3.2 ADS（Analytical Data Store）
1. 表血缘

|表名|必要性|依赖情况|
|--------|--------|--------|
|ads_kpi_daily_di|A类|dws_user_daily_di，dws_user_info_di|
|ads_kpi_daily_hi|A类|dws_user_hourly_hi，dws_user_info_di|
|ads_core_addreason_di|A类|dws_core_snapshot_di，dws_user_info_di|
|ads_core_costreason_di|A类|dws_core_snapshot_di，dws_user_info_di|
|ads_active_daily_di|A类|dws_user_daily_di，dws_user_info_di|
|ads_retention_daily_di|A类|dws_user_daily_di，dws_user_info_di|
|ads_user_retention_di|A类|dws_user_daily_di，dws_user_info_di|
|ads_level_daily_di|B类|dws_user_daily_di，dws_user_info_di|
|ads_kpi_server_df|B类|dws_server_daily_df|
|ads_kpi_daily_hf|华清|dws_user_hourly_hi，dws_user_info_di|
|ads_kpi_life_df|华清|dws_user_daily_di，dws_user_info_di|
|ads_herostatus_upgrade_df|B类|独立|
|ads_act_battle_df|C类|dws_user_daily_di,dws_user_daily2_di|
|ads_act_daily_df|C类|dws_user_daily_di,dws_user_info_di|
|ads_battle_herostatus_df|C类|独立|

## 3.3 更新时间表
pass

# 4. 新产品接入
1. 合并必要事件表，形成视图。
  1. 按照格式填写合并事件，协调@冉桦林 完成。
  2. 全项目事件合并汇总
2. 规划项目所需报表，并构思更为合理的更新时间
3. 检查表名，根据数据库表名规范，以及更新时间确定
  1. 通用大数据规范-对应分析
4. 上传白名单，dim_testusers
5. 完成 DWS 、ADS 层 A类 表代码改写，根据游戏特性挑选非必选代码改写
6. 协调@杨德贵 完成历史数据
7. 根据数据依赖情况和血缘（参考2），结合实际情况错峰部署各表更新时间