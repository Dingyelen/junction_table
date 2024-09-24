# 1. 目的
- 梳理数仓 dws，ads 通用规范
- 保证所有项目数据处理一致性、规范性
- 满足新项目上线，快速接入数据，构建高复用性数据模型
- 为可视化打好统一规范的基础数据

# 2. 模型版本现状与改动
## 2.1 现状
- 目前 DWS 有 11 个表模型，包含了日活，小时活跃，用户属性，主题快照等内容。ADS 有 17 个表模型，包含了核心货币产销存、核心 KPI 指标、同期群指标等内容。
## 2.2 较 v0511的改动
表名
表层
调整类型
调整简述
调整原因
ads_active_daily_di
ads
逻辑矫正
date_cube_agg 计算表修改逻辑
原先方式会丢失小批量数据
# 3. 数据模型规划
## 3.1 DWS（Data Warehouse）
1. 必要性说明：
  - 1类：所有项目必须，通用看板依赖项
  - 2类：非必要，根据日常分析，或者非通用看板增加或者改写
  - 3类：不必要，单纯 DOW 独有内容
2. 根据自己更新频率调整表名
3. 表血缘

|表名|必要性|依赖情况|
|--------|--------|--------|
|dws_user_daily_di|1类|独立|
|dws_user_hourly_hi|1类|独立|
|dws_user_info_di|1类|dws_user_daily_di|
|dws_user_daily_derive_df|1类|dws_user_daily_di、dws_user_info_di|
|dws_core_snapshot_di|1类|独立|
|dws_item_snapshot_di|1类|独立|
|dws_hero_snapshot_di|2类|独立|
|dws_summon_snapshot_di|2类|独立|
|dws_artifact_snapshot_di|3类|独立|
|dws_flag_snapshot_di|3类|独立|
|dws_user_info_mi|2类|独立|

## 3.2 ADS（Analytical Data Store）
1. 表血缘

|表名|必要性|依赖情况|
|--------|--------|--------|
|ads_kpi_daily_di|1类|dws_user_daily_di，dws_user_info_di|
|ads_kpi_hourly_di|1类|dws_user_hourly_hi，dws_user_info_di|
|ads_kpi_monthly_mi|1类|独立|
|ads_active_daily_di|1类|dws_user_daily_di，dws_user_info_di|
|ads_retention_daily_di|1类|dws_user_daily_di，dws_user_info_di|
|ads_user_retention_di|1类|dws_user_daily_di，dws_user_info_di|
|ads_level_daily_di|2类|dws_user_daily_di，dws_user_info_di|
|ads_viplevel_daily_di|2类|dws_user_daily_di，dws_user_info_di|
|ads_core_addreason_di|1类|dws_core_snapshot_di，dws_user_info_di|
|ads_core_costreason_di|1类|dws_core_snapshot_di，dws_user_info_di|
|ads_core_daily_di|2类|dws_core_snapshot_di，dws_user_info_di|
|ads_item_di|2类|dws_item_snapshot_di|
|ads_hero_df|2类|dws_hero_snapshot_di|
|ads_summon_di|2类|dws_summon_snapshot_di|
|ads_flag_df|3类|dws_flag_snapshot_di|
|ads_artifact_df|3类|dws_artifact_snapshot_di|
|ads_item_end_df|3类|ads_item_di|

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
5. 完成 DWS 、ADS 层 1类 表代码改写，根据游戏特性挑选非必选代码改写
6. 协调@杨德贵 完成历史数据
7. 根据数据依赖情况和血缘（参考2），结合实际情况错峰部署各表更新时间
   
