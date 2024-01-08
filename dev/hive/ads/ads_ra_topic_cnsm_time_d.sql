-------------------------------------------------------------------------#
-- 任务名：      ads_ra_topic_cnsm_time_d
-- 目标表：      ads_ra_topic_cnsm_time_d
-- 源表：        rtassets_dw. dws_pub_ssys_liqd_time_d
-- 运行频度：    每日
-- 任务功能说明：实时资产TOPIC消费时间日表
-- 作者：        zhangyuanhui
-- 创建日期：    20221122
-------------------------------------------------------------------------#
-- 修改人        修改日期     修改内容
--
--
-------------------------------------------------------------------------#

--不启用锁特性
set hive.support.concurrency=false;
--动态分区严格模式
set hive.exec.dynamic.partition.mode=strict;
--日常跑批脚本关闭动态分区特性
set hive.exec.dynamic.partition=false;
--不进行parquet压缩
set parquet.compression=uncompressed;
--取消小表加载至内存中
set hive.auto.convert.join = false;
set mapreduce.job.queuename=root.users.rtassets;

--删除数据
alter table rtassets_ads.ads_ra_topic_cnsm_time_d drop if exists partition(part_ymd = '${batch_date}');

-- 更新数据
insert into table rtassets_ads.ads_ra_topic_cnsm_time_d partition (part_ymd = '${batch_date}') 
(
 data_date  --data_date
,etl_time   --etl_time
,ssys_code  --系统来源代码
,topic_name --topic名称
,cnsm_time  --消费时间
)
select
     '${batch_date}'                                         as data_date  --data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')   as etl_time   --etl_time
    ,a2.ssys_code                                            as ssys_code  --系统来源代码
    ,a1.topic_name                                           as topic_name --topic名称
    ,a2.liqd_time                                            as cnsm_time  --消费时间
from rtassets_ads.ads_ra_topic_cfg a1
left join (select * from rtassets_dw.dws_pub_ssys_liqd_time_d where part_ymd='${batch_date}') a2
on a1.ssys_code=a2.ssys_code
;
