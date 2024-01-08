-------------------------------------------------------------------------#
-- 任务名：      dws_pub_ssys_liqd_time_d
-- 目标表：      dws_pub_ssys_liqd_time_d
-- 源表：        rtassets_ods.ods_cts_kgdb_sett_log
--               rtassets_ods.ods_cts_kgdb_sett_task
--               rtassets_ods.ods_mts_kgdbrzrq_sett_log
--               rtassets_ods.ods_mts_kgdbrzrq_sett_task
--               rtassets_ods.ods_ois_kbssoptsett_user_log
--               rtassets_ods.ods_otc_otcnewsett_otc_sett_log
-- 运行频度：    每日
-- 任务功能说明：源系统清算时间日表
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
--开启多job并发
set hive.exec.parallel=true;
set mapreduce.job.queuename=root.users.rtassets;

--删除数据
alter table rtassets_dw.dws_pub_ssys_liqd_time_d drop if exists partition(part_ymd = '${batch_date}');

-- 更新数据
insert into table rtassets_dw.dws_pub_ssys_liqd_time_d partition (part_ymd = '${batch_date}') 
(
 data_date  --data_date
,etl_time   --etl_time
,ssys_code  --系统来源代码
,liqd_time  --清算时间
)
select
     '${batch_date}'                                         as data_date  --data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')   as etl_time   --etl_time
    ,'cts'                                                   as ssys_code  --来源系统代码
    ,cast(cast(cast(concat(concat_ws('-',substr('${batch_date}',1,4),substr('${batch_date}',5,2),substr('${batch_date}',7,2)),' ',max(end_time),'.000') as timestamp) as double)*1000 as bigint)  as liqd_time  --清算时间
from rtassets_ods.ods_cts_kgdb_sett_log
where part_ymd='${batch_date}' and cast(trd_date as string)='${batch_date}'
      and sett_task in (select sett_task from rtassets_ods.ods_cts_kgdb_sett_task where sett_task_name like '%归档%')
union all
select
     '${batch_date}'                                         as data_date  --data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')   as etl_time   --etl_time
    ,'mts'                                                   as ssys_code  --来源系统代码
    ,cast(cast(cast(concat(concat_ws('-',substr('${batch_date}',1,4),substr('${batch_date}',5,2),substr('${batch_date}',7,2)),' ',max(end_time),'.000') as timestamp) as double)*1000 as bigint)  as liqd_time  --清算时间
from rtassets_ods.ods_mts_kgdbrzrq_sett_log
where part_ymd='${batch_date}' and cast(trd_date as string)='${batch_date}'
      and sett_task in (select sett_task from rtassets_ods.ods_mts_kgdbrzrq_sett_task where sett_task_name like '%归档%')
union all
select
     '${batch_date}'                                            as data_date  --data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')      as etl_time   --etl_time
    ,'ois'                                                      as ssys_code  --来源系统代码
    ,cast(cast(cast(max(occur_time) as timestamp) as double)*1000 as bigint)    as liqd_time  --清算时间
from rtassets_ods.ods_ois_kbssoptsett_user_log
where part_ymd='${batch_date}' and cast(occur_date as string)='${batch_date}'
      and (cast(biz_code as string)='54070182' or biz_content like '%数据归档处理%')
union all
select
     '${batch_date}'                                                    as data_date  --data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')              as etl_time   --etl_time
    ,'otc'                                                              as ssys_code  --来源系统代码
    ,cast(cast(cast(substr(cast(max(sett_end_time) as string),1,23) as timestamp) as double)*1000 as bigint) as liqd_time  --清算时间
from rtassets_ods.ods_otc_otcnewsett_otc_sett_log
where part_ymd='${batch_date}' and cast(sett_date as string)='${batch_date}' and sett_func_code = 76
;