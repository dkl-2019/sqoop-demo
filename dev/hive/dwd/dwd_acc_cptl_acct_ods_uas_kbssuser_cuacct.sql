-------------------------------------------------------------------------#
-- 任务名：       dwd_acc_cptl_acct_ods_uas_kbssuser_cuacct.q
-- 目标表：       dwd_acc_cptl_acct
-- 源表：         rtassets_ods.ods_uas_kbssuser_cuacct
-- 运行频度：     每日
-- 任务功能说明：资金账户 
-- 作者：        xujianliang
-- 创建日期：    20220818
-------------------------------------------------------------------------#
--不启用锁特性
set hive.support.concurrency=false;
--动态分区严格模式
set hive.exec.dynamic.partition.mode=strict;
--日常跑批脚本关闭动态分区特性
set hive.exec.dynamic.partition=false;
--不进行parquet压缩
set parquet.compression=uncompressed;
set mapreduce.job.queuename=root.users.rtassets;


--删除数据
alter table rtassets_dw.dwd_acc_cptl_acct drop if exists partition (ssys_tab = 'ods_uas_kbssuser_cuacct');

-- 更新数据
insert into table rtassets_dw.dwd_acc_cptl_acct partition (ssys_tab = 'ods_uas_kbssuser_cuacct')
(
   data_date
   ,etl_time
   ,ide_id               --主体id
   ,acct_num             --账户编码
   ,acct_type_code       --账户类型代码
   ,cptl_acct            --资金账户
   ,ide_num              --主体编码
   ,open_acct_org_num    --开户机构编码
   ,open_acct_date       --开户日期
   ,close_acct_date      --销户日期
   ,acct_state           --账户状态
   ,main_acct_flag       --主账户标识
   ,cptl_acct_attr_code  --资金账户属性代码
   ,cptl_acct_lvl_code   --资金账户级别代码
   ,cptl_acct_room_code  --资金账户组别代码
   ,cptl_acct_class_code --资金账户类别代码
   ,cptl_acct_ext_attr   --资金账户扩展属性
   ,cptl_limit           --资金限制
   ,update_date          --更新日期
   ,ssys_code            --系统来源代码
   ,ssys_num             --系统来源编码
)
select
     '${batch_date}'                                           as data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')     as etl_time
    ,cast(t1.user_code as string)                              as ide_id               --主体id
    ,concat('acc','|','2','|',cast(t1.cuacct_code as string))  as acct_num             --账户编码
    ,'2'                                                       as acct_type_code       --账户类型代码
    ,cast(t1.cuacct_code as string)                            as cptl_acct            --资金账户
    ,null                                                      as ide_num              --主体编码
    ,cast(t1.int_org as string)                                as open_acct_org_num    --开户机构编码
    ,cast(t1.open_date as string)                              as open_acct_date       --开户日期
    ,cast(t1.close_date as string)                             as close_acct_date      --销户日期
    ,t1.cuacct_status                                          as acct_state           --账户状态
    ,t1.main_flag                                              as main_acct_flag       --主账户标识
    ,t1.cuacct_attr                                            as cptl_acct_attr_code  --资金账户属性代码
    ,t1.cuacct_lvl                                             as cptl_acct_lvl_code   --资金账户级别代码
    ,t1.cuacct_grp                                             as cptl_acct_room_code  --资金账户组别代码
    ,t1.cuacct_cls                                             as cptl_acct_class_code --资金账户类别代码
    ,t1.cuacct_ext_attr                                        as cptl_acct_ext_attr   --资金账户扩展属性
    ,null                                                      as cptl_limit           --资金限制
    ,null                                                      as update_date          --更新日期
    ,'uas'                                                     as ssys_code            --系统来源代码
    ,'ods_uas_kbssusr_cuacct'                                  as ssys_num             --系统来源编码
from rtassets_ods.ods_uas_kbssuser_cuacct t1
where part_ymd='${batch_date}' 
;
