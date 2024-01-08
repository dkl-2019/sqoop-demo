-------------------------------------------------------------------------#
-- 任务名：       dwd_acc_tran_acct_ods_uas_kbssuser_stk_trdacct.q
-- 目标表：       dwd_acc_tran_acct
-- 源表：         rtassets_ods.ods_uas_kbssuser_stk_trdacct
-- 运行频度：     每日
-- 任务功能说明：交易账户
-- 作者：        xujianliang
-- 创建日期：    20220817
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
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set mapreduce.job.queuename=root.users.rtassets;

--删除数据
alter table rtassets_dw.dwd_acc_tran_acct drop if exists partition (ssys_tab = 'ods_uas_kbssuser_stk_trdacct');

-- 更新数据
insert into table rtassets_dw.dwd_acc_tran_acct partition (ssys_tab = 'ods_uas_kbssuser_stk_trdacct')
(
   data_date
   ,etl_time
   ,ide_id                        --主体id
   ,acct_num                      --账户编码
   ,acct_type_code                --账户类型代码
   ,trade_acct                    --交易账户
   ,cptl_acct                     --资金账户
   ,ide_num                       --主体编码
   ,trade_mkt                     --交易市场
   ,open_acct_org_num             --开户机构编码
   ,trade_acct_seq                --交易账户序号
   ,ofer_trade_acct               --报盘交易账户
   ,trade_acct_type_code          --交易账户类型代码
   ,trade_acct_class_code         --交易账户类别代码
   ,trade_acct_name               --交易账户名称
   ,acct_state                    --账户状态
   ,trade_appoint_status_code     --交易指定状态代码
   ,repo_appoint_status_code      --回购指定状态代码
   ,trade_unit                    --交易单元
   ,open_acct_date                --开户日期
   ,close_acct_date               --销户日期
   ,unif_num                      --一码通编号
   ,sub_acct_num                  --子账户编码
   ,sub_acct_type_code            --子账户类型代码
   ,rep_status_code               --申报状态代码
   ,sub_acct_state_code           --子账户状态代码
   ,contr_acct_lvl_code           --合约账户级别代码
   ,contr_acct_usage              --合约账户用途
   ,reg_org_num                   --登记机构编码
   ,reg_acct                      --登记账户
   ,pd_sub                        --产品子类
   ,update_date                   --更新日期
   ,ext_trade_acct                --外部交易账户
   ,ext_trade_unit                --外部交易单元
   ,ext_ast_acct                  --外部资产账户
   ,oper_flag                     --操作标志
   ,buy_in_limit                  --买入额度
   ,fin_receipt_type              --融资开仓类型
   ,pchs_qlf                      --申购资格
   ,pchs_mode                     --申购模式
   ,crdt_shaholder_flag           --信用股东标志
   ,fin_crdt_limit                --融资信用额度
   ,shts_crdt_limit               --融券信用额度
   ,sum_credit_amount             --总信用额度
   ,shts_shaholder                --融券股东
   ,ssys_code                     --系统来源代码
   ,ssys_num                      --系统来源编码
)
select
     '${batch_date}'                               as data_date
    ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as etl_time
    ,cast(t1.cust_code as string)                          as ide_id                                   --主体id
    ,concat('acc','|','4','|',cast(t1.cuacct_code as string),'|',t1.stkbd,'|',t1.trdacct)          as acct_num             --账户编码
    ,'4'                                                   as acct_type_code                           --账户类型代码
    ,t1.trdacct                                            as trade_acct                               --交易账户
    ,cast(t1.cuacct_code as string)                        as cptl_acct                                --资金账户
    ,null                                                  as ide_num                                  --主体编码
    ,case when a1.std_code is null then concat('@',t1.stkex) else a1.std_code end                  as trade_mkt            --交易市场
    ,null                                                  as open_acct_org_num                        --开户机构编码
    ,cast(t1.trdacct_sn as string)                         as trade_acct_seq                           --交易账户序号
    ,t1.trdacct_exid                                       as ofer_trade_acct                          --报盘交易账户
    ,t1.trdacct_type                                       as trade_acct_type_code                     --交易账户类型代码
    ,t1.trdacct_excls                                      as trade_acct_class_code                    --交易账户类别代码
    ,t1.trdacct_name                                       as trade_acct_name                          --交易账户名称
    ,t1.trdacct_status                                     as acct_state                               --账户状态
    ,t1.treg_status                                        as trade_appoint_status_code                --交易指定状态代码
    ,t1.breg_status                                        as repo_appoint_status_code                 --回购指定状态代码
    ,t1.stkpbu                                             as trade_unit                               --交易单元
    ,cast(t1.open_date as string)                          as open_acct_date                           --开户日期
    ,cast(t1.close_date as string)                         as close_acct_date                          --销户日期
    ,t1.ymt_code                                           as unif_num                                 --一码通编号
    ,null                                                  as sub_acct_num                             --子账户编码
    ,null                                                  as sub_acct_type_code                       --子账户类型代码
    ,null                                                  as rep_status_code                          --申报状态代码
    ,null                                                  as sub_acct_state_code                      --子账户状态代码
    ,null                                                  as contr_acct_lvl_code                      --合约账户级别代码
    ,null                                                  as contr_acct_usage                         --合约账户用途
    ,cast(t1.int_org as string)                            as reg_org_num                              --登记机构编码
    ,null                                                  as reg_acct                                 --登记账户
    ,null                                                  as pd_sub                                   --产品子类
    ,cast(null as timestamp)                               as update_date                              --更新日期
    ,null                                                  as ext_trade_acct                           --外部交易账户
    ,null                                                  as ext_trade_unit                           --外部交易单元
    ,null                                                  as ext_ast_acct                             --外部资产账户
    ,null                                                  as oper_flag                                --操作标志
    ,cast(null as decimal(24,6))                           as buy_in_limit                             --买入额度
    ,null                                                  as fin_receipt_type                         --融资开仓类型
    ,null                                                  as pchs_qlf                                 --申购资格
    ,null                                                  as pchs_mode                                --申购模式
    ,null                                                  as crdt_shaholder_flag                      --信用股东标志
    ,cast(null as decimal(24,6))                           as fin_crdt_limit                           --融资信用额度
    ,cast(null as decimal(24,6))                           as shts_crdt_limit                          --融券信用额度
    ,cast(null as decimal(24,6))                           as sum_credit_amount                        --总信用额度
    ,null                                                  as shts_shaholder                           --融券股东
    ,'uas'                                                 as ssys_code                                --系统来源代码
    ,concat(cast(t1.cuacct_code as string),'|',t1.stkbd,'|',t1.trdacct)    as ssys_num                 --系统来源编码
from (select * from rtassets_ods.ods_uas_kbssuser_stk_trdacct where part_ymd='${batch_date}') t1
left join
(select * from rtassets_dw.dwd_pub_code_map_info where src_sys_code='uas' and src_code_type='stkex' and std_code_type ='trade_mkt')a1
on t1.stkex=a1.src_code
;
