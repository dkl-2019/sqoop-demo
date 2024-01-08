-------------------------------------------------------------------------#
-- 任务名：       dwd_ide_cust_ods_uas_kbssuser_users.q
-- 目标表：       dwd_ide_cust  客户基础信息表
-- 源表：         ods_uas_kbssuser_users
-- 源表：         ods_uas_kbssuser_customer
-- 源表：         ods_uas_kbssuser_cust_other_info
-- 源表：         ods_uas_kbssuser_user_basic_info
-- 源表：         ods_uas_kbssuser_org
-- 运行频度：     每日
-- 任务功能说明：客户
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
alter table rtassets_dw.dwd_ide_cust drop partition(part_ymd='${batch_date}',ssys_tab='ods_uas_kbssuser_users');

-- 更新数据
insert into table rtassets_dw.dwd_ide_cust partition(part_ymd='${batch_date}',ssys_tab='ods_uas_kbssuser_users')
(
    data_date
    ,etl_time                     --数据加载时间
    ,cust_num                     --客户编码
    ,ide_id                       --主体id
    ,ide_num                      --主体编码
    ,ide_class_code               --主体类别代码
    ,topacct_num                  --一账通编号
    ,cust_name                    --客户名称
    ,cust_abbr                    --客户简称
    ,cust_engname                 --客户英文名称
    ,cust_en_abbr                 --客户英文简称
    ,cust_type                    --客户类型
    ,cust_busi_type               --客户业务类型
    ,cust_class                   --客户类别
    ,cust_group                   --客户分组
    ,cust_state                   --客户状态
    ,open_acct_date               --开户日期
    ,close_acct_date              --销户日期
    ,cust_oth_flag                --客户其他标志
    ,innerorg_num                 --内部机构编码
    ,innerorg_name                --内部机构名称
    ,oper_org                     --操作机构
    ,open_acct_agt                --开户代理人
    ,serv_lvl                     --服务级别
    ,aml_lvl                      --反洗钱等级
    ,aml_cnfm_date                --反洗钱确认日期
    ,aml_cust_type                --反洗钱客户类型
    ,cert_type                    --证件类型
    ,cert_num                     --证件号码
    ,iss_cert_org                 --发证机关
    ,cert_start_dt                --证件开始日期
    ,cert_valdate                 --证件有效日期
    ,cert_zip                     --证件邮编
    ,cert_addr                    --证件地址
    ,cert_check_flag              --证件卡校验标志
    ,cert_readcard_flag           --证件卡读卡标志
    ,pstcd                        --邮政编码
    ,conta_addr                   --联系地址
    ,cont_tel                     --联系电话
    ,fax_tel                      --传真电话
    ,mob_phn                      --移动电话
    ,eml                          --电子邮箱
    ,nation                       --国籍
    ,nplc_or_reg_addr             --籍贯或者注册地
    ,birthday                     --出生日期或者注册日期
    ,office_tel                   --办公电话
    ,first_cont_tel               --首选联系电话
    ,office_addr                  --办公地址
    ,corp_addr                    --公司地址
    ,first_conta_addr             --首选联系地址
    ,open_acct_src                --开户来源
    ,mobile_check_flag            --手机校验标识
    ,email_check_flag             --邮箱校验标识
    ,bar_no                       --磁卡号码
    ,standard_flag                --规范标志（复数）
    ,risk_factor                  --风险因素（复数）
    ,oper_chnl                    --操作渠道（复数）
    ,crdt_lvl                     --信用级别
    ,remote_sn_agt                --远程签署协议
    ,cust_ext_attr                --客户拓展属性
    ,ide_status                   --主体身份
    ,dom_overs_status             --境内外身份
    ,spec_status                  --特殊身份
    ,filcabinet_num               --文件柜编号
    ,earliest_trade_dt            --最早交易日期
    ,ssys_code                    --系统来源代码
    ,ssys_num                     --系统来源编码
)
select '${batch_date}'                                             as data_date
      ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')       as etl_time  -- 数据加载时间
      ,cast(t1.user_code as string)                                as cust_num                     --客户编码
      ,cast(t1.user_code as string)                                as ide_id                       --主体id
      ,concat('ide','|','1','|',cast(t1.user_code as string))      as ide_num                      --主体编码
      ,'1'                                                         as ide_class_code               --主体类别代码
      ,null                                                        as topacct_num                  --一账通编号
      ,t1.user_name                                                 as cust_name                    --客户名称
      ,null                                                        as cust_abbr                    --客户简称
      ,null                                                        as cust_engname                 --客户英文名称
      ,null                                                        as cust_en_abbr                 --客户英文简称
      ,t1.user_type                                                as cust_type                    --客户类型
      ,'0'                                                         as cust_busi_type               --客户业务类型
      ,t2.cust_cls                                                 as cust_class                   --客户类别
      ,t3.cust_grp                                                 as cust_group                   --客户分组
      ,t2.cust_status                                              as cust_state                   --客户状态
      ,cast(t1.open_date as string)                                as open_acct_date               --开户日期
      ,cast(t1.close_date as string)                               as close_acct_date              --销户日期
      ,t3.cust_flag                                                as cust_oth_flag                --客户其他标志
      ,cast(t1.int_org as string)                                  as innerorg_num                 --内部机构编码
      ,t5.org_name                                                 as innerorg_name                --内部机构名称
      ,cast(t2.oper_org as string)                                 as oper_org                     --操作机构
      ,cast(t2.open_agent as string)                               as open_acct_agt                --开户代理人
      ,t2.service_lvl                                              as serv_lvl                     --服务级别
      ,t3.aml_lvl                                                  as aml_lvl                      --反洗钱等级
      ,cast(t3.aml_lvl_date  as string)                            as aml_cnfm_date                --反洗钱确认日期
      ,t3.aml_cust_type                                            as aml_cust_type                --反洗钱客户类型
      ,t1.id_type                                                  as cert_type                    --证件类型
      ,t1.id_code                                                  as cert_num                     --证件号码
      ,t4.id_iss_agcy                                              as iss_cert_org                 --发证机关
      ,null                                                        as cert_start_dt                --证件开始日期
      ,null                                                        as cert_valdate                 --证件有效日期
      ,t4.id_zip_code                                              as cert_zip                     --证件邮编
      ,t4.id_addr                                                  as cert_addr                    --证件地址
      ,t3.idcard_check_flag                                        as cert_check_flag              --证件卡校验标志
      ,t3.idcard_read_flag                                         as cert_readcard_flag           --证件卡读卡标志
      ,t4.zip_code                                                 as pstcd                        --邮政编码
      ,t4.address                                                  as conta_addr                   --联系地址
      ,t4.tel                                                      as cont_tel                     --联系电话
      ,t4.fax                                                      as fax_tel                      --传真电话
      ,t4.mobile_tel                                               as mob_phn                      --移动电话
      ,t4.email                                                    as eml                          --电子邮箱
      ,t4.citizenship                                              as nation                       --国籍
      ,t4.native_place                                             as nplc_or_reg_addr             --籍贯或者注册地
      ,t4.birthday                                                 as birthday                     --出生日期
      ,t4.office_tel                                               as office_tel                   --办公电话
      ,t4.linktel_order                                            as first_cont_tel               --首选联系电话
      ,t4.office_addr                                              as office_addr                  --办公地址
      ,t4.corp_addr                                                as corp_addr                    --公司地址
      ,t4.linkaddr_order                                           as first_conta_addr             --首选联系地址
      ,t4.open_source                                              as open_acct_src                --开户来源
      ,t4.tel_chk_flag                                             as mobile_check_flag            --手机校验标识
      ,t4.email_chk_flag                                           as email_check_flag             --邮箱校验标识
      ,t2.card_id                                                  as bar_no                       --磁卡号码
      ,t2.criterion                                                as standard_flag                --规范标志（复数）
      ,t2.risk_factor                                              as risk_factor                  --风险因素（复数）
      ,t2.channels                                                 as oper_chnl                    --操作渠道（复数）
      ,t2.credit_lvl                                               as crdt_lvl                     --信用级别
      ,t2.remote_protocol                                          as remote_sn_agt                --远程签署协议
      ,t2.cust_ext_attr                                            as cust_ext_attr                --客户拓展属性
      ,t2.subject_identity                                         as ide_status                   --主体身份
      ,t2.inoutside_identity                                       as dom_overs_status             --境内外身份
      ,t2.special_status                                           as spec_status                  --特殊身份
      ,t3.filingcabinet_no                                         as filcabinet_num               --文件柜编号
      ,null                                                        as earliest_trade_dt            --最早交易日期
      ,'uas'                                                       as ssys_code                    --系统来源代码
      ,cast(t1.user_code as string)                                                as ssys_num                     --系统来源编码
from (select * from rtassets_ods.ods_uas_kbssuser_users where part_ymd='${batch_date}') t1
left join (select * from rtassets_ods.ods_uas_kbssuser_customer where part_ymd='${batch_date}') t2
 on t1.user_code = t2.cust_code
left join (select * from rtassets_ods.ods_uas_kbssuser_cust_other_info where part_ymd='${batch_date}') t3
 on t1.user_code = t3.cust_code
left join (select * from rtassets_ods.ods_uas_kbssuser_user_basic_info where part_ymd='${batch_date}') t4
 on t1.user_code = t4.user_code
left join (select * from rtassets_ods.ods_uas_kbssuser_org where part_ymd='${batch_date}' and org_type='0') t5
on t1.int_org=t5.org_code
;
