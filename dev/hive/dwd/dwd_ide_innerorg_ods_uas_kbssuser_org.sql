-------------------------------------------------------------------------#
-- 任务名：       dwd_ide_innerorg_ods_uas_kbssuser_org.q
-- 目标表：       dwd_ide_innerorg
-- 源表：         ods_uas_kbssuser_org
--                dwm_ide_ide
-- 运行频度：     每日
-- 任务功能说明：内部机构
-- 作者：        xujianliang
-- 创建日期：    20220818
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
set mapreduce.job.queuename=root.users.rtassets;

alter table rtassets_dw.dwd_ide_innerorg drop if exists partition(ssys_tab='ods_uas_kbssuser_org');
-- 更新数据
insert into table rtassets_dw.dwd_ide_innerorg partition(ssys_tab='ods_uas_kbssuser_org')
(
      data_date
      ,etl_time
      ,ide_id                   --主体id
      ,ide_num                  --主体编码
      ,ide_class_code           --主体类别代码
      ,innerorg_fullname        --内部机构全称
      ,innerorg_abbr            --内部机构简称
      ,org_tier_code            --机构层级代码
      ,super_org_num            --上级机构编码
      ,ide_status_code          --主体状态代码
      ,innerorg_engname         --内部机构英文名称
      ,innerorg_en_abbr         --内部机构英文简称
      ,stp_date                 --成立日期
      ,undo_date                --撤销日期
      ,innerorg_class_code      --内部机构类别代码
      ,innerorg_num             --内部机构编码
      ,area_code                --区域代码
      ,zip                      --邮编
      ,acct_prfx                --账户前缀
      ,update_date              --更新日期
      ,tel_num                  --电话号码
      ,conta_addr               --联系地址
      ,ssys_code                --系统来源代码
      ,ssys_num                 --系统来源编码
)
select '${batch_date}'                                          as data_date
      ,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')            as etl_time
      ,cast(t1.org_code as string)                                      as ide_id                   --主体id
      ,concat('ide','|','2','|',cast(t1.org_code as string),'|',t1.org_type)            as ide_num                  --主体编码
      ,'2'                                                              as ide_class_code           --主体类别代码
      ,t1.org_full_name                                                 as innerorg_fullname        --内部机构全称
      ,t1.org_name                                                      as innerorg_abbr            --内部机构简称
      ,null                                                             as org_tier_code            --机构层级代码
      ,cast(t1.parent_org as string)                                    as super_org_num            --上级机构编码
      ,''                                                               as ide_status_code          --主体状态代码
      ,null                                                             as innerorg_engname         --内部机构英文名称
      ,null                                                             as innerorg_en_abbr         --内部机构英文简称
      ,cast(t1.open_date as string)                                     as stp_date                 --成立日期
      ,null                                                             as undo_date                --撤销日期
      ,t1.org_cls                                                       as innerorg_class_code      --内部机构类别代码
      ,cast(t1.org_code as string)                                      as innerorg_num             --内部机构编码
      ,''                                                               as area_code                --区域代码
      ,t1.zip_code                                                      as zip                      --邮编
      ,t1.acct_prefix                                                   as acct_prfx                --账户前缀
      ,cast(t1.upd_date as string)                                      as update_date              --更新日期
      ,t1.tel                                                           as tel_num                  --电话号码
      ,t1.area_addr                                                     as conta_addr               --联系地址
      ,'uas'                                                            as ssys_code                --系统来源代码
      ,concat(cast(t1.org_code as string),'|',t1.org_type)              as ssys_num                 --系统来源编码
from rtassets_ods.ods_uas_kbssuser_org t1
where part_ymd='${batch_date}' and org_type = '0'
;