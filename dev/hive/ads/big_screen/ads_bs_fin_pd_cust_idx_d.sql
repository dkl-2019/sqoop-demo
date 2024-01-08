------------------- 1.客户年龄分布 -------------------
set mapreduce.job.queuename = root.users.rtassets;
insert overwrite table rtassets_ads.ads_bs_fin_pd_cust_idx_d partition (part_ymd = '${batch_date}')
select '${batch_date}'                                        as DATA_DATE
     , t4.INNERORG_CODE
     , t4.INNERORG_NAME
     , t4.AGE_RANGE
     , count(1)                                               as VALID_ACCT_CNT
     , from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') as DATA_TIME
from (
         select t3.innerorg_num  as INNERORG_CODE
              , t3.innerorg_name as INNERORG_NAME
              , case
                    when age <= 35 then '青年'
                    when age > 35 and age <= 50 then '中年'
                    when age > 50 and age <= 65 then '中老年'
                    when age > 65 and age <= 75 then '老年'
                    when age > 75 then '长寿'
             end                 as AGE_RANGE
              -- 按照天算年龄
         from (
                  select t1.part_ymd,
                         t1.innerorg_num,
                         t1.innerorg_name,
                         floor(months_between(
                                       from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),
                                       from_unixtime(unix_timestamp(t1.birthday, 'yyyyMMdd'), 'yyyy-MM-dd')) /
                               12) as age
                  from (select *
                        from rtassets_dw.dwd_ide_cust
                        where part_ymd = '${batch_date}'
                          and ssys_tab = 'ods_uas_kbssuser_customer'
                          and birthday is not null
                          and birthday != '0') t1
                           join
                       (
                           select cust_code,
                                  count(*) ct
                           from rtassets_ods.ods_cts_kgdb_shares
                           where part_ymd = '${batch_date}'
                             and share_bln > 0
                           group by cust_code -- 表中有重复的需要去重
                       ) t2 on t1.cust_num = cast(t2.cust_code as string)
              ) t3
     ) t4
group by INNERORG_CODE, INNERORG_NAME, AGE_RANGE;