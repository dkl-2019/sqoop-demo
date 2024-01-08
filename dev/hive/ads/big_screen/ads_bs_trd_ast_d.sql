-- TODO 5. 资产相关 营业部机构维度 (前提:资金表,持仓表,两融表)
set mapreduce.job.queuename = root.users.rtassets;
with cptl as (
    select SSYS_CODE,
           INNERORG_CODE,
           INNERORG_NAME,
           sum(YES_ACCT_BAL + YES_INTRNS_CPTL) as tol_cptl,
           sum(YES_ACCT_BAL) * 0.000037534246  as tol_inst
    from rtassets_ads.ads_bs_trd_cptl_d
    where part_ymd = '${batch_date}'
    group by SSYS_CODE, INNERORG_CODE, INNERORG_NAME
),
     share as (
         select SSYS_CODE,
                INNERORG_CODE,
                INNERORG_NAME,
                sum(YES_PD_SHARE * YES_PD_PRC) / 10000 as pd_mval
         from rtassets_ads.ads_bs_trd_hldp_d
         where part_ymd = '${batch_date}'
         group by SSYS_CODE, INNERORG_CODE, INNERORG_NAME
     ),
     mts as (
         select 'mts'      as SSYS_CODE,
                INNERORG_CODE,
                YES_FI_BLN as FIN_AST,
                YES_SL_BLN as SEC_AST
         from rtassets_ads.ads_bs_trd_mts_d
         where part_ymd = '${batch_date}'
     )
insert overwrite table rtassets_ads.ads_bs_trd_ast_d partition (part_ymd = '${batch_date}')
select '${batch_date}'                                                    as DATA_DATE,
       INNERORG_CODE,
       INNERORG_NAME,
       cast(sum(if(SSYS_CODE = 'cts', INTR_INCOME, 0)) as decimal(19, 6)) as INTR_INCOME,
       cast(sum(if(SSYS_CODE = 'cts', TOT_AST, 0)) as decimal(19, 6))     as PT_TOT_AST,
       cast(sum(if(SSYS_CODE = 'mts', TOT_AST, 0)) as decimal(19, 6))     as LR_TOT_AST,
       cast(sum(if(SSYS_CODE = 'mts', FIN_AST, 0)) as decimal(19, 6))     as FIN_AST,
       cast(sum(if(SSYS_CODE = 'mts', SEC_AST, 0)) as decimal(19, 6))     as SEC_AST,
       from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss')             as DATA_TIME
from (
         select cptl.SSYS_CODE                                          as SSYS_CODE,
                cptl.INNERORG_CODE                                      as INNERORG_CODE,
                cptl.INNERORG_NAME                                      as INNERORG_NAME,
                cptl.tol_inst                                           as INTR_INCOME,
                cast((cptl.tol_cptl + share.pd_mval) as decimal(19, 6)) as TOT_AST,
                mts.FIN_AST                                             as FIN_AST,
                mts.SEC_AST                                             as SEC_AST
         from cptl
                  left join share
                            on cptl.SSYS_CODE = share.SSYS_CODE
                                and cptl.INNERORG_CODE = share.INNERORG_CODE
                  left join mts
                            on cptl.SSYS_CODE = mts.SSYS_CODE
                                and cptl.INNERORG_CODE = mts.INNERORG_CODE
     ) t1
group by INNERORG_CODE, INNERORG_NAME;