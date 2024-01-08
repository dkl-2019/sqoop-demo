set hive.exec.dynamic.partition.mode=strict;
set hive.support.quoted.identifiers=None;
insert overwrite table ods.ods_cts_kgdb_fund_vol partition(part_ymd = "${batch_date}")
select 
\`(part_ymd)?+.+\`
from stg.stg_cts_kgdb_fund_vol
where part_ymd = "${batch_date}";