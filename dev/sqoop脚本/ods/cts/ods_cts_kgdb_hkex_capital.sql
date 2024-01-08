set hive.exec.dynamic.partition.mode=strict;
set hive.support.quoted.identifiers=None;
insert overwrite table ods.ods_cts_kgdb_hkex_capital partition(part_ymd = "${batch_date}")
select 
\`(part_ymd)?+.+\`
from stg.stg_cts_kgdb_hkex_capital
where part_ymd = "${batch_date}";