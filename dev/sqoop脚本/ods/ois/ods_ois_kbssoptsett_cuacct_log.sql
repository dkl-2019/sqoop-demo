set hive.exec.dynamic.partition.mode=strict;
set hive.support.quoted.identifiers=None;
insert overwrite table ods.ods_ois_kbssoptsett_cuacct_log partition(part_ymd = '${batch_date}')
select 
	\`(part_ymd)?+.+\`
from stg.stg_ois_kbssoptsett_cuacct_log
where part_ymd = '${batch_date}';