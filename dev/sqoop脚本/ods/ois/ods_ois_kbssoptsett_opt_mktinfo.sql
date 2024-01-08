set hive.exec.dynamic.partition.mode=strict;
set hive.support.quoted.identifiers=None;
insert overwrite table ods.ods_ois_kbssoptsett_opt_mktinfo partition(part_ymd = '${batch_date}')
select 
	\`(part_ymd)?+.+\`
from stg.stg_ois_kbssoptsett_opt_mktinfo
where part_ymd = '${batch_date}';