set hive.exec.dynamic.partition.mode=strict;
set hive.support.quoted.identifiers=None;
insert overwrite table ods.ods_uas_kbssuser_org partition(part_ymd = '${batch_date}')
select 
	\`(part_ymd)?+.+\`
from stg.stg_uas_kbssuser_org
where part_ymd = '${batch_date}';