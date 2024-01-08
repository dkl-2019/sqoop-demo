#!/bin/bash
#######################################
#  功能：同步hdfs数据到clickhouse
#  usage：sh  shell_name  table_name
#######################################
# 获取Hive表在HDFS上的存储路径
export HADOOP_HOME=/opt/cloudera/parcels/CDH/lib/hadoop
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
ck_db=rltm_bs_sts
table_name=ads_bs_fin_pd_val_d
if [ ! -d "/home/rtassets/hive_to_ck" ]; then
  mkdir -p /home/rtassets/hive_to_ck
fi

if [ ! -d "/home/rtassets/hive_to_ck/${table_name}" ]; then
  mkdir -p /home/rtassets/hive_to_ck/${table_name}
fi
current_dir=$(cd /home/rtassets/hive_to_ck/${table_name}; pwd)
input_file=`hadoop fs -ls /user/hive/warehouse/rtassets_ads.db/${table_name}/part_ymd=${batch_date}/* |awk '{print $8}' > ${current_dir}/${table_name}_${batch_date}`

echo "input_file is: ${input_file}"

# 定义Clickhouse 数据源连接
ch="clickhouse-client -h 172.18.8.198 --port 9004 -u bigscreen --password BigScreen@dsg_2023 -m --query "

# 将Hive存储在Hdfs上的数据同步到clickhouse
for i in `cat ${current_dir}/${table_name}_${batch_date}`
do
    echo `date +"%Y-%m-%d %H:%m:%S"`  '开始执行 .....' $i
    hadoop fs -text $i|${ch} "insert into ${ck_db}.${table_name}_all format CSV"
    echo `date +"%Y-%m-%d %H:%m:%S"`  '结束执行 .....' $i
done