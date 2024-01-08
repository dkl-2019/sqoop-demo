sqoop采用shell脚本方式同步数据：
  上游数据：Oracle --> Hive(stg) --
            --> Hive(ods) --> Hive(dwd) --> Hive(dws) --> Hive(dws) --> Hive(ads)
