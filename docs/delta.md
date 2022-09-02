
# Hive Metastore

```
apt update
apt upgrade -y
apt install -y python3-pip default-jre

curl -OL https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
curl -OL https://dlcdn.apache.org/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz

schematool -initSchema -dbType derby
```

```
export HIVE_OPTS="-hiveconf mapred.job.tracker=local \
   -hiveconf fs.default.name=file://`pwd`/tmp \
   -hiveconf hive.metastore.warehouse.dir=file://`pwd`/tmp/warehouse \
   -hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=`pwd`/tmp/metastore_db;create=true
```

: https://funinit.wordpress.com/2020/12/08/how-to-start-apache-hive-without-hdfs/
