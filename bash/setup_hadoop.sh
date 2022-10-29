# connect node with remote HADOOP cluster
source ./hadoop_secrets.txt

echo "USER: ${hadoop_user}"
echo "URL: ${hadoop_url}"

ssh ${hadoop_user}@${hadoop_url} "cat /etc/apt/sources.list.d/yandex-dataproc.list" | sudo tee /etc/apt/sources.list.d/yandex-dataproc.list
deb [arch=amd64] http://storage.yandexcloud.net/dataproc/releases/0.2.10 xenial main

ssh ${hadoop_user}@${hadoop_url} \
  "cat /srv/dataproc.gpg" | sudo apt-key add -

sudo apt update
sudo apt install openjdk-8-jre-headless hadoop-client hadoop-hdfs spark-core

sudo -E scp -r \
  ${hadoop_user}@${hadoop_url}:/etc/hadoop/conf/* \
  /etc/hadoop/conf/ &&
  sudo -E scp -r \
    ${hadoop_user}@${hadoop_url}:/etc/spark/conf/* \
    /etc/spark/conf/

sudo useradd sparkuser &&
  ssh ${hadoop_user}@${hadoop_url} \
    "sudo -u hdfs hdfs dfs -ls /user/sparkuser" &&
  ssh ${hadoop_user}@${hadoop_url} \
    "sudo -u hdfs hdfs dfs -chown sparkuser:sparkuser /user/sparkuser"

echo "Testing on example"
sudo -u sparkuser spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class org.apache.spark.examples.SparkPi \
  /usr/lib/spark/examples/jars/spark-examples.jar 1000
