## sparksftpTest is a project to showcase an ETL pipeline: SFTP - Hive - SFTP with Spark

The process can be summarized in

-   Ingestion of the Data (CSV) from an SFTP server.
-   Processed by Spark and stored in Parquet/Hive, original data + the ones with tranformations
-   Storing the results on the SFTP server.

There are currently two implementations:

- Spark with a SFTP connector
- Spark with Hadoop's SFTP FileSystem Support

### Spark with a SFTP connector

This is one of the prototypes developed that works and is based on the springml connector https://github.com/springml/spark-sftp

This solution is currently the fastest
[ReadingSFTPConnectorApp](/src/main/scala/org/fortysevendeg/sparksftp/ReadingSFTPConnectorApp.scala)

### Spark with Hadoop's SFTP FileSystem Support

[ReadingSFTPHadoopApp](/src/main/scala/org/fortysevendeg/sparksftp/ReadingSFTPHadoopApp.scala)


With this solution we used the native support by Hadoop, although the implementation in the current releases contain
some bugs, the patches that exists have not been merged into the Hadoop released libraries, so we have included
the patched version in our code.


Issues about native support of SFTP in Hadoop:
https://issues.apache.org/jira/browse/HADOOP-5732
https://issues.apache.org/jira/browse/HADOOP-14444

### How to run the programs

#### Local:

To run them locally with "sbt run" you would need to add a setting to the SparkConf `.set("spark.master", "local[*]"`

#### On your local Spark instance through spark-submit

``sbt assembly`` to build the .jar

```
PATH-TO-SPARK/bin/spark-submit  --class org.fortysevendeg.sparksftp.ReadingSFTPHadoopApp --driver-class-path (/PATH-TO-HADOOP/bin/hadoop classpath) ./target/scala-2.11/sparksftpTest-assembly-0.0.1.jar --conf 'spark.executor.extraJavaOptions=-Dspark.executorEnv.SFTP_USER=XXXX' --files sftp.conf --driver-java-options ="-Dspark.executorEnv.SFTP_USER=XXX"
```

### On your Google Cloud Dataproc instance for Spark.

You can use the scripts `gcloudsubmit1.sh` and `gcloudsubmit2.sh` to see examples for submitting the spark jobs to a Google Cloud Dataproc cluster.
