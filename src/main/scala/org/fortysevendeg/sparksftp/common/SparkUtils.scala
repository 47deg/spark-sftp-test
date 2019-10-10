package org.fortysevendeg.sparksftp.common

import java.net.URI

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.fortysevendeg.sparksftp.config.model.configs.{ReadingSFTPConfig, SFTPConfig}

object SparkUtils {

  def createSparkConfWithSFTPSupport(config: ReadingSFTPConfig): SparkConf = {
    new SparkConf()
      .set("spark.serializer", config.spark.serializer)
      .set("spark.master", "local")
      .set(
        "spark.kryo.registrationRequired",
        config.spark.serializer.contains("KryoSerializer").toString
      )
      .set("spark.hadoop.fs.sftp.impl", "org.apache.hadoop.fs.sftp.SFTPFileSystem")
      .set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true") //https://kb.azuredatabricks.net/jobs/spark-overwrite-cancel.html
      .registerKryoClasses(RegisterInKryo.classes.toArray)
  }

  def getSparkSessionWithHive(sparkconf: SparkConf): SparkSession = {
    val sparkSession = SparkSession.builder
      .config(sparkconf)
      .enableHiveSupport
      .getOrCreate()

    sparkSession
  }

  def dataframeFromCSV(sparkSession: SparkSession, sftpUri: String): DataFrame = {
    val inferSchema         = true
    val first_row_is_header = true
    val sourceUri           = new URI(sftpUri)

    sparkSession.read
      .option("header", first_row_is_header)
      .option("inferSchema", inferSchema)
      .csv(sourceUri.toString)
  }

  def persistDataFrame(sparkSession: SparkSession, df: DataFrame, name: String) = {
    // Persist the dataframes into Hive tables with parquet file format, the default compression for parquet is snappy, that is splittable for parquet.
    // If we wanted to debug any issue with the databases, we could use this: sparkSession.sparkContext.setLogLevel("DEBUG")
    sparkSession.sql(s"DROP TABLE IF EXISTS ${name}")
    df.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable(name)
  }

  def dataframeToCompressedCsv(df: DataFrame, path: String) = {
    df.write
      .mode(SaveMode.Overwrite)
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .csv(path)
  }

  /**
   * Construct a Spark DataFrame reading a file from SFTP using the `springml` connector
   */
  def dataframeFromCsvWithSFTPConnector(
      sparkSession: SparkSession,
      sftpConfig: SFTPConfig,
      path: String
  ): DataFrame = {
    sparkSession.read
      .format("com.springml.spark.sftp")
      .option("host", sftpConfig.sftpHost)
      .option("username", sftpConfig.sftpUser)
      .option("password", sftpConfig.sftpPass)
      .option("header", true)
      .option("fileType", "csv")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(path)
  }

  /**
   * Persist a Spark DataFrame in SFTP using the `springml` connector
   */
  def dataframeToCompressedCsvWithSFTPConnector(
      df: DataFrame,
      sftpConfig: SFTPConfig,
      path: String
  ) = {
    df.write
      .format("com.springml.spark.sftp")
      .option("host", sftpConfig.sftpHost)
      .option("username", sftpConfig.sftpUser)
      .option("password", sftpConfig.sftpPass)
      .option("header", true)
      .option("delimiter", ",")
      .option("fileType", "csv")
      .save(path)
  }

}
