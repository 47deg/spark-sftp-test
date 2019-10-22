package org.fortysevendeg.sparksftp

import pureconfig.generic.auto._
import cats.effect.{ExitCode, IO, IOApp}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.fortysevendeg.sparksftp.common.HiveUserData._
import org.fortysevendeg.sparksftp.common.HiveUserData
import org.fortysevendeg.sparksftp.common.SparkUtils._
import org.fortysevendeg.sparksftp.config.model.configs
import org.fortysevendeg.sparksftp.config.model.configs.{ReadingSFTPConfig, SFTPConfig}
import org.training.trainingbot.config.ConfigLoader

case class Salary(name: String, salary: Long)

object ReadingSFTPHadoopApp extends IOApp {

  def setupConfig: IO[ReadingSFTPConfig] =
    ConfigLoader[IO]
      .loadConfig[ReadingSFTPConfig]

  /**
   * When preparing the SparkConf, we could also use:
   * .set("spark.master", "local")
   * .set("spark.master", "local[*]")
   *  to run the application locally, for example, through "sbt run"
   */
  def run(args: List[String]): IO[ExitCode] = {
    def createSparkSession(config: ReadingSFTPConfig): IO[SparkSession] = IO {
      val defaultSparkConf: SparkConf = createSparkConfWithSFTPSupport(config)
        .set("fs.sftp.impl", "org.apache.hadoop.fs.sftpwithseek.SFTPFileSystem")
        .set("fs.sftp.impl.disable.cache", "true")
      getSparkSessionWithHive(defaultSparkConf)
    }

    def userPath(sftpConfig: SFTPConfig): String =
      s"sftp://${sftpConfig.sftpUser}:${sftpConfig.sftpPass}@${sftpConfig.sftpHost}" + s":${sftpConfig.sftpUserPath}"

    def salariesPath(sftpConfig: SFTPConfig): String =
      s"sftp://${sftpConfig.sftpUser}:${sftpConfig.sftpPass}@${sftpConfig.sftpHost}" + s":${sftpConfig.sftpSalaryPath}"

    def readDataFramesFromSFTP(
        sparkSession: SparkSession,
        sftpConfig: SFTPConfig
    ): IO[(DataFrame, DataFrame)] =
      for {
        users    <- dataframeFromCSV(sparkSession, userPath(sftpConfig))
        salaries <- dataframeFromCSV(sparkSession, salariesPath(sftpConfig))
      } yield (users, salaries)

    def toSFTPCompressedCSV(
        userData: DataFrame,
        salariesData: DataFrame,
        userNewSalaries: DataFrame,
        sftpConfig: SFTPConfig
    ): IO[Unit] =
      for {
        _ <- dataframeToCompressedCsv(userData, s"${userPath(sftpConfig)}_output")
        _ <- dataframeToCompressedCsv(salariesData, s"${salariesPath(sftpConfig)}_output")
        _ <- dataframeToCompressedCsv(
          userNewSalaries,
          s"${salariesPath(sftpConfig)}_transformed_output"
        )
      } yield ()

    for {
      config  <- setupConfig
      session <- createSparkSession(config)
      sftpConfig = configs.SFTPConfig
        .configFromContextProperties(session.sparkContext, config.sftp)
      (users, salaries) <- readDataFramesFromSFTP(session, sftpConfig)
      HiveUserData(userData, salariesData, userSalaries) <- persistAndReadUserData(
        session,
        users,
        salaries
      )
      userNewSalaries <- calculateAndPersistNewSalary(session, userSalaries)
      _               <- toSFTPCompressedCSV(userData, salariesData, userNewSalaries, sftpConfig)
    } yield ExitCode.Success
  }
}
