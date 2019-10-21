package org.fortysevendeg.sparksftp

import cats.effect.{ExitCode, IO, IOApp}
import pureconfig.generic.auto._
import org.apache.spark.SparkConf
import org.fortysevendeg.sparksftp.common.SparkUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.fortysevendeg.sparksftp.common.HiveUserData._
import org.fortysevendeg.sparksftp.common.HiveUserData
import org.fortysevendeg.sparksftp.config.model.configs.{ReadingSFTPConfig, SFTPConfig}
import org.training.trainingbot.config.ConfigLoader
import org.fortysevendeg.sparksftp.config.model.configs

object ReadingSFTPConnectorApp extends IOApp {

  def setupConfig: IO[ReadingSFTPConfig] =
    ConfigLoader[IO]
      .loadConfig[ReadingSFTPConfig]

  def run(args: List[String]): IO[ExitCode] = {

    def createSparkSession(config: ReadingSFTPConfig): IO[SparkSession] = IO {
      val defaultSparkConf: SparkConf = createSparkConfWithSFTPSupport(config)
      SparkSession.builder
        .config(defaultSparkConf)
        .enableHiveSupport
        .getOrCreate()
    }

    def readDataFramesWithSFTPConnector(
        sparkSession: SparkSession,
        sftpConfig: SFTPConfig
    ): IO[(DataFrame, DataFrame)] = {
      val users =
        dataframeFromCsvWithSFTPConnector(sparkSession, sftpConfig, sftpConfig.sftpUserPath)
      val salaries = dataframeFromCsvWithSFTPConnector(
        sparkSession,
        sftpConfig,
        sftpConfig.sftpSalaryPath
      )
      IO((users, salaries))
    }

    def toSFTPCompressedCSVWithSFTPConnector(
        userData: DataFrame,
        salariesData: DataFrame,
        userNewSalaries: DataFrame,
        sftpConfig: SFTPConfig
    ): IO[Unit] = IO {
      dataframeToCompressedCsvWithSFTPConnector(
        userData,
        sftpConfig,
        s"${sftpConfig.sftpUserPath}_output"
      )
      dataframeToCompressedCsvWithSFTPConnector(
        salariesData,
        sftpConfig,
        s"${sftpConfig.sftpSalaryPath}_output"
      )
      dataframeToCompressedCsvWithSFTPConnector(
        userNewSalaries,
        sftpConfig,
        s"${sftpConfig.sftpSalaryPath}_transformed_output"
      )
    }

    for {
      config  <- setupConfig
      session <- createSparkSession(config)
      sftpConfig = configs.SFTPConfig
        .configFromContextProperties(session.sparkContext, config.sftp)
      (users, salaries) <- readDataFramesWithSFTPConnector(session, sftpConfig)
      HiveUserData(userData, salariesData, userSalaries) <- persistAndReadUserData(
        session,
        users,
        salaries
      )
      userNewSalaries <- calculateAndPersistNewSalary(session, userSalaries)
      _               <- toSFTPCompressedCSVWithSFTPConnector(userData, salariesData, userNewSalaries, sftpConfig)
    } yield ExitCode.Success
  }
}
