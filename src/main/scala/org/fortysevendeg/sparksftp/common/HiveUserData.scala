package org.fortysevendeg.sparksftp.common

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import SparkUtils._
import cats.effect.IO
import org.apache.spark.sql.types.IntegerType

/**
 * Notes regarding operating with Hive:
 * Creating databases do not work in Dataproc: https://github.com/mozafari/verdictdb/issues/163
 * https://stackoverflow.com/questions/30664008/how-to-save-dataframe-directly-to-hive
 *
 * Sample operations to perform on the user data
 */
case class HiveUserData(
    usersData: DataFrame,
    salariesData: DataFrame,
    userAndSalariesData: DataFrame
)

object HiveUserData {

  def persistUserData(sparkSession: SparkSession, users: DataFrame, salaries: DataFrame): Unit = {

    persistDataFrame(sparkSession, users.select("ID", "name", "age"), "user_data", List("age"))
    persistDataFrame(sparkSession, salaries.select("ID", "salary"), "salaries")

    val userWithSalaries = users.join(salaries, "ID").select("ID", "name", "age", "salary")
    persistDataFrame(sparkSession, userWithSalaries, "user_salary")

    // Show the list of tables in the spark console
    users.printSchema()
    salaries.printSchema()
    sparkSession.catalog.listTables().show(truncate = false)
    sparkSession.sql("show tables").show(truncate = false)
  }

  def readUserData(sparkSession: SparkSession): HiveUserData = {
    //Used to return the dataframe and show an excerpt in console
    val userDataFromHive = sparkSession.sql("select name from user_data")
    userDataFromHive.show(false)

    val salariesDataFromHive = sparkSession.sql("select ID,salary from salaries")
    salariesDataFromHive.show(false)

    //Excerpt from the joined table
    val userSalaries = sparkSession.sql("select name,salary from user_salary")
    userSalaries.show(false)

    HiveUserData(userDataFromHive, salariesDataFromHive, userSalaries)
  }

  def persistAndReadUserData(
      sparkSession: SparkSession,
      users: DataFrame,
      salaries: DataFrame
  ): IO[HiveUserData] = IO {
    HiveUserData.persistUserData(sparkSession, users, salaries)
    HiveUserData.readUserData(sparkSession)
  }

  def calculateAndPersistNewSalary(
      sparkSession: SparkSession,
      userSalaries: DataFrame
  ): IO[Dataset[Row]] = IO {
    val newSalaries =
      userSalaries.withColumn("new_salary", (userSalaries("salary") * 1.1).cast(IntegerType))
    SparkUtils.persistDataFrame(sparkSession, newSalaries, "user_new_salary")
    sparkSession.sql("select name,salary from user_new_salary")
  }

}
