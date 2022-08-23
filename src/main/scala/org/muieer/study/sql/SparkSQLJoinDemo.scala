package org.muieer.study.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SparkSQLJoinDemo {

  def buildRDD(sc: SparkContext, spark: SparkSession) = {

    // 员工信息
    val seq = Seq((1, "Mike", 28, "Male"), (2, "Lily", 30, "Female"), (3, "Raymond", 26, "Male"), (5, "Dave", 36, "Male"))
    val rdd = sc.makeRDD(seq)

    // 薪资信息
    val seq2 = Seq((1, 26000), (2, 30000), (4, 25000), (3, 20000))
    val rdd2 = sc.makeRDD(seq2)

    (rdd, rdd2)
  }

  def buildDataFrame(sc: SparkContext, spark: SparkSession) = {

    val (rdd, rdd1) = buildRDD(sc, spark)

    val rowRdd = rdd.map{
      case (id, name, age, gender) => Row(id, name, age, gender)
    }

    val schema = StructType(Array(StructField("id", IntegerType), StructField("name", StringType),
      StructField("age", IntegerType), StructField("gender", StringType)))

    val rowRdd1 = rdd1.map{
      case (id, salary) => Row(id, salary)
    }

    val schema1 = StructType(Array(StructField("id", IntegerType), StructField("salary", IntegerType)))

    (spark.createDataFrame(rowRdd, schema), spark.createDataFrame(rowRdd1, schema1))
  }

  def joinTypes(sc: SparkContext, spark: SparkSession) = {

    val (employees, salaries) = buildDataFrame(sc, spark)

    val bcEmployees = broadcast(employees)
    println(
      s"""
         |${employees.show()}
         |${salaries.show()}
         |内关联
         |${salaries.join(bcEmployees, salaries("id") === employees("id"), "inner").show}
         |""".stripMargin)
//    salaries.join(employees, salaries("id") === employees("id"), "inner").show

    println(
      s"""
         |全外关联
         |${salaries.join(employees, salaries("id") === employees("id"), "full").show}
         |""".stripMargin)

    println(
      s"""
         |左逆关联
         |${salaries.join(employees, salaries("id") === employees("id"), "leftanti").show}
         |""".stripMargin)
  }

}
