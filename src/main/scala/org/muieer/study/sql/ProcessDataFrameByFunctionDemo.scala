package org.muieer.study.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.muieer.study.{buildLocalSparkEnv, outPath, sc, spark}

object ProcessDataFrameByFunctionDemo {

  def buildRDD(sc: SparkContext, spark: SparkSession) = {

    val seq = Seq((1, "Mike", 28, "Male"), (2, "Lily", 30, "Female"), (3, "Raymond", 26, "Male"))
    val rdd = sc.makeRDD(seq)

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

  def computeDataFrame(sc: SparkContext, spark: SparkSession) = {

    val (employees, salaries) = buildDataFrame(sc, spark)
    employees.show()
    salaries.show()

    val joinDF = salaries.join(employees, Seq("id"), "inner")
    joinDF.show()

    val df = joinDF.groupBy("gender").agg(sum("salary").as("sum_salary"),
                                            avg("salary").as("avg_salary"))
    df.show()
    df.orderBy(desc("sum_salary"), asc("gender")).show

    df
  }

  def saveDFWithCSV(sc: SparkContext, spark: SparkSession) = {

    computeDataFrame(sc, spark)
      .repartition(1)
      .write
      .format("csv")
      .mode(SaveMode.Append)
      .save(s"$outPath/0816")

  }

  def main(args: Array[String]): Unit = {

    buildLocalSparkEnv()

    computeDataFrame(sc, spark)
    saveDFWithCSV(sc, spark)
  }

}
