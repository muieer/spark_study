package org.muieer.study.rdd

import org.apache.spark.sql.SparkSession

object InputDemo {

  var path: String = _

  def callTextFileMethod(spark: SparkSession) = {

    val sc = spark.sparkContext
    sc.textFile(path)

  }

  def callWholeTextFiles(spark: SparkSession) = {

    val sc = spark.sparkContext
    sc.wholeTextFiles(s"$path/*", 100)
  }
}
