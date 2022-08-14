package org.muieer

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

package object study {

  var spark: SparkSession= _
  var sc: SparkContext = _
  val userPath = "/Users/muieer"
  val outPath = s"$userPath/temp"

  def buildLocalSparkEnv(): Unit = {
    spark =  SparkSession.builder()
      .appName("study_" + System.currentTimeMillis())
      .master("local")
      .getOrCreate()
    sc = spark.sparkContext
  }

}
