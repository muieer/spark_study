package org.muieer

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

package object study {

  var spark: SparkSession= _
  var sc: SparkContext = _
  val userPath = "/Users/muieer"
  val outPath = s"$userPath/temp"

  def buildLocalSparkEnv(): Unit = {

    val sparkConfig = new SparkConf
    sparkConfig.set("spark.memory.fraction", "0.9")

    spark =  SparkSession.builder()
      .appName("study_" + System.currentTimeMillis())
      .master("local")
      .config(sparkConfig)
      .getOrCreate()
    sc = spark.sparkContext
  }

}
