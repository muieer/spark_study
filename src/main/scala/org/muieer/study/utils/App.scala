package org.muieer.study.utils

import it.unimi.dsi.fastutil.objects.ObjectArrayList
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.muieer.study.{sc, sparkSession}

import java.util

object App {

  def main(args: Array[String]): Unit = {

    val map = Map(1 -> 2, 3 -> 4)
    val num: Integer = 1
    val list = new util.ArrayList[String]()
    val list1 = new ObjectArrayList()

    // 获取可用内存大小
    println(s"${Runtime.getRuntime.maxMemory() / 1024 / 1024 /1024}GB")

    Runtime.getRuntime.totalMemory()
    val rdd = sc.textFile("/Users/muieer/a.mp4")
      .map(line =>{
        line
      })

    rdd.cache()
    rdd.count()

    val df = sparkSession.read.text("/Users/yangdahu/mvn100802.txt")
    df.printSchema()
    df.show()

    val sp = sparkSession
    import sp.implicits._

    // Spark SQL 支持限定类型的数据
    df.select("value")
      .map(row =>
//        row.getString(0)
        (new Object(), 1) // todo 报错
      )
      .coalesce(1)
      .write
      .text("/Users/muieer/1014")

    println(
      s"""
         |获取对象内训占用
         |${SizeEstimator.estimate(map)}
         |${SizeEstimator.estimate(num)}
         |${SizeEstimator.estimate(list)}
         |${SizeEstimator.estimate(list1)}
         |""".stripMargin)


  }
}
