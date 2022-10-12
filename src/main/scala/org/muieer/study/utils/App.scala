package org.muieer.study.utils

import org.apache.spark.util.SizeEstimator

object App {

  def main(args: Array[String]): Unit = {

    val map = Map(1 -> 2, 3 -> 4)
    val num: Integer = 1



    println(
      s"""
         |获取对象内训占用
         |${SizeEstimator.estimate(map)}
         |${SizeEstimator.estimate(num)}
         |""".stripMargin)
  }
}
