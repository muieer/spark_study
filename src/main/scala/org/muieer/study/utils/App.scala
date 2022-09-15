package org.muieer.study.utils

object App {

  def main(args: Array[String]): Unit = {

    println(
      s"""
         |${BloomFilterUseDemo.demoBySparkImpl()}
         |""".stripMargin)
  }
}
