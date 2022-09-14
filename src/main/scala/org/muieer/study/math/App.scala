package org.muieer.study.math

import java.lang.{Long => JLong}

object App {

  def main(args: Array[String]): Unit = {


    println(
      s"""
         |JLong.bitCount(16)=${JLong.bitCount(16)} //计算该值二进制1的数量
         |
         |""".stripMargin)
  }

}
