package org.muieer.study.utils

import com.google.common.hash.{BloomFilter, Funnels}
import java.lang.{Long => JLong}

object BloomFilterByGuavaAchieve {

  def main(args: Array[String]): Unit = {

    demo()

  }

  def demo(): Unit = {

    val filter: BloomFilter[JLong] = BloomFilter.create(Funnels.longFunnel(), 100, 0.01)
    filter.put(1L)
    println(filter.mightContain(1))

  }

}
