package org.muieer.study.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.muieer.study.{buildLocalSparkEnv, sc, spark}
//import

object ProcessDataFrameBySQLDemo {

  def demo(sc: SparkContext, spark: SparkSession): Unit = {

    val seq = Seq(("Alice", 18), ("Bob", 14))
    val schema = StructType(Array(StructField("name", StringType), StructField("age", IntegerType)))

    val dataFrame = spark.createDataFrame(sc.makeRDD(seq).map(line => Row(line._1, line._2)), schema)
    // 创建表
    dataFrame.createTempView("temp1")

    val query: String = "select * from temp1"
    // spark为SparkSession实例对象
    val result: DataFrame = spark.sql(query)

    result.show()

  }

  def main(args: Array[String]): Unit = {

    buildLocalSparkEnv()
    demo(sc, spark)
  }

}
