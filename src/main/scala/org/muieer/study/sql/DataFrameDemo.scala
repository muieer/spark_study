package org.muieer.study.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.muieer.study
import org.muieer.study.{buildLocalSparkEnv, outPath, sc, spark, userPath}


object DataFrameDemo {

  def buildDataFrameFromRDD(sc: SparkContext, spark: SparkSession): Unit = {

    val rdd = sc.textFile(s"$userPath/temp/0812_1660295878442")
      .map(line => {
      val Array(key, value) = line.split("\t")
      Row(key, value)
    })

    val schema =
      StructType(
        StructField("imei", StringType, false) ::
          StructField("feature", StringType, true) :: Nil)

    val dataFrame = spark.createDataFrame(rdd, schema)
    dataFrame.show()

    dataFrame.repartition(1)
      .write.mode(SaveMode.Overwrite).parquet(s"$outPath/081401")

  }

  def buildDataFrameFromParquetFile(sc: SparkContext, spark: SparkSession): Unit = {

    val dataFrame = spark.read.parquet(s"$outPath/081401")
    dataFrame.show()
    dataFrame.printSchema()
    println(s"${dataFrame.count()}")

    println(
      s"""
         |${dataFrame.select("feature").distinct().count()}"""
        .stripMargin
    )

  }


  def main(args: Array[String]): Unit = {

    buildLocalSparkEnv()
    buildDataFrameFromRDD(sc, spark)
    buildDataFrameFromParquetFile(sc, spark)

  }
}
