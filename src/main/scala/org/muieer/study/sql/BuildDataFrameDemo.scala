package org.muieer.study.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.muieer.study.{buildLocalSparkEnv, outPath, sc, sparkSession, userPath}


object BuildDataFrameDemo {

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

  def buildDataFrameFromCSVFile(sc: SparkContext, spark: SparkSession): Unit = {

    val schema = StructType( Array(StructField("name", StringType),StructField("age", IntegerType)))
    val dataFrame = spark.read.format("csv")
      .schema(schema)
      .option("header", true) // 首行为列属性
      .option("mode", "dropMalformed") // 丢弃不正确的数据
      .load(s"$userPath/temp/test.csv")

    dataFrame.show()
    println(s"${dataFrame.count()}")
    println(s"${dataFrame.select("name").collect().toList.toString()}")

  }



  def main(args: Array[String]): Unit = {

    buildLocalSparkEnv()
    buildDataFrameFromRDD(sc, sparkSession)
    buildDataFrameFromParquetFile(sc, sparkSession)
    buildDataFrameFromCSVFile(sc, sparkSession)

  }

}
