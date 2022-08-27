package org.muieer.study.MLlib

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.muieer.study.spark

object HomePricePrediction {

  // 数据读取与探索
  var rootPath: String = "/Users/muieer/house-prices-advanced-regression-techniques"
  var filePath: String = s"${rootPath}/train.csv"

  val trainDF = spark.read.format("csv").option("header", true).load(filePath)
  trainDF.show()
  trainDF.printSchema

  //数据提取
  val selectedFields  = trainDF.select("LotArea", "GrLivArea", "TotalBsmtSF", "GarageArea", "SalePrice")

  val typedFields = selectedFields
    .withColumn("LotAreaInt",col("LotArea").cast(IntegerType)).drop("LotArea")
    .withColumn("GrLivAreaInt",col("GrLivArea").cast(IntegerType)).drop("GrLivArea")
    .withColumn("TotalBsmtSFInt",col("TotalBsmtSF").cast(IntegerType)).drop("TotalBsmtSF")
    .withColumn("GarageAreaInt",col("GarageArea").cast(IntegerType)).drop("GarageArea")
    .withColumn("SalePriceInt",col("SalePrice").cast(IntegerType)).drop("SalePrice")

  typedFields.printSchema()

  //准备训练样本

  //待捏合的特征字段集合
  val features: Array[String] = Array("LotAreaInt", "GrLivAreaInt", "TotalBsmtSFInt", "GarageAreaInt")

  //准备"捏合器"，指定输入特征字段集合，与捏合后的特征向量字段名
  val assembler = new VectorAssembler().setInputCols(features).setOutputCol("features")

  //调用捏合器的transform函数，完成特征向量的捏合
  val featuresAdded = assembler.transform(typedFields)
    .drop("LotAreaInt")
    .drop("GrLivAreaInt")
    .drop("TotalBsmtSFInt")
    .drop("GarageAreaInt")

  // 此为样本
  featuresAdded.printSchema()

  // 训练集和测试集
  val Array(trainSet, testSet) = featuresAdded.randomSplit(Array(0.7, 0.3))

  // 模型训练
  // 构建线性回归模型，指定特征向量、预测标的与迭代次数
  val lr = new LinearRegression()
    .setLabelCol("SalePriceInt")
    .setFeaturesCol("features")
    .setMaxIter(10)


  // 使用训练集trainSet训练线性回归模型
  val lrModel = lr.fit(trainSet)

  // 模型效果评估
  val trainingSummary = lrModel.summary
  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")


}
