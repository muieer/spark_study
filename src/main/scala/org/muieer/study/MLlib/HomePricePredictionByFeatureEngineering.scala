package org.muieer.study.MLlib

import org.apache.spark.sql.functions.{col, min}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.ml.feature.{ChiSqSelector, MinMaxScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.muieer.study.{sparkSession => spark}

import scala.collection.mutable.ArrayBuffer

object HomePricePredictionByFeatureEngineering {

  var rootPath: String = "/Users/yangdahu/code/spark_study/data/house-prices-advanced-regression-techniques"
  var filePath: String = s"${rootPath}/train.csv"

  val sourceDataDF = spark.read.format("csv").option("header", true).load(filePath)

  // 第一步，预处理，非数值字段转换为数值
  // 所有非数值型字段，也即StringIndexer所需的“输入列”
  val categoricalFields: Array[String] = Array("MSSubClass", "MSZoning", "Street", "Alley", "LotShape", "LandContour", "Utilities", "LotConfig", "LandSlope", "Neighborhood", "Condition1", "Condition2", "BldgType", "HouseStyle", "OverallQual", "OverallCond", "YearBuilt", "YearRemodAdd", "RoofStyle", "RoofMatl", "Exterior1st", "Exterior2nd", "MasVnrType", "ExterQual", "ExterCond", "Foundation", "BsmtQual", "BsmtCond", "BsmtExposure", "BsmtFinType1", "BsmtFinType2", "Heating", "HeatingQC", "CentralAir", "Electrical", "KitchenQual", "Functional", "FireplaceQu", "GarageType", "GarageYrBlt", "GarageFinish", "GarageQual", "GarageCond", "PavedDrive", "PoolQC", "Fence", "MiscFeature", "MiscVal", "MoSold", "YrSold", "SaleType", "SaleCondition")

  // 非数值字段对应的目标索引字段，也即StringIndexer所需的“输出列”
  val indexFields: Array[String] = categoricalFields.map(_ + "Index").toArray

  // 将engineeringDF定义为var变量，后续所有的特征工程都作用在这个DataFrame之上
  var engineeringDF: DataFrame = sourceDataDF

  // 核心代码：循环遍历所有非数值字段，依次定义StringIndexer，完成字符串到数值索引的转换
  for ((field, indexField) <- categoricalFields.zip(indexFields)) {

    // 定义StringIndexer，指定输入列名、输出列名
    val indexer = new StringIndexer().setInputCol(field).setOutputCol(indexField)

    // 使用StringIndexer对原始数据做转换
    engineeringDF = indexer.fit(engineeringDF).transform(engineeringDF)

    // 删除掉原始的非数值字段列
    engineeringDF = engineeringDF.drop(field)

  }

  engineeringDF.select("GarageType", "GarageTypeIndex").show(5)

  // 第二步 特征选择
  // Spark MLLib 会封装一些统计方法

  // 所有数值型字段
  val numericFields: Array[String] = Array("LotFrontage", "LotArea", "MasVnrArea", "BsmtFinSF1", "BsmtFinSF2", "BsmtUnfSF", "TotalBsmtSF", "1stFlrSF", "2ndFlrSF", "LowQualFinSF", "GrLivArea", "BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath", "BedroomAbvGr", "KitchenAbvGr", "TotRmsAbvGrd", "Fireplaces", "GarageCars", "GarageArea", "WoodDeckSF", "OpenPorchSF", "EnclosedPorch", "3SsnPorch", "ScreenPorch", "PoolArea")
  // 预测标的字段
  val labelFields: Array[String] = Array("SalePrice")

  for (field <- (numericFields ++ labelFields)) {
    engineeringDF.withColumn(s"${field}Int", col(field).cast(IntegerType)).drop(field)
  }

  // 所有类型为Int的数值型字段
  val numericFeatures: Array[String] = numericFields.map(_ + "Int").toArray

  // 定义并初始化 VectorAssembler，用来创建特征向量
  val assembler = new VectorAssembler()
    .setInputCols(numericFeatures)
    .setOutputCol("features")

  engineeringDF = assembler.transform(engineeringDF)

  // 创建 ChiSqSelector，封装用来做特征选择的统计方法，如卡方检验与卡方分布
  val selector = new ChiSqSelector()
    .setFeaturesCol("features")
    .setLabelCol("SalePriceInt")

  // 调用fit函数，在DataFrame之上完成卡方检验
  val chiSquareModel = selector.fit(engineeringDF)

  // 获取ChiSqSelector选取出来的入选特征集合（索引）
  val indexs: Array[Int] = chiSquareModel.selectedFeatures

  val selectedFeatures: ArrayBuffer[String] = ArrayBuffer[String]()

  // 特征选择后留下的特征
  for (index <- indexs) {
    selectedFeatures += numericFields(index)
  }

  // 归一化，目的是将特征数据都被约束到同一个值域，训练效率会大幅提升，此例是[0,1]
  for (field <- numericFeatures) {
     // 定义并初始化 VectorAssembler，用来创建特征向量
    val assembler = new VectorAssembler()
      .setInputCols(Array(field))
      .setOutputCol(s"${field}Vector")

    // 调用transform把每个字段由Int转换为Vector类型
    engineeringDF = assembler.transform(engineeringDF)
  }

  // 开始归一化

  // 锁定所有 Vector 数据量
  private val vectorFields: Array[String] = numericFeatures.map(_ + "Vector").toArray
  // 归一化后的数据列
  val scaledFields = vectorFields.map(_ + "Scaled").toArray

  for(vector <- vectorFields) {
    //
    val minMaxScaler = new MinMaxScaler()
      .setInputCol(vector)
      .setOutputCol(s"${vector}Scaled")
    // 使用 MinMaxScaler，完成Vector数据列的归一化
    engineeringDF = minMaxScaler.fit(engineeringDF).transform(engineeringDF)
  }



}
