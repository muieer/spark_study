[https://spark.apache.org/docs/3.2.2/rdd-programming-guide.html](https://spark.apache.org/docs/3.2.2/rdd-programming-guide.html)
## 概述
## 弹性分布式数据集 RDD
### 数据来源
1）从基于 Drive 的集合创建  
2）从外部文件系统读取
- `sc.textFile`读取文件可以指定分区数目，但是分区数不能少于文件块
- 支持多种格式，本地、SequenceFile、
- 可以以对象的形式保存
### 算子
Spark 支持两种类型算子：转换和行动算子，只有行动算子调用时才会触发计算
#### 传递函数
1. 推荐匿名函数
2. 使用单例对象的静态方法，避免传递不必要的类
#### 理解闭包