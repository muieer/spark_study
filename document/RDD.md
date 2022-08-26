[https://spark.apache.org/docs/3.2.2/rdd-programming-guide.html](https://spark.apache.org/docs/3.2.2/rdd-programming-guide.html)
## 概述
## 弹性分布式数据集 RDD
### 数据来源
1）从基于 Drive 的集合创建  
2）从外部文件系统读取
- `sc.textFile`读取文件可以指定分区数目，但是分区数不能少于文件块
- 支持多种格式
- 可以以对象的形式保存
