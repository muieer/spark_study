#!/bin/zsh

mainClass=org.muieer.study.sql.ProcessDataFrameByFunctionDemo
spark-submit --class $mainClass \
            /Users/yangdahu/code/spark_study/target/spark_study-1.0-SNAPSHOT-jar-with-dependencies.jar