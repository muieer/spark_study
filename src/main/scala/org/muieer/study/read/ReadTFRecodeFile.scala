package org.muieer.study.read

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.muieer.study._
import org.tensorflow.hadoop.io.TFRecordFileInputFormat
import org.tensorflow.example.Example

object ReadTFRecodeFile {

  val path = ""
  val rdd =
    sc.newAPIHadoopFile[BytesWritable, NullWritable, TFRecordFileInputFormat](path)
      .map{
        case (bytesWritable: BytesWritable, nullWritable: NullWritable) => {
          val example = Example.parseFrom(bytesWritable.getBytes)
          example
        }
      }
      .map(example => example.getFeatures)
      .map(_.toString)

}
