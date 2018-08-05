package io.github.interestinglab.waterdrop.input

import com.typesafe.config.Config
import io.github.interestinglab.waterdrop.apis.BaseInput
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class Hdfs(config: Config) extends BaseInput(config) {

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("path") match {
      case true => {

        val dir = config.getString("path")
        val path = new org.apache.hadoop.fs.Path(dir)
        Option(path.toUri.getScheme) match {
          case None => (true, "")
          case Some(schema) => (true, "")
          case _ =>
            (
              false,
              "unsupported schema, please set the following allowed schemas: hdfs://, for example: hdfs://<name-service>:<port>/var/log")
        }
      }
      case false => (false, "please specify [path] as non-empty string")
    }
  }

  override def getDStream(ssc: StreamingContext): DStream[(String, String)] = {

    ssc.textFileStream(config.getString("path")).map(s => { ("", s) })
  }

  /**
   * No matter what kind of Input it is, all you have to do is create a DStream to be used latter
   * */
  override def getRDD(sc: SparkContext): RDD[(String, String)] = {
    sc.textFile(config.getString("path")).map(s => { ("", s) })
  }
}
