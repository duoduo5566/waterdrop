package io.github.interestinglab.waterdrop.input

import com.typesafe.config.Config
import io.github.interestinglab.waterdrop.apis.BaseInput
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class S3(config: Config) extends BaseInput(config) {

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("path") match {
      case true => {
        val allowedURISchema = List("s3://", "s3a://", "s3n://")
        val dir = config.getString("path")
        val unSupportedSchema = allowedURISchema.forall(schema => {
          // there are 3 "/" in dir, first 2 are from URI Schema, and the last is from path
          !dir.startsWith(schema + "/")
        })

        unSupportedSchema match {
          case true =>
            (false, "unsupported schema, please set the following allowed schemas: " + allowedURISchema.mkString(", "))
          case false => (true, "")
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
