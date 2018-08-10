package io.github.interestinglab.waterdrop.launch.streaming
import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseInput, BaseOutput}
import io.github.interestinglab.waterdrop.config.ConfigBuilder
import io.github.interestinglab.waterdrop.filter.UdfRegister
import io.github.interestinglab.waterdrop.launch.App
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Try

case class StreamingApp(configBuilder: ConfigBuilder) extends App {

  var inputs: List[BaseInput] = _
  var filters: List[BaseFilter] = _
  var outputs: List[BaseOutput] = _

  var sparkSession: SparkSession = _
  var ssc: StreamingContext = _

  override def init: Try[_] = Try {
    val sparkConf = createSparkConf(configBuilder)
    val duration = configBuilder.getSparkConfigs.getLong("spark.streaming.batchDuration")

    inputs = configBuilder.createInputs
    outputs = configBuilder.createOutputs
    filters = configBuilder.createFilters

    ssc = new StreamingContext(sparkConf, Seconds(duration))
    sparkSession = SparkSession.builder.config(ssc.sparkContext.getConf).getOrCreate()

    UdfRegister.findAndRegisterUdfs(sparkSession)
  }

  override def process: Try[_] = Try {

    for (i <- inputs) {
      i.prepare(sparkSession, ssc)
    }

    for (o <- outputs) {
      o.prepare(sparkSession, ssc)
    }

    for (f <- filters) {
      f.prepare(sparkSession, ssc)
    }

    val dstreamList = inputs.map(p => {
      p.getDStream(ssc)
    })

    val unionedDStream = dstreamList.reduce((d1, d2) => {
      d1.union(d2)
    })

    val dStream = unionedDStream.mapPartitions { partitions =>
      val strIterator = partitions.map(r => r._2)
      val strList = strIterator.toList
      strList.iterator
    }

    dStream.foreachRDD { strRDD =>
      val rowsRDD = strRDD.mapPartitions { partitions =>
        val row = partitions.map(Row(_))
        val rows = row.toList
        rows.iterator
      }

      val spark = SparkSession.builder.config(rowsRDD.sparkContext.getConf).getOrCreate()

      val schema = StructType(Array(StructField("raw_message", StringType)))
      var df = spark.createDataFrame(rowsRDD, schema)

      for (f <- filters) {
        df = f.process(spark, df)
      }

      inputs.foreach(p => {
        p.beforeOutput
      })

      outputs.foreach(p => {
        p.process(df)
      })

      inputs.foreach(p => {
        p.afterOutput
      })

    }

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * application will exit if it fails in run phase.
   * if retryable is true, the exception will be threw to spark env,
   * and enable retry strategy of spark application
   */
  override def retryAble: Boolean = false
}
