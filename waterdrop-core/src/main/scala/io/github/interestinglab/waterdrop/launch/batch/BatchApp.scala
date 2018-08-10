package io.github.interestinglab.waterdrop.launch.batch
import io.github.interestinglab.waterdrop.apis.{BaseFilter, BaseInput, BaseOutput}
import io.github.interestinglab.waterdrop.config.ConfigBuilder
import io.github.interestinglab.waterdrop.filter.UdfRegister
import io.github.interestinglab.waterdrop.launch.App
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Try

case class BatchApp(configBuilder: ConfigBuilder) extends App {

  var sparkSession: SparkSession = _

  var inputs: List[BaseInput] = _
  var filters: List[BaseFilter] = _
  var outputs: List[BaseOutput] = _

  override def init: Try[_] = Try {
    val sparkConf = createSparkConf(configBuilder)
    sparkSession = SparkSession.builder.config(sparkConf).getOrCreate()

    inputs = configBuilder.createInputs
    outputs = configBuilder.createOutputs
    filters = configBuilder.createFilters

    UdfRegister.findAndRegisterUdfs(sparkSession)
  }

  override def process: Try[_] = Try {

    for (i <- inputs) {
      i.prepare(sparkSession)
    }

    for (o <- outputs) {
      o.prepare(sparkSession)
    }

    for (f <- filters) {
      f.prepare(sparkSession)
    }

    val rddList = inputs.map(p => {
      p.getRDD(sparkSession.sparkContext)
    })

    val unionRdd = rddList.reduce((d1, d2) => {
      d1.union(d2)
    })

    val rdd = unionRdd.mapPartitions { partitions => partitions.map(r => r._2)
    }

    val rowsRDD = rdd.mapPartitions { partitions =>
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

    sparkSession.close()
    sparkSession.stop()
  }

  /**
   * application will exit if it fails in run phase.
   * if retryable is true, the exception will be threw to spark env,
   * and enable retry strategy of spark application
   */
  override def retryAble: Boolean = false
}
