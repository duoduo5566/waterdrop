package io.github.interestinglab.waterdrop.launch
import io.github.interestinglab.waterdrop.config.ConfigBuilder
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

trait App extends Logging with Serializable {

  def configCheck(configBuilder: ConfigBuilder): Unit = {
    val inputs = configBuilder.createInputs
    val outputs = configBuilder.createOutputs
    val filters = configBuilder.createFilters

    var configValid = true
    val plugins = inputs ::: filters ::: outputs
    for (p <- plugins) {
      val (isValid, msg) = Try(p.checkConfig) match {
        case Success(info) => {
          val (ret, message) = info
          (ret, message)
        }
        case Failure(exception) => (false, exception.getMessage)
      }

      if (!isValid) {
        configValid = false
        printf("Plugin[%s] contains invalid config, error: %s\n", p.name, msg)
      }
    }

    if (!configValid) {
      System.exit(-1) // invalid configuration
    }
  }

  def init: Try[_]

  def process: Try[_]

  /**
   * application will exit if it fails in run phase.
   * if retryable is true, the exception will be threw to spark env,
   * and enable retry strategy of spark application
   */
  def retryAble: Boolean

  def createSparkConf(configBuilder: ConfigBuilder): SparkConf = {
    val sparkConf = new SparkConf()

    configBuilder.getSparkConfigs
      .entrySet()
      .filter(_.getKey.startsWith("spark."))
      .foreach(entry => {
        sparkConf.set(entry.getKey, String.valueOf(entry.getValue.unwrapped()))
      })

    sparkConf
  }
}
