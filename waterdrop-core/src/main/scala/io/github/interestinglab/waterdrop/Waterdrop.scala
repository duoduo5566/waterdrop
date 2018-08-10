package io.github.interestinglab.waterdrop

import io.github.interestinglab.waterdrop.config.{CommandLineArgs, CommandLineUtils, Common, ConfigBuilder}
import io.github.interestinglab.waterdrop.launch.batch.BatchApp
import io.github.interestinglab.waterdrop.launch.streaming.StreamingApp
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging

import scala.util.{Failure, Success}

object Waterdrop extends Logging {

  def main(args: Array[String]) {

    CommandLineUtils.parser.parse(args, CommandLineArgs()) match {
      case Some(cmdArgs) => {
        Common.setDeployMode(cmdArgs.deployMode)

        val configFilePath = Common.getDeployMode match {
          case Some(m) => {
            if (m.equals("cluster")) {
              // only keep filename in cluster mode
              new Path(cmdArgs.configFile).getName
            } else {
              cmdArgs.configFile
            }
          }
        }

        cmdArgs.testConfig match {
          case true => {
            new ConfigBuilder(configFilePath).checkConfig
            println("config OK !")
          }
          case false => {

            entrypoint(configFilePath)
          }
        }
      }
      case None =>
      // CommandLineUtils.parser.showUsageAsError()
      // CommandLineUtils.parser.terminate(Right(()))
    }

  }

  private def entrypoint(configFile: String): Unit = {

    val configBuilder = new ConfigBuilder(configFile)
    val sparkConfig = configBuilder.getSparkConfigs

    val ss = sparkConfig.hasPath("spark.streaming.batchDuration")
    val app: launch.App = if (ss) StreamingApp(configBuilder) else BatchApp(configBuilder)

    app.init match {
      case Success(_) => {
        logInfo("process init success")
      }
      case Failure(ex) => {
        logError(s"process init error: ${ex.getMessage}")
        System.exit(-1)
      }
    }

    app.process match {
      case Success(_) => {
        logInfo("process run success")
      }
      case Failure(ex) => {
        logError(s"process run error: ${ex.getMessage}")

        if (app.retryAble) {
          throw ex
        } else {
          System.exit(-1)
        }
      }
    }
  }
}
