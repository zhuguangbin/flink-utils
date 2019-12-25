package net.qihoo.ads.flink.env.scala

import java.io.{File, IOException}

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.{CoreOptions, GlobalConfiguration}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.slf4j.{Logger, LoggerFactory}

object FlinkStreamingEnv {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  def init(args: Array[String], jobPropertiesFileName: String): StreamExecutionEnvironment = {
    val classLoader = Thread.currentThread().getContextClassLoader()

    val jobConfFile = classLoader.getResourceAsStream("conf/" + jobPropertiesFileName)
    if (jobConfFile == null) {
      throw new IOException("Properties file conf/" + jobPropertiesFileName + " does not exist")
    } else {

      val jobDefault = ParameterTool.fromPropertiesFile(classLoader.getResourceAsStream("conf/flinkjob-default.properties"))
      val jobSpecific = ParameterTool.fromPropertiesFile(jobConfFile)
      val argsParam = ParameterTool.fromArgs(args)

      // merge all conf, the priority order is argsParam > jobSpecific > jobDefault
      val jobConf = jobDefault.mergeWith(jobSpecific).mergeWith(argsParam).getConfiguration
      val conf = GlobalConfiguration.loadConfiguration(jobConf)
      LOG.info("Global Configuration: " + conf.toString)

      val env = if (conf.getBoolean("flinkjob.local", false)) StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf) else StreamExecutionEnvironment.getExecutionEnvironment
      env.getConfig.setGlobalJobParameters(conf)

      env.setParallelism(conf.getInteger(CoreOptions.DEFAULT_PARALLELISM, 1))
      env.setStreamTimeCharacteristic(TimeCharacteristic.valueOf(conf.getString("flinkjob.timeCharacteristic", "EventTime")))

      if (conf.getBoolean("flinkjob.checkpoint.config.enabled", true)) {
        env.enableCheckpointing(conf.getLong("flinkjob.checkpoint.config.interval", 600000L), CheckpointingMode.valueOf(conf.getString("flinkjob.checkpoint.config.mode", "EXACTLY_ONCE")))
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(conf.getInteger("flinkjob.checkpoint.config.maxConcurrentCheckpoints", CheckpointConfig.DEFAULT_MAX_CONCURRENT_CHECKPOINTS))
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(conf.getLong("flinkjob.checkpoint.config.minPauseBetweenCheckpoints",CheckpointConfig.DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS))
        env.getCheckpointConfig.setCheckpointTimeout(conf.getLong("flinkjob.checkpoint.config.timeout", CheckpointConfig.DEFAULT_TIMEOUT))

        env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        env.getCheckpointConfig.setPreferCheckpointForRecovery(conf.getBoolean("flinkjob.checkpoint.config.preferCheckpointForRecovery", false))
        env.getCheckpointConfig.setTolerableCheckpointFailureNumber(conf.getInteger("flinkjob.checkpoint.config.tolerableCheckpointFailureNumber", 3))
      }

      env
    }

  }

}
