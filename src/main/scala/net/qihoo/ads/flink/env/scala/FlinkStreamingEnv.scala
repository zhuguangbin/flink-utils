package net.qihoo.ads.flink.env.scala

import java.io.IOException
import java.util.Properties

import com.mediav.data.log.unitedlog.UnitedEvent
import net.qihoo.ads.flink.formats.parquet.avro.LZOCompressedParquetAvroWriters
import net.qihoo.ads.flink.functions.timestamps.UnitedEventTimestampsAndWatermarks
import net.qihoo.ads.flink.kafka.serde.avro.ConfluentRegistryAvroKafkaSerializationSchema
import net.qihoo.ads.flink.kafka.serde.thrift.ThriftTBaseKafkaDeserializationSchema
import net.qihoo.ads.flink.sink.filesystem.bucketassigners.HiveDateHourPartitionBucketAssigner
import net.qihoo.ads.kafka.KafkaConfigClient
import org.apache.avro.specific.{SpecificRecord, SpecificRecordBase}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.{CoreOptions, GlobalConfiguration}
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.thrift.protocol.TBinaryProtocol
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.reflect.{ClassTag, _}

object FlinkStreamingEnv {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val schemaRegistryUrl = KafkaConfigClient.KAFKA_SCHEMA_REGISTRY_URL_ADDRESS

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
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(conf.getLong("flinkjob.checkpoint.config.minPauseBetweenCheckpoints", CheckpointConfig.DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS))
        env.getCheckpointConfig.setCheckpointTimeout(conf.getLong("flinkjob.checkpoint.config.timeout", CheckpointConfig.DEFAULT_TIMEOUT))

        env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
        env.getCheckpointConfig.setPreferCheckpointForRecovery(conf.getBoolean("flinkjob.checkpoint.config.preferCheckpointForRecovery", false))
        env.getCheckpointConfig.setTolerableCheckpointFailureNumber(conf.getInteger("flinkjob.checkpoint.config.tolerableCheckpointFailureNumber", 3))
      }

      env
    }

  }

  def kafkaThriftSource[T <: org.apache.thrift.TBase[_ <: org.apache.thrift.TBase[_ <: AnyRef, _ <: org.apache.thrift.TFieldIdEnum], _ <: org.apache.thrift.TFieldIdEnum] : ClassTag](topics: java.util.List[String], consumerConfig: Properties)(implicit evidence: TypeInformation[T], env: StreamExecutionEnvironment) = {
    env.addSource(new FlinkKafkaConsumer[T](topics, new ThriftTBaseKafkaDeserializationSchema[T](classTag[T].runtimeClass.asInstanceOf[Class[T]], new TBinaryProtocol.Factory()), consumerConfig))
      .name("KafkaThriftSource[%s]".format(topics.mkString(",")))
  }

  def kafkaAvroSource[T <: SpecificRecord : ClassTag](topics: java.util.List[String], consumerConfig: Properties)(implicit evidence: TypeInformation[T], env: StreamExecutionEnvironment, schemaRegistryUrl: String) = {
    env.addSource(new FlinkKafkaConsumer[T](topics, ConfluentRegistryAvroDeserializationSchema.forSpecific(classTag[T].runtimeClass.asInstanceOf[Class[T]], schemaRegistryUrl), consumerConfig))
      .name("KafkaAvroSource[%s]".format(topics.mkString(",")))
  }

  def kafkaAvroSink[T <: SpecificRecord : ClassTag](ds: DataStream[T], topic: String, producerConfig: Properties, semantic: FlinkKafkaProducer.Semantic)(implicit schemaRegistryUrl: String) = {
    ds.addSink(new FlinkKafkaProducer[T](topic, new ConfluentRegistryAvroKafkaSerializationSchema[T](topic, schemaRegistryUrl, producerConfig), producerConfig, semantic))
      .name("KafkaAvroSink[%s]".format(topic))
  }

  def hiveParquetAvroSink[T <: SpecificRecordBase : ClassTag](ds: DataStream[T], path: String) = {
    ds.addSink(StreamingFileSink.forBulkFormat(new Path(path), LZOCompressedParquetAvroWriters.forSpecificRecord(classTag[T].runtimeClass.asInstanceOf[Class[T]]))
      .withBucketAssigner(new HiveDateHourPartitionBucketAssigner()).build())
      .name("HiveParquetAvroSink[%s]".format(path))
  }

}
