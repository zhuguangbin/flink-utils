package net.qihoo.ads.flink.env.scala

import net.qihoo.ads.flink.sql.utils.FlinkSQLProject
import net.qihoo.ads.kafka.KafkaConfigClient
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Avro, Kafka, Rowtime, Schema}
import org.apache.flink.util.Preconditions

object FlinkSQLEnv {
  def registerAvroKafkaTableSource(project: FlinkSQLProject, tableEnv: StreamTableEnvironment, cluster: String, topic: String, proctime:Boolean, rowtime: Boolean, rowtimeField: String, watermarkDelay: Long) = {
    Preconditions.checkNotNull(project.getKafkaConsumerInfo)
    val consumerProps = KafkaConfigClient.getConsumerConf(project.getKafkaConsumerInfo.getCluster, project.getKafkaConsumerInfo.getConsumerName, project.getKafkaConsumerInfo.getApiSecret)
    val kafkaConnector = new Kafka().version("universal").topic(topic).properties(consumerProps)

    val avroSchemaJson = KafkaConfigClient.latestAvroSchema(topic)
    val subject = avroSchemaJson.get("subject").asText()
    val avroSchemaStr = avroSchemaJson.get("schema").asText()

    val avroSchema = deriveAvroSchema(subject, avroSchemaStr)

    val tableSchema = deriveTableSchema(avroSchemaStr, proctime, rowtime, rowtimeField, watermarkDelay)

    tableEnv.connect(
      kafkaConnector
    ).withFormat(
      avroSchema
    ).withSchema(
      tableSchema
    ).inAppendMode()
      .registerTableSource("kafka_table_source_%s".format(topic.replaceAll("\\.", "_")))

  }

  def registerAvroKafkaTableSink(project: FlinkSQLProject, tableEnv: StreamTableEnvironment, cluster: String, topic: String) = {
    Preconditions.checkNotNull(project.getKafkaProducerInfo)
    val producerProps = KafkaConfigClient.getProducerConf(project.getKafkaProducerInfo.getCluster, project.getKafkaProducerInfo.getProducerName, project.getKafkaProducerInfo.getApiSecret)
    val kafkaConnector = new Kafka().version("universal").topic(topic).properties(producerProps)

    val avroSchemaJson = KafkaConfigClient.latestAvroSchema(topic)
    val subject = avroSchemaJson.get("subject").asText()
    val avroSchemaStr = avroSchemaJson.get("schema").asText()

    val avroSchema = deriveAvroSchema(subject, avroSchemaStr)

    val tableSchema = deriveTableSchema(avroSchemaStr, false, false, null, 0)

    tableEnv.connect(
      kafkaConnector
    ).withFormat(
      avroSchema
    ).withSchema(
      tableSchema
    ).inAppendMode()
      .registerTableSink("kafka_table_sink_%s".format(topic.replaceAll("\\.", "_")))

  }

  private def deriveAvroSchema(subject: String, avroSchemaStr: String) = {

    val avroSchema = new Avro()
      .useRegistry(true)
      .registryUrl(KafkaConfigClient.KAFKA_SCHEMA_REGISTRY_URL_ADDRESS)
      .registrySubject(subject)
      .avroSchema(avroSchemaStr)
    avroSchema
  }

  private def deriveTableSchema(avroSchemaString: String, proctime:Boolean, rowtime: Boolean, rowtimeField: String, watermarkDelay: Long) = {
    val tableSchema = new Schema()

    if (rowtime) {
      Preconditions.checkNotNull(rowtimeField, "You must set rowtimeField when you set rowtime")
      Preconditions.checkNotNull(watermarkDelay, "You must set watermarkDelay when you set rowtime")
      tableSchema
        .field("rowtime", Types.SQL_TIMESTAMP)
        .rowtime(
          new Rowtime()
            .timestampsFromField(rowtimeField)
            .watermarksPeriodicBounded(watermarkDelay)
        )
    } else if (proctime){
      tableSchema.field("proctime", Types.SQL_TIMESTAMP)
        .proctime()
    }

    val row = AvroSchemaConverter.convertToTypeInfo(avroSchemaString).asInstanceOf[RowTypeInfo]
    val fieldNames = row.getFieldNames
    val fieldTypes = row.getFieldTypes
    fieldNames.zip(fieldTypes)
      .foreach(f => tableSchema.field(f._1, f._2))

    tableSchema
  }

}
