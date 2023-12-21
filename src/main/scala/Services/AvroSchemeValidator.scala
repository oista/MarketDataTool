package Services

import org.apache.avro.Schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.struct
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.avro.read.confluent.{SchemaManager, SchemaManagerFactory}
import za.co.absa.abris.avro.registry.SchemaSubject
import za.co.absa.abris.config.{AbrisConfig, ToAvroConfig}

class AvroSchemeValidator {
  val schemaRegistryClientConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> "http://localhost:8081")
  val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)

  val abrisConfig = AbrisConfig
    .fromConfluentAvro
    .downloadReaderSchemaByLatestVersion
    .andTopicNameStrategy("XNAS")
    .usingSchemaRegistry(schemaRegistryClientConfig) // use the map instead of just url

  // register schema with topic name strategy
  def registerSchema1(schema: Schema, schemaManager: SchemaManager): Int = {
    val subject = SchemaSubject.usingTopicNameStrategy("topic", isKey=true) // Use isKey=true for the key schema and isKey=false for the value schema
    schemaManager.register(subject, schema)
  }

  // register schema with record name strategy
  def registerSchema2(schema: Schema, schemaManager: SchemaManager): Int = {
    val subject = SchemaSubject.usingRecordNameStrategy(schema)
    schemaManager.register(subject, schema)
  }

  // register schema with topic record name strategy
  def registerSchema3(schema: Schema, schemaManager: SchemaManager): Int = {
    val subject = SchemaSubject.usingTopicRecordNameStrategy("topic", schema)
    schemaManager.register(subject, schema)
  }

  def get_scheme_id_by_subject(subject: String):Int = {
   // val sr = SchemaRegistryClient('localhost: 8081')
   // my_schema = sr.get_schema(subject = 'mySubject', version = 'latest')
    1
  }

  def writeAvro(dataFrame: DataFrame, toAvroConfig: ToAvroConfig): DataFrame = {
    val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
    dataFrame.select(to_avro(allColumns, toAvroConfig) as 'value)
  }
}
