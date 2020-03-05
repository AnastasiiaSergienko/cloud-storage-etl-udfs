package com.exasol.cloudetl.kinesis

import java.util

import com.amazonaws.auth._
import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import com.exasol._
import com.fasterxml.jackson.databind.{MapperFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

import scala.collection.JavaConverters._

object KinesisShardDataImporter {
  def run(exaMetadata: ExaMetadata, exaIterator: ExaIterator): Unit = {
    val kinesisArgumentsReader = new KinesisArgumentsReader(exaIterator)
    val awsAccessKeyId = kinesisArgumentsReader.getAwsAccessKeyArgument
    val awsSecretAccessKey = kinesisArgumentsReader.getAwsSecretKeyArgument
    val awsSessionToken = kinesisArgumentsReader.getAwsSessionTokenArgument
    val streamName = kinesisArgumentsReader.getStreamNameArgument
    val region = kinesisArgumentsReader.getRegionArgument
    val shardId = kinesisArgumentsReader.getShardIdArgument
    val shardSequenceNumber = kinesisArgumentsReader.getShardSequenceNumber
    val records = importRecordsForShard(
      awsAccessKeyId,
      awsSecretAccessKey,
      awsSessionToken,
      streamName,
      region,
      shardId,
      shardSequenceNumber
    )
    try {
      records.asScala.foreach(
        record => {
          val values = createTableValuesListFromRecord(record, shardId)
          val columns: Seq[Object] = values.map(_.asInstanceOf[AnyRef])
          exaIterator.emit(columns: _*)
        }
      )
    } catch {
      case exception @ (_: ExaDataTypeException | _: ExaIterationException) =>
        throw new KinesisConnectorException(
          "KinesisShardDataImporter cannot emit records. Caused: " + exception.getMessage,
          exception
        )
    }
  }

  private def importRecordsForShard(
    awsAccessKeyId: String,
    awsSecretAccessKey: String,
    awsSessionToken: String,
    streamName: String,
    region: String,
    shardId: String,
    shardSequenceNumber: String
  ): util.List[Record] = {
    val awsCredentials = getAwsCredentials(awsAccessKeyId, awsSecretAccessKey, awsSessionToken)
    val amazonKinesis = AmazonKinesisClientBuilder.standard
      .withRegion(region)
      .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
      .build
    val shardIteratorRequest =
      createShardIteratorRequest(shardId, shardSequenceNumber, streamName)
    val shardIteratorResult = amazonKinesis.getShardIterator(shardIteratorRequest)
    val shardIterator = shardIteratorResult.getShardIterator
    getRecords(amazonKinesis, shardIterator)
  }

  private def getAwsCredentials(
    awsAccessKeyId: String,
    awsSecretAccessKey: String,
    awsSessionToken: String
  ) = new BasicSessionCredentials(awsAccessKeyId, awsSecretAccessKey, awsSessionToken)

  private def createShardIteratorRequest(
    shardId: String,
    shardSequenceNumber: String,
    streamName: String
  ): GetShardIteratorRequest = {
    val getShardIteratorRequest = new GetShardIteratorRequest
    getShardIteratorRequest.setStreamName(streamName)
    getShardIteratorRequest.setShardId(shardId)
    if (shardSequenceNumber != null) {
      getShardIteratorRequest.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
      getShardIteratorRequest.setStartingSequenceNumber(shardSequenceNumber)
    } else {
      getShardIteratorRequest.setShardIteratorType(ShardIteratorType.TRIM_HORIZON)
    }
    getShardIteratorRequest
  }

  private def getRecords(
    amazonKinesis: AmazonKinesis,
    shardIterator: String
  ): util.List[Record] = {
    val getRecordsRequest = new GetRecordsRequest
    getRecordsRequest.setShardIterator(shardIterator)
    val result = amazonKinesis.getRecords(getRecordsRequest)
    result.getRecords
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def createTableValuesListFromRecord(record: Record, shardId: String): Seq[Any] = {
    val data = record.getData
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
    val valuesNew =
      mapper.readValue[util.LinkedHashMap[String, AnyRef]](new String(data.array()))
    valuesNew.values().stream().toArray().toSeq ++ Seq(
      shardId,
      record.getSequenceNumber
    )
  }
}
