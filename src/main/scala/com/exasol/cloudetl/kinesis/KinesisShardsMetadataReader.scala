package com.exasol.cloudetl.kinesis

import java.util

import com.amazonaws.auth._
import com.amazonaws.services.kinesis.model._
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClientBuilder}
import com.exasol._

import scala.collection.JavaConverters._

object KinesisShardsMetadataReader {
  def run(exaMetadata: ExaMetadata, exaIterator: ExaIterator): Unit = {
    val kinesisArgumentsReader = new KinesisArgumentsReader(exaIterator)
    val awsAccessKeyId = kinesisArgumentsReader.getAwsAccessKeyArgument
    val awsSecretAccessKey = kinesisArgumentsReader.getAwsSecretKeyArgument
    val awsSessionToken = kinesisArgumentsReader.getAwsSessionTokenArgument
    val streamName = kinesisArgumentsReader.getStreamNameArgument
    val region = kinesisArgumentsReader.getRegionArgument
    val kinesisMetadata = getKinesisShardsIds(
      awsAccessKeyId,
      awsSecretAccessKey,
      awsSessionToken,
      streamName,
      region
    )
    try {
      kinesisMetadata.foreach(
        shardId => exaIterator.emit(shardId)
      )
    } catch {
      case exception @ (_: ExaDataTypeException | _: ExaIterationException) =>
        val message = exception.getMessage
        throw new KinesisConnectorException(
          s"KinesisShardsMetadataReader cannot emit shards' ids. Caused: $message",
          exception
        )
    }
  }

  private def getKinesisShardsIds(
    awsAccessKeyId: String,
    awsSecretAccessKey: String,
    awsSessionToken: String,
    streamName: String,
    region: String
  ): List[String] = {
    val awsCredentials = getAwsCredentials(awsAccessKeyId, awsSecretAccessKey, awsSessionToken)
    val amazonKinesis = AmazonKinesisClientBuilder.standard
      .withRegion(region)
      .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
      .build
    val shards = getAllShardsFromStream(streamName, amazonKinesis)
    collectShardsIds(shards)
  }

  private def getAwsCredentials(
    awsAccessKeyId: String,
    awsSecretAccessKey: String,
    awsSessionToken: String
  ) = new BasicSessionCredentials(awsAccessKeyId, awsSecretAccessKey, awsSessionToken)

  private def getAllShardsFromStream(
    streamName: String,
    amazonKinesis: AmazonKinesis
  ): util.ArrayList[Shard] = {
    val describeStreamRequest = new DescribeStreamRequest
    describeStreamRequest.setStreamName(streamName)
    val shards = new util.ArrayList[Shard]
    var exclusiveStartShardId: String = null
    do {
      describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId)
      val describeStreamResult = amazonKinesis.describeStream(describeStreamRequest)
      val _ = shards.addAll(describeStreamResult.getStreamDescription.getShards)
      if (describeStreamResult.getStreamDescription.getHasMoreShards && !shards.isEmpty) {
        exclusiveStartShardId = shards.get(shards.size - 1).getShardId
      } else {
        exclusiveStartShardId = null
      }
    } while ({
      exclusiveStartShardId != null
    })
    shards
  }

  private def collectShardsIds(shards: util.List[Shard]): List[String] =
    shards.asScala.toStream.map(shard => shard.getShardId).toList
}
