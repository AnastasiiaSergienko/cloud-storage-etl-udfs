package com.exasol.cloudetl.kinesis

import com.exasol._

object KinesisArgumentsReader {
  val AWS_ACCESS_KEY_ARGUMENT: Int = 0
  val AWS_SECRET_KEY_ARGUMENT: Int = 1
  val AWS_SESSION_TOKEN_ARGUMENT: Int = 2
  val STREAM_NAME_ARGUMENT: Int = 3
  val REGION_ARGUMENT: Int = 4
  val SHARD_ID_ARGUMENT: Int = 5
  val SHARD_SEQUENCE_NUMBER_ARGUMENT: Int = 6
}

class KinesisArgumentsReader(val exaIterator: ExaIterator) {
  final def getAwsAccessKeyArgument: String =
    getStringProperty(KinesisArgumentsReader.AWS_ACCESS_KEY_ARGUMENT)

  private def getStringProperty(columnNumber: Int): String =
    try this.exaIterator.getString(columnNumber)
    catch {
      case exception @ (_: ExaIterationException | _: ExaDataTypeException) =>
        val message = exception.getMessage
        throw new KinesisConnectorException(
          s"""Kinesis connector cannot read argument with column number $columnNumber. "
            "Please make sure that you specified this argument. Caused: $message.""",
          exception
        )
    }

  final def getAwsSecretKeyArgument: String =
    getStringProperty(KinesisArgumentsReader.AWS_SECRET_KEY_ARGUMENT)

  final def getAwsSessionTokenArgument: String =
    getStringProperty(KinesisArgumentsReader.AWS_SESSION_TOKEN_ARGUMENT)

  final def getStreamNameArgument: String =
    getStringProperty(KinesisArgumentsReader.STREAM_NAME_ARGUMENT)

  final def getRegionArgument: String =
    getStringProperty(KinesisArgumentsReader.REGION_ARGUMENT)

  final def getShardIdArgument: String =
    getStringProperty(KinesisArgumentsReader.SHARD_ID_ARGUMENT)

  final def getShardSequenceNumber: String =
    getStringProperty(KinesisArgumentsReader.SHARD_SEQUENCE_NUMBER_ARGUMENT)
}
