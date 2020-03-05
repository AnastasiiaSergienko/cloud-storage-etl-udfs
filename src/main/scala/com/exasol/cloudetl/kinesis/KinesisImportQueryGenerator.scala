package com.exasol.cloudetl.kinesis

import com.exasol.cloudetl.kinesis.KinesisConstants._
import com.exasol.{ExaImportSpecification, ExaMetadata}

object KinesisImportQueryGenerator {
  def generateSqlForImportSpec(
    exaMetadata: ExaMetadata,
    importSpecification: ExaImportSpecification
  ): String = {
    val kinesisPropertiesReader = new KinesisPropertiesReader(importSpecification)
    val awsAccessKeyId = kinesisPropertiesReader.getAwsAccessKeyProperty
    val awsSecretAccessKey = kinesisPropertiesReader.getAwsSecretKeyProperty
    val awsSessionToken = kinesisPropertiesReader.getAwsSessionTokenProperty
    val streamName = kinesisPropertiesReader.getStreamNameProperty
    val region = kinesisPropertiesReader.getRegionProperty
    val tableName = kinesisPropertiesReader.getTableNameProperty

    s"""SELECT IMPORT_KINESIS_SINGLE_SHARD_DATA(
       |  '$awsAccessKeyId', '$awsSecretAccessKey', '$awsSessionToken', '$streamName', '$region',
       |  $KINESIS_SHARD_ID_COLUMN_NAME, $KINESIS_SEQUENCE_NUMBER_COLUMN_NAME
       |)
       |FROM (
       |  SELECT KINESIS_SHARDS.$KINESIS_SHARD_ID_COLUMN_NAME,
       |  MAX($KINESIS_SEQUENCE_NUMBER_COLUMN_NAME) AS $KINESIS_SEQUENCE_NUMBER_COLUMN_NAME
       |  FROM (
       |    SELECT READ_KINESIS_SHARDS_METADATA(
       |    '$awsAccessKeyId', '$awsSecretAccessKey', '$awsSessionToken', '$streamName', '$region'
       |    )) AS KINESIS_SHARDS LEFT JOIN $tableName
       |    ON $tableName.$KINESIS_SHARD_ID_COLUMN_NAME =
       |    KINESIS_SHARDS.$KINESIS_SHARD_ID_COLUMN_NAME
       |    GROUP BY KINESIS_SHARDS.$KINESIS_SHARD_ID_COLUMN_NAME
       |)
       |GROUP BY $KINESIS_SHARD_ID_COLUMN_NAME;
       |""".stripMargin
  }
}
