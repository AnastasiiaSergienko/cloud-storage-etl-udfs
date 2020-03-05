package com.exasol.cloudetl.kinesis

import com.exasol.ExaImportSpecification

import scala.collection.JavaConverters._

object KinesisPropertiesReader {
  val AWS_ACCESS_KEY_PROPERTY: String = "AWS_ACCESS_KEY"
  val AWS_SECRET_KEY_PROPERTY: String = "AWS_SECRET_KEY"
  val AWS_SESSION_TOKEN_PROPERTY: String = "AWS_SESSION_TOKEN"
  val STREAM_NAME_PROPERTY: String = "STREAM_NAME"
  val REGION_PROPERTY: String = "REGION"
  val TABLE_NAME_PROPERTY: String = "TABLE_NAME"
}

class KinesisPropertiesReader(val exaImportSpecification: ExaImportSpecification) {
  private val parametersMap = exaImportSpecification.getParameters.asScala.toMap

  final def getAwsAccessKeyProperty: String =
    getPropertyByName(KinesisPropertiesReader.AWS_ACCESS_KEY_PROPERTY)

  private def getPropertyByName(propertyName: String): String =
    this.parametersMap
      .get(propertyName)
      .fold {
        throw new KinesisConnectorException(
          s"""A mandatory property $propertyName is missing.""",
          null
        )
      }(identity)

  final def getAwsSecretKeyProperty: String =
    getPropertyByName(KinesisPropertiesReader.AWS_SECRET_KEY_PROPERTY)

  final def getAwsSessionTokenProperty: String =
    getPropertyByName(KinesisPropertiesReader.AWS_SESSION_TOKEN_PROPERTY)

  final def getStreamNameProperty: String =
    getPropertyByName(KinesisPropertiesReader.STREAM_NAME_PROPERTY)

  final def getRegionProperty: String =
    getPropertyByName(KinesisPropertiesReader.REGION_PROPERTY)

  final def getTableNameProperty: String =
    getPropertyByName(KinesisPropertiesReader.TABLE_NAME_PROPERTY)
}
