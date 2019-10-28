package com.exasol.cloudetl.bucket

import com.exasol.cloudetl.storage.StorageProperties

import org.apache.hadoop.conf.Configuration

/** A [[Bucket]] implementation for the AWS S3 */
final case class S3Bucket(path: String, params: StorageProperties) extends Bucket {

  private[this] val S3_ENDPOINT: String = "S3_ENDPOINT"
  private[this] val S3_ACCESS_KEY: String = "S3_ACCESS_KEY"
  private[this] val S3_SECRET_KEY: String = "S3_SECRET_KEY"

  /** @inheritdoc */
  override val bucketPath: String = path

  /** @inheritdoc */
  override val properties: StorageProperties = params

  /** Returns the list of required property keys for AWS S3 Storage. */
  override def getRequiredProperties(): Seq[String] =
    Seq(S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY)

  /**
   * @inheritdoc
   *
   * Additionally validates that all required parameters are available
   * in order to create a configuration.
   */
  override def getConfiguration(): Configuration = {
    validate()

    val conf = new Configuration()
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    conf.set("fs.s3a.impl", classOf[org.apache.hadoop.fs.s3a.S3AFileSystem].getName)
    conf.set("fs.s3a.endpoint", properties.getString(S3_ENDPOINT))
    conf.set("fs.s3a.access.key", properties.getString(S3_ACCESS_KEY))
    conf.set("fs.s3a.secret.key", properties.getString(S3_SECRET_KEY))

    conf
  }

}
