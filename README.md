# Exasol Public Cloud Storage ETL UDFs

###### Please note that this is an open source project which is *not officially supported* by Exasol. We will try to help you as much as possible, but can't guarantee anything since this is not an official Exasol product.

## Table of Contents

* [Overview](#overview)
* [Usage](#usage)
* [Building from Source](#building-from-source)

## Overview

This repository contains helper code to create [Exasol][exasol] ETL UDFs in
order to transfer data to/from public cloud storage services such as [AWS
S3][s3], [Google Cloud Storage][gcs] and [Azure Blob Storage][azure].

**Currently only importing parquet (primitive types) files from AWS S3 into
Exasol is supported.**

## Usage

Please follow the steps described below in order to setup the UDFs.

### Download the JAR file

Download the latest jar file from [here][jar].

Additionally, you can also build it from the source by following the [build from
source](#building-from-source) step.

### Upload the JAR file to Exasol BucketFS

```bash
curl \
  -X PUT \
  -T path/to/jar/cloud-storage-etl-udfs-{VERSION}.jar \
  http://w:MY-PASSWORD@EXA-NODE-ID:2580/bucket1/cloud-storage-etl-udfs-{VERSION}.jar
```

Please change required parameters.

### Create UDFs scripts

```sql
CREATE SCHEMA ETL;
OPEN SCHEMA ETL;

CREATE OR REPLACE JAVA SET SCRIPT IMPORT_S3_PATH(...) EMITS (...) AS
%scriptclass com.exasol.s3etl.scriptclasses.ImportS3Path;
%jar /buckets/bfsdefault/bucket1/cloud-storage-etl-udfs-{VERSION}.jar;
/

CREATE OR REPLACE JAVA SET SCRIPT IMPORT_S3_FILES(...) EMITS (...) AS
%env LD_LIBRARY_PATH=/tmp/;
%scriptclass com.exasol.s3etl.scriptclasses.ImportS3Files;
%jar /buckets/bfsdefault/bucket1/cloud-storage-etl-udfs-{VERSION}.jar;
/

CREATE OR REPLACE JAVA SCALAR SCRIPT IMPORT_S3_METADATA(...)
EMITS (s3_filename VARCHAR(200), partition_index VARCHAR(100)) AS
%scriptclass com.exasol.s3etl.scriptclasses.ImportS3Metadata;
%jar /buckets/bfsdefault/bucket1/cloud-storage-etl-udfs-{VERSION}.jar;
/
```

### Import data from cloud storage

```sql
CREATE SCHEMA TEST;
OPEN SCHEMA TEST;

DROP TABLE IF EXISTS SALES_POSITIONS;

CREATE TABLE SALES_POSITIONS (
  SALES_ID    INTEGER,
  POSITION_ID SMALLINT,
  ARTICLE_ID  SMALLINT,
  AMOUNT      SMALLINT,
  PRICE       DECIMAL(9,2),
  VOUCHER_ID  SMALLINT,
  CANCELED    BOOLEAN
);

-- ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS='10.0.2.162:3000';

IMPORT INTO SALES_POSITIONS
FROM SCRIPT ETL.IMPORT_S3_PATH WITH
 S3_BUCKET_PATH = 's3a://exa-mo-frankfurt/test/retail/sales_positions/*'
 S3_ACCESS_KEY  = 'MY_AWS_ACCESS_KEY'
 S3_SECRET_KEY  = 'MY_AWS_SECRET_KEY'
 PARALLELISM    = 'nproc()*10';


SELECT * FROM SALES_POSITIONS LIMIT 10;
```

## Building from Source

Clone the repository,

```bash
git clone https://github.com/EXASOL/cloud-storage-etl-udfs

cd cloud-storage-etl-udfs/
```

Create assembly jar,

```bash
./sbtx assembly
```

The packaged jar should be located at
`target/scala-2.11/cloud-storage-etl-udfs-{VERSION}.jar`.

[exasol]: https://www.exasol.com/en/
[s3]: https://aws.amazon.com/s3/
[gcs]: https://cloud.google.com/storage/
[azure]: https://azure.microsoft.com/en-us/services/storage/blobs/
[jar]: https://github.com/exasol/cloud-storage-etl-udfs/releases/download/v0.0.1/cloud-storage-etl-udfs-0.0.1.jar
