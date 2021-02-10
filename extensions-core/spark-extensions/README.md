<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

# Apache Spark Reader and Writer for Druid

## Reader
The reader reads Druid segments from deep storage into Spark. It locates the segments to read and determines their schema if not provided by querying the brokers for the relevant metadata but otherwise does not interact with a running Druid cluster.

Sample Code:
```scala
val metadataProperties = Map[String, String](
  "metadataDbType" -> "mysql",
  "metadataConnectUri" -> "jdbc:mysql://druid.metadata.server:3306/druid",
  "metadataUser" -> "druid",
  "metadataPassword" -> "diurd"
)

sparkSession
  .read
  .format("druid")
  .options(Map[String, String]("table" -> "dataSource") ++ metadataProperties)
  .load()
```

If you know the schema of the Druid data source you're reading from, you can save needing to determine the schema via calls to the broker with
```scala
sparkSession
  .read
  .format("druid")
  .schema(schema)
  .options(Map[String, String]("table" -> "dataSource") ++ metadataProperties)
  .load()
```

Filters should be applied to the read-in data frame before any [Spark actions](http://spark.apache.org/docs/2.4.5/api/scala/index.html#org.apache.spark.sql.Dataset) are triggered, to allow predicates to be pushed down to the reader and avoid full scans of the underlying Druid data.

## Writer
The writer writes Druid segments directly to deep storage and then updates the Druid cluster's metadata, bypassing the running cluster entirely.

Sample Code:
```scala
val metadataProperties = Map[String, String](
  "metadataDbType" -> "mysql",
  "metadataConnectUri" -> "jdbc:mysql://druid.metadata.server:3306/druid",
  "metadataUser" -> "druid",
  "metadataPassword" -> "diurd"
)

val writerConfigs = Map[String, String] (
  "table" -> "dataSource",
  "version" -> 1,
  "deepStorageType" -> "local",
  "storageDirectory" -> "/mnt/druid/druid-segments/"
)

df
  .write
  .format("druid")
  .mode(SaveMode.Overwrite)
  .options(Map[String, String](writerConfigs ++ metadataProperties))
  .save()
```

### Partitioning & `PartitionMap`s
The segments written by this writer are controller by the calling DataFrame's internal partitioning.
If there are many small partitions, or the DataFrame's partitions span output intervals, then many
small Druid segments will be written with poor overall roll-up. If the DataFrame's partitions are
skewed, then the Druid segments produced will also be skewed. To avoid this, users should take
care to properly partition their DataFrames prior to calling `.write()`. Additionally, for some shard
specs such as `HashBasedNumberedShardSpec` or `SingleDimensionShardSpec` which require a higher-level
view of the data than can be obtained from a partition, callers should pass along a `PartitionMap`
containing metadata for each Spark partition. This partition map can be serialized using the
`PartitionMapProvider.serializePartitionMap` and passed along with the writer options using the
`partitionMap` key. The partitioners in `org.apache.druid.spark.partitioners` can be used to partition
DataFrames and generate the corresponding `partitionMap` if necessary, but this is less efficient
than partitioning the DataFrame in the desired manner in the course of preparing the data.

If a "simpler" shard spec such as `NumberedShardSpec` or `LinearShardSpec` is used, a `partitionMap`
can be provided but is unnecessary unless the name of a segment's directory on deep storage should
match the segment's id exactly. The writer will rationalize shard specs within time chunks to
ensure data is atomically loaded in Druid. Care should still be taken in partitioning the DataFrame
to write regardless.

## Configuration Reference

### Metadata Client Configs
The properties used to configure the client that interacts with the Druid metadata server directly. Used by both reader and the writer. During early development, `metadataPassword` is expected in plaintext. This will change.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`metadataDbType`|The metadata server's database type (e.g. `mysql`)|Yes||
|`metadataHost`|The metadata server's host name|If using derby|`localhost`|
|`metadataPort`|The metadata server's port|If using derby||
|`metadataConnectUri`|The URI to use to connect to the metadata server|If not using derby||
|`metadataUser`|The user to use when connecting to the metadata server|Yes||
|`metadataPassword`|The password to use when connecting to the metadata server|Yes||
|`metadataDbcpProperties`|The connection pooling properties to use when connecting to the metadata server|No||
|`metadataBaseName`|The base name used when creating Druid metadata tables|No|`druid`|

### Druid Client Configs
The configuration properties used to query the Druid cluster for segment metadata. Only used in the reader.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`brokerHost`|The hostname of a broker in the Druid cluster to read from|No|`localhost`|
|`brokerPort`|The port of the broker in the Druid cluster to read from|No|8082|

### Reader Configs
The properties used to configure the DataSourceReader when reading data from Druid in Spark.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`table`|The Druid data source to read from|Yes||
|`segments`|A hard-coded list of Druid segments to read. If set, all other configurations are ignored and the specified segments are read directly. Must be deserializable into Druid DataSegment instances|No|
|`useCompactSketches`|Controls whether or not compact representations of complex metrics are used (only for metrics that support compact forms)|No|False|


### Writer Configs
The properties used to configure the DataSourceWriter when writing data to Druid from Spark. See the [ingestion specs documentation](../../ingestion/index.html#ingestion-specs) for more details.

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`table`|The Druid data source to write to|Yes||
|`version`|The version of the segments to be written|No|The current Unix epoch time|
|`dimensions`|A comma-delimited list of the dimensions to write to Druid. If not set, all dimensions in the dataframe that aren't either explicitly set as metrics or excluded via `excludedDimensions` will be used|No||
|`metrics`|The [metrics spec](../../ingestion/index.html#metricsspec) used to define the metrics for the segments written to Druid. `fieldName` must match a column in the source dataframe|No|`[]`|
|`excludedDimensions`|A comma-delimited list of the columns in the data frame to exclude when writing to Druid. Ignored if `dimensions` is set|No||
|`segmentGranularity`|The chunking [granularity](../../querying/granularities.html) of the Druid segments written (e.g. what granularity to partition the output segments by on disk)|No|`all`|
|`queryGranularity`|The resolution [granularity](../../querying/granularities.html) of rows _within_ the Druid segments written|No|`none`|
|`partitionMap`|A mapping between partitions of the source Spark dataframe and the necessary information for generating Druid segment partitions from the Spark partitions. Has the type signature `Map[Int, Map[String, String]]`|No||
|`deepStorageType`|The type of deep storage used to back the target Druid cluster|No|`local`|
|`timestampColumn`|The Spark dataframe column to use as a timestamp for each record|No|`ts`|
|`timestampFormat`|The format of the timestamps in `timestampColumn`|No|`auto`|
|`shardSpecType`|The type of shard spec used to partition the segments produced|No|`numbered`|
|`rollUpSegments`|Whether or not to roll up segments produced|No|True|
|`rowsPerPersist`|How many rows to hold in memory before flushing intermediate indices to disk|No|2000000|
|`rationalizeSegments`|Whether or not to rationalize segments to ensure contiguity and completeness|No|True if `partitionMap` is not set, False otherwise|

### Deep Storage Configs
The configuration properties used when interacting with deep storage systems directly. Only used in the writer.

**Caution**: The S3, GCS, and Azure storage configs don't work. Users will have to implement their own segment writers and register them with the SegmentWriterRegistry (and hopefully contribute them back to this warning can be removed! :))

#### Local Deep Storage Config
`deepStorageType` = `local`

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`storageDirectory`|The location to write segments out to|Yes||

#### HDFS Deep Storage Config
`deepStorageType` = `hdfs`

|Key|Description|Required|Default|
|---|-----------|--------|-------|
|`storageDirectory`|The location to write segments out to|Yes||
|`hadoopConf`|A Base64 encoded representation of dumping a Hadoop Configuration to a byte array via `.write`|Yes||

#### S3 Deep Storage Config
`deepStorageType` = `s3`

|Key|Description|Required|Default|
|---|-----------|--------|-------|

#### GCS Deep Storage Config
`deepStorageType` = `google`

|Key|Description|Required|Default|
|---|-----------|--------|-------|

#### Azure Deep Storage Config
`deepStorageType` = `azure`

|Key|Description|Required|Default|
|---|-----------|--------|-------|