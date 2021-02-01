/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.spark.v2

import com.google.common.base.{Supplier, Suppliers}
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.druid.java.util.common.granularity.GranularityType

import java.io.File
import java.util
import org.apache.druid.java.util.common.{FileUtils, Intervals, StringUtils}
import org.apache.druid.metadata.{MetadataStorageConnectorConfig, MetadataStorageTablesConfig,
  SQLMetadataConnector}
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.registries.SQLConnectorRegistry
import org.apache.druid.spark.utils.DruidDataSourceOptionKeys
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.partition.NumberedShardSpec
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{ArrayType, BinaryType, DoubleType, FloatType, LongType,
  StringType, StructField, StructType}
import org.joda.time.Interval
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException
import org.skife.jdbi.v2.{DBI, Handle}

import java.util.{Properties, UUID}
import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter,
  seqAsJavaListConverter}
import scala.collection.mutable.ArrayBuffer

trait DruidDataSourceV2TestUtils {
  val dataSource: String = "spark_druid_test"
  val interval: Interval = Intervals.of("2020-01-01T00:00:00.000Z/2020-01-02T00:00:00.000Z")
  val secondInterval: Interval = Intervals.of("2020-01-02T00:00:00.000Z/2020-01-03T00:00:00.000Z")
  val version: String = "0"
  val segmentsDir: File =
    new File(makePath("src", "test", "resources", "segments")).getCanonicalFile
  val firstSegmentPath: String =
    makePath("spark_druid_test", "2020-01-01T00:00:00.000Z_2020-01-02T00:00:00.000Z", "0", "0", "index.zip")
  val secondSegmentPath: String =
    makePath("spark_druid_test", "2020-01-01T00:00:00.000Z_2020-01-02T00:00:00.000Z", "0", "1", "index.zip")
  val thirdSegmentPath: String =
    makePath("spark_druid_test", "2020-01-02T00:00:00.000Z_2020-01-03T00:00:00.000Z", "0", "0", "index.zip")
  val loadSpec: String => util.Map[String, AnyRef] = (path: String) =>
    Map[String, AnyRef]("type" -> "local", "path" -> path).asJava
  val dimensions: util.List[String] = List("dim1", "dim2", "id1", "id2").asJava
  val metrics: util.List[String] = List(
    "count", "sum_metric1","sum_metric2","sum_metric3","sum_metric4","uniq_id1").asJava
  val metricsSpec: String =
    """[
      |  { "type": "count", "name": "count" },
      |  { "type": "longSum", "name": "sum_metric1", "fieldName": "sum_metric1" },
      |  { "type": "longSum", "name": "sum_metric2", "fieldName": "sum_metric2" },
      |  { "type": "doubleSum", "name": "sum_metric3", "fieldName": "sum_metric3" },
      |  { "type": "floatSum", "name": "sum_metric4", "fieldName": "sum_metric4" },
      |  { "type": "thetaSketch", "name": "uniq_id1", "fieldName": "uniq_id1", "isInputThetaSketch": true }
      |]""".stripMargin
  val binaryVersion: Integer = 9

  val firstSegment: DataSegment = new DataSegment(
    dataSource,
    interval,
    version,
    loadSpec(makePath(segmentsDir.getCanonicalPath, firstSegmentPath)),
    dimensions,
    metrics,
    new NumberedShardSpec(0, 0),
    binaryVersion,
    3278L
  )
  val secondSegment: DataSegment = new DataSegment(
    dataSource,
    interval,
    version,
    loadSpec(makePath(segmentsDir.getCanonicalPath, secondSegmentPath)),
    dimensions,
    metrics,
    new NumberedShardSpec(1, 0),
    binaryVersion,
    3299L
  )
  val thirdSegment: DataSegment = new DataSegment(
    dataSource,
    secondInterval,
    version,
    loadSpec(makePath(segmentsDir.getCanonicalPath, thirdSegmentPath)),
    dimensions,
    metrics,
    new NumberedShardSpec(0, 0),
    binaryVersion,
    3409L
  )

  val firstSegmentString: String = MAPPER.writeValueAsString(firstSegment)
  val secondSegmentString: String = MAPPER.writeValueAsString(secondSegment)
  val thirdSegmentString: String = MAPPER.writeValueAsString(thirdSegment)

  val idOneSketch: Array[Byte] = StringUtils.decodeBase64String("AQMDAAA6zJNV0wc7TCHDCQ==")
  val idTwoSketch: Array[Byte] = StringUtils.decodeBase64String("AQMDAAA6zJNHlmybd5/laQ==")
  val idThreeSketch: Array[Byte] = StringUtils.decodeBase64String("AQMDAAA6zJOppPrHQT61Dw==")

  val schema: StructType = StructType(Seq[StructField](
    StructField("__time", LongType),
    StructField("dim1", ArrayType(StringType, false)),
    StructField("dim2", StringType),
    StructField("id1", StringType),
    StructField("id2", StringType),
    StructField("count", LongType),
    StructField("sum_metric1", LongType),
    StructField("sum_metric2", LongType),
    StructField("sum_metric3", DoubleType),
    StructField("sum_metric4", FloatType),
    StructField("uniq_id1", BinaryType)
  ))

  val columnTypes: Option[Set[String]] =
    Option(Set("LONG", "STRING", "FLOAT", "DOUBLE", "thetaSketch"))

  private val tempDirs: ArrayBuffer[String] = new ArrayBuffer[String]()
  def testWorkingStorageDirectory: String = {
    val tempDir = FileUtils.createTempDir("druid-spark-tests").getCanonicalPath
    tempDirs += tempDir
    tempDir
  }

  private val testDbUri = "jdbc:derby:memory:TestDatabase"
  def generateUniqueTestUri(): String = testDbUri + dbSafeUUID

  val metadataClientProps: String => Map[String, String] = (uri: String) => Map[String, String](
    DruidDataSourceOptionKeys.metadataDbTypeKey -> "embedded_derby",
    DruidDataSourceOptionKeys.metadataConnectUriKey -> uri
  )

  lazy val writerProps: Map[String, String] = Map[String, String](
    DataSourceOptions.TABLE_KEY -> dataSource,
    DruidDataSourceOptionKeys.versionKey -> version,
    DruidDataSourceOptionKeys.localStorageDirectoryKey -> testWorkingStorageDirectory,
    DruidDataSourceOptionKeys.dimensionsKey -> dimensions.asScala.mkString(","),
    DruidDataSourceOptionKeys.metricsKey -> metricsSpec,
    DruidDataSourceOptionKeys.timestampColumnKey -> "__time",
    DruidDataSourceOptionKeys.segmentGranularity -> GranularityType.DAY.name
  )

  def createTestDb(uri: String): Unit = new DBI(s"$uri;create=true").open().close()
  def openDbiToTestDb(uri: String): Handle = new DBI(uri).open()
  def tearDownTestDb(uri: String): Unit = {
    try {
      new DBI(s"$uri;shutdown=true").open().close()
    } catch {
      // Closing an in-memory Derby database throws an expected exception. It bubbles up as an
      // UnableToObtainConnectionException from skiffie.
      // TODO: Just open the connection directly and check the exception there
      case _: UnableToObtainConnectionException =>
    }}

  def registerEmbeddedDerbySQLConnector(): Unit = {
    SQLConnectorRegistry.register("embedded_derby",
      (connectorConfigSupplier: Supplier[MetadataStorageConnectorConfig],
       metadataTableConfigSupplier: Supplier[MetadataStorageTablesConfig]) => {
        val connectorConfig = connectorConfigSupplier.get()
        val amendedConnectorConfigSupplier =
          new MetadataStorageConnectorConfig
          {
            override def isCreateTables: Boolean = true
            override def getHost: String = connectorConfig.getHost
            override def getPort: Int = connectorConfig.getPort
            override def getConnectURI: String = connectorConfig.getConnectURI
            override def getUser: String = connectorConfig.getUser
            override def getPassword: String = connectorConfig.getPassword
            override def getDbcpProperties: Properties = connectorConfig.getDbcpProperties
          }

        val res: SQLMetadataConnector =
          new SQLMetadataConnector(Suppliers.ofInstance(amendedConnectorConfigSupplier), metadataTableConfigSupplier) {
            val datasource: BasicDataSource = getDatasource
            datasource.setDriverClassLoader(getClass.getClassLoader)
            datasource.setDriverClassName("org.apache.derby.jdbc.EmbeddedDriver")
            private val dbi = new DBI(connectorConfigSupplier.get().getConnectURI)
            private val SERIAL_TYPE = "BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1)"

            override def getSerialType: String = SERIAL_TYPE

            override def getStreamingFetchSize: Int = 1

            override def getQuoteString: String = "\\\""

            override def tableExists(handle: Handle, tableName: String): Boolean =
              !handle.createQuery("select * from SYS.SYSTABLES where tablename = :tableName")
                .bind("tableName", StringUtils.toUpperCase(tableName)).list.isEmpty;

            override def getDBI: DBI = dbi
          }
        res.createSegmentTable()
        res
      })
  }

  def cleanUpWorkingDirectory(): Unit = {
    tempDirs.foreach(dir => FileUtils.deleteDirectory(new File(dir).getCanonicalFile))
  }

  def partitionReaderToSeq(reader: InputPartitionReader[InternalRow]): Seq[InternalRow] = {
    val res = new ArrayBuffer[InternalRow]()
    while (reader.next()) {
      res += reader.get()
    }
    reader.close()
    res
  }

  def compareInternalRows(left: InternalRow, right: InternalRow, schema: StructType): Boolean = {
    left.numFields == right.numFields && !left.toSeq(schema).forall(right.toSeq(schema).contains(_))
  }

  def makePath(components: String*): String = {
    components.mkString(File.separator)
  }

  def dbSafeUUID: String = StringUtils.removeChar(UUID.randomUUID.toString, '-')
}
