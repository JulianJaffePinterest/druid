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

import java.util
import java.util.{List => JList}

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.druid.java.util.common.{DateTimes, Intervals, JodaUtils}
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.clients.{DruidClient, DruidMetadataClient}
import org.apache.druid.spark.registries.ComplexMetricRegistry
import org.apache.druid.spark.utils.{Configuration, DruidConfigurationKeys}
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.{And, EqualNullSafe, EqualTo, Filter, GreaterThan,
  GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains,
  StringEndsWith, StringStartsWith}
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition,
  SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsScanColumnarBatch}
import org.apache.spark.sql.types.{ArrayType, BinaryType, DoubleType, FloatType, LongType,
  StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.joda.time.Interval

import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

/**
  * A DruidDataSourceReader handles the actual work of reading data from Druid. It does this by querying to determine
  * where Druid segments live in deep storage and then reading those segments into memory in order to avoid straining
  * the Druid cluster. In general, users should not directly instantiate instances of this class but instead use
  * sparkSession.read.format("druid").options(Map(...)).load(). If the schema of the data in Druid is known, overhead
  * can be further reduced by providing it directly (e.g. sparkSession.read.format("druid").schema(schema).options...)
  *
  * To aid comprehensibility, some idiomatic Scala has been somewhat java-fied.
  *
  * @param schema
  * @param conf
  */
class DruidDataSourceReader(
                             var schema: Option[StructType] = None,
                             conf: Configuration
                           ) extends DataSourceReader
  with SupportsPushDownRequiredColumns with SupportsPushDownFilters with SupportsScanColumnarBatch {
  private lazy val metadataClient =
    DruidDataSourceReader.createDruidMetaDataClient(conf)
  private lazy val druidClient = DruidDataSourceReader.createDruidClient(conf)

  private var filters: Array[Filter] = Array.empty
  private var druidColumnTypes: Option[Set[String]] = Option.empty

  override def readSchema(): StructType = {
    if (schema.isDefined) {
      schema.get
    } else {
      require(conf.isPresent(DruidConfigurationKeys.tableKey),
        s"Must set ${DruidConfigurationKeys.tableKey}!")
      // TODO: Optionally accept a granularity so that if lowerBound to upperBound spans more than
      //  twice the granularity duration, we can send a list with two disjoint intervals and
      //  minimize the load on the broker from having to merge large numbers of segments
      val (lowerBound, upperBound) = getTimeFilterBounds
      val columnMap = druidClient.getSchema(
        conf.getString(DruidConfigurationKeys.tableKey),
        List[Interval](Intervals.utc(
          lowerBound.getOrElse(JodaUtils.MIN_INSTANT),
          upperBound.getOrElse(JodaUtils.MAX_INSTANT)
        ))
      )
      schema = Option(DruidDataSourceReader.convertDruidSchemaToSparkSchema(columnMap))
      druidColumnTypes = Option(columnMap.map(_._2._1).toSet)
      schema.get
    }
  }

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    // For now, one partition for each Druid segment partition
    // Future improvements can use information from SegmentAnalyzer results to do smart things
    if (schema.isEmpty) {
      readSchema()
    }
    println(conf.toString) // scalastyle:ignore println
    val readerConf = conf.dive(DruidConfigurationKeys.readerPrefix)
    val useCompactSketches = readerConf.isPresent(DruidConfigurationKeys.useCompactSketchesKey)
    // Allow passing hard-coded list of segments to load
    if (readerConf.isPresent(DruidConfigurationKeys.segmentsKey)) {
      val segments: JList[DataSegment] = MAPPER.readValue(
        readerConf.getString(DruidConfigurationKeys.segmentsKey),
        new TypeReference[JList[DataSegment]]() {})
      segments.asScala
        .map(segment =>
          new DruidInputPartition(
            segment,
            schema.get,
            filters,
            druidColumnTypes,
            useCompactSketches
          ): InputPartition[InternalRow]
        ).asJava
    } else {
      getSegments
        .map(segment=>
          new DruidInputPartition(
            segment,
            schema.get,
            filters,
            druidColumnTypes,
            useCompactSketches
          ): InputPartition[InternalRow]
        ).asJava
    }
  }

  override def pruneColumns(structType: StructType): Unit = {
    schema = Option(structType)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    filters.partition(isSupportedFilter) match {
      case (supported, unsupported) =>
        this.filters = supported
        unsupported
    }
  }

  override def pushedFilters(): Array[Filter] = filters

  private def isSupportedFilter(filter: Filter): Boolean = filter match {
    case _: And => true
    case _: Or => true
    case _: Not => true
    case _: IsNull => false // Setting null-related filters to false for now
    case _: IsNotNull => false // Setting null-related filters to false for now
    case _: In => true
    case _: StringContains => true
    case _: StringStartsWith => true
    case _: StringEndsWith => true
    case _: EqualTo => true
    case _: EqualNullSafe => false // Setting null-related filters to false for now
    case _: LessThan => true
    case _: LessThanOrEqual => true
    case _: GreaterThan => true
    case _: GreaterThanOrEqual => true

    case _ => false
  }

  private[v2] def getSegments: Seq[DataSegment] = {
    require(conf.isPresent(DruidConfigurationKeys.tableKey),
      s"Must set ${DruidConfigurationKeys.tableKey}!")

    // Check filters for any bounds on __time
    // Otherwise, we'd need to full scan the segments table
    val (lowerTimeBound, upperTimeBound) = getTimeFilterBounds

    metadataClient.getSegmentPayloads(conf.getString(DruidConfigurationKeys.tableKey),
      lowerTimeBound.map(bound =>
        DateTimes.utc(bound).toString("yyyy-MM-ddTHH:mm:ss.SSS'Z'")
      ),
      upperTimeBound.map(bound =>
        DateTimes.utc(bound).toString("yyyy-MM-ddTHH:mm:ss.SSS'Z'")
      )
    )
  }

  private[v2] def getTimeFilterBounds: (Option[Long], Option[Long]) = {
    val timeFilters = filters
      .filter(_.references.contains("__time"))
      .flatMap(DruidDataSourceReader.decomposeTimeFilters)
      .partition(_._1 == DruidDataSourceReader.LOWER)
    (timeFilters._1.map(_._2).reduceOption(_ max _ ),
      timeFilters._2.map(_._2).reduceOption(_ min _ ))
  }

  // Stubs for columnar reads; for now always return false for enableBatchRead, meaning this will
  // never be called
  override def planBatchInputPartitions(): JList[InputPartition[ColumnarBatch]] = {
    List.empty[InputPartition[ColumnarBatch]].asJava
  }

  override def enableBatchRead(): Boolean = false
}

object DruidDataSourceReader {
  def apply(schema: StructType, dataSourceOptions: DataSourceOptions): DruidDataSourceReader = {
    new DruidDataSourceReader(Option(schema), Configuration(dataSourceOptions))
  }

  def apply(dataSourceOptions: DataSourceOptions): DruidDataSourceReader = {
    new DruidDataSourceReader(None, Configuration(dataSourceOptions))
  }

  /* Unfortunately, there's no single method of interacting with a Druid cluster that provides all
   * three operations we need: get segment locations, get dataSource schemata, and publish segments.
   *
   * Segment locations can be determined either via direct interaction with the metadata server or
   * via the coordinator API, but not via querying the `sys.segments` table served from a cluster
   * since the `sys.segments` table prunes load specs.
   *
   * Data source schemata can be determined via querying the `INFORMATION_SCHEMA.COLUMNS` table, via
   * SegmentMetadataQueries, or via pulling segments into memory and analyzing them. However,
   * SegmentMetadataQueries can be expensive and time-consuming for large numbers of segments. This
   * could be worked around by only checking the first and last segments for an interval, which
   * would catch schema evolution that spans the interval to query, but not schema evolution within
   * the interval and would prevent determining accurate statistics. Likewise, pulling segments into
   * memory on the driver to check their schema is expensive and inefficient and has the same schema
   * evolution and accurate statistics problem. The downside of querying the
   * `INFORMATION_SCHEMA.COLUMNS` table is that unlike sending a SegmentMetadataQuery or pulling a
   * segment into memory, we wouldn't have access to possibly useful statistics about the segments
   * that could be used to perform more efficient reading, and the Druid cluster to read from would
   * need to have sql querying initialized and be running a version of Druid >= 0.14. Since we're
   * not currently doing any intelligent partitioning for reads, concerns about statistics are
   * mostly irrelevant.
   *
   * Publishing segments can only be done via direct interaction with the metadata server.
   *
   * Since there's no way to satisfy these constraints with a single method of interaction, we will
   * need to use a metadata client and a druid client. The metadata client can fetch segment
   * locations and publish segments, and the druid client will issue sql queries to determine
   * datasource schemata. If there's concerns around performance issues due to "dumb" readers or
   * a need to support non-sql enabled Druid clusters, the druid client can instead be used to send
   * SegmentMetadataQueries. In order to allow growth in this direction and avoid requiring users
   * to include avatica jars in their Spark cluster, this client uses HTTP requests instead of the
   * JDBC protocol.
   */

  def createDruidMetaDataClient(conf: Configuration): DruidMetadataClient = {
    DruidMetadataClient(conf)
  }

  def createDruidClient(conf: Configuration): DruidClient = {
    DruidClient(conf)
  }

  /**
    * Convert a COLUMNMAP representing a Druid datasource's schema as returned by
    * DruidMetadataClient.getClient into a Spark StructType.
    *
    * @param columnMap The Druid schema to convert into a corresponding Spark StructType.
    * @return The StructType equivalent of the Druid schema described by COLUMNMAP.
    */
  def convertDruidSchemaToSparkSchema(columnMap: Map[String, (String, Boolean)]): StructType = {
    StructType.apply(
      columnMap.map { case (name, (colType, hasMultipleValues)) =>
        val sparkType = colType match {
          case "LONG" => LongType
          case "STRING" => StringType
          case "DOUBLE" => DoubleType
          case "FLOAT" => FloatType
          case "TIMESTAMP" => TimestampType
          case complexType
            if ComplexMetricRegistry.getRegisteredMetricNames.contains(complexType) =>
            BinaryType
          // Add other supported types later
          case _ => throw new IllegalArgumentException(s"Unrecognized type $colType!")
        }
        if (hasMultipleValues) {
          StructField(name, new ArrayType(sparkType, false))
        } else {
          StructField(name, sparkType)
        }
      }.toSeq
    )
  }

  private val emptyBoundSeq = Seq.empty[(Bound, Long)]

  private[v2] def decomposeTimeFilters(filter: Filter): Seq[(Bound, Long)] = {
    filter match {
      case And(left, right) =>
        Seq(left, right).filter(_.references.contains("__time")).flatMap(decomposeTimeFilters)
      case Or(left, right) => // TODO: Support
        emptyBoundSeq
      case Not(condition) => // TODO: Support
        emptyBoundSeq
      case EqualTo(field, value) =>
        if (field == "__time") {
          Seq(
            (LOWER, value.asInstanceOf[Long]),
            (UPPER, value.asInstanceOf[Long])
          )
        } else {
          emptyBoundSeq
        }
      case LessThan(field, value) =>
        if (field == "__time") {
          Seq((UPPER, value.asInstanceOf[Long] - 1))
        } else {
          emptyBoundSeq
        }
      case LessThanOrEqual(field, value) =>
        if (field == "__time") {
          Seq((UPPER, value.asInstanceOf[Long]))
        } else {
          emptyBoundSeq
        }
      case GreaterThan(field, value) =>
        if (field == "__time") {
          Seq((LOWER, value.asInstanceOf[Long] + 1))
        } else {
          emptyBoundSeq
        }
      case GreaterThanOrEqual(field, value) =>
        if (field == "__time") {
          Seq((LOWER, value.asInstanceOf[Long]))
        } else {
          emptyBoundSeq
        }
      case _ => emptyBoundSeq
    }
  }

  private[v2] sealed trait Bound
  case object LOWER extends Bound
  case object UPPER extends Bound
}
