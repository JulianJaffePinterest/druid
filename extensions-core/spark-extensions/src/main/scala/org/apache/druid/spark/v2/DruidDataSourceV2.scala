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

import java.util.Optional

import com.fasterxml.jackson.databind.{InjectableValues, ObjectMapper}
import org.apache.druid.jackson.DefaultObjectMapper
import org.apache.druid.math.expr.ExprMacroTable
import org.apache.druid.query.expression.{LikeExprMacro, RegexpExtractExprMacro,
  TimestampCeilExprMacro, TimestampExtractExprMacro, TimestampFloorExprMacro,
  TimestampFormatExprMacro, TimestampParseExprMacro, TimestampShiftExprMacro, TrimExprMacro}
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory
import org.apache.druid.segment.{IndexIO, IndexMergerV9}
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters.seqAsJavaListConverter

class DruidDataSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport
  with DataSourceRegister with Logging {
  override def shortName(): String = "druid"

  override def createReader(dataSourceOptions: DataSourceOptions): DataSourceReader = {
    DruidDataSourceReader(dataSourceOptions)
  }

  override def createReader(schema: StructType,
                            dataSourceOptions: DataSourceOptions): DataSourceReader = {
    DruidDataSourceReader(schema, dataSourceOptions)
  }

  /**
    * Create a writer to save a dataframe as a Druid table. Spark knows the partitioning information
    * for the dataframe, but won't share. We also have only limited ways to detect issues, so for
    * now we'll need to trust that we're passed valid input. This means that data must already be
    * bucketed by segment granularity (e.g. each partition must contain rows from exactly one
    * segment interval), and that each Spark partition will be written as one segment, regardless of
    * the number of rows in the segment. Something like the DateBucketAndHashPartitioner from the
    * druid-spark-batch GitHub project can be used to ensure that all partitions have only one time
    * bucket they're responsible for while also setting a soft upper bound on the maximum size of
    * a segment. Longer term, we can write multiple segments per DruidDataWriter, which would mean
    * data wouldn't be dropped if a partition contained rows from more than one segment interval,
    * but does mean that almost all users of this writer will write out severely non-optimal
    * segments. This also doesn't solve the problem below :/
    *
    * Additionally, while the caller knows how many partitions there are total for each segment, we
    * don't, and so the caller will need to provide the total number of partitions if necessary for
    * the desired shard spec (e.g. Numbered or HashedNumbered). Otherwise, we'll have to default to
    * a partition count of 1 in the shard spec, meaning that Numbered will degrade to Linear and
    * users won't have atomic loading of segments within an interval.
    *
    * TODO: This may actually be bigger problem. Partition ids may not be contiguous for all segments
    *  in a bucket unless callers are meticulous about partitioning, in which case almost all shard
    *  specs will consider the output incomplete.
    *
    * @param uuid
    * @param schema
    * @param saveMode
    * @param dataSourceOptions
    * @return
    */
  override def createWriter(uuid: String,
                            schema: StructType,
                            saveMode: SaveMode,
                            dataSourceOptions: DataSourceOptions): Optional[DataSourceWriter] = {
    // Spark knows the partitioning information for the df, but it won't tell us. We also have very
    // limited ways to detect issues, so for now we'll need to trust that we're passed
    // TODO: Take advantage of the job id being provided (uuid in the args list)
    DruidDataSourceWriter(schema, saveMode, dataSourceOptions)
  }
}

object DruidDataSourceV2 {
  val MAPPER: ObjectMapper = new DefaultObjectMapper()

  private val injectableValues: InjectableValues =
    new InjectableValues.Std()
      .addValue(classOf[ExprMacroTable], new ExprMacroTable(Seq(
        new LikeExprMacro(),
        new RegexpExtractExprMacro(),
        new TimestampCeilExprMacro(),
        new TimestampExtractExprMacro(),
        new TimestampFormatExprMacro(),
        new TimestampParseExprMacro(),
        new TimestampShiftExprMacro(),
        new TimestampFloorExprMacro(),
        new TrimExprMacro.BothTrimExprMacro(),
        new TrimExprMacro.LeftTrimExprMacro(),
        new TrimExprMacro.RightTrimExprMacro()).asJava))
      .addValue(classOf[ObjectMapper], MAPPER)
      .addValue(classOf[DataSegment.PruneSpecsHolder], PruneSpecsHolder.DEFAULT)

  MAPPER.setInjectableValues(injectableValues)

  val INDEX_IO = new IndexIO(
    DruidDataSourceV2.MAPPER,
    () => 1000000
  )

  val INDEX_MERGER_V9 = new IndexMergerV9(
    DruidDataSourceV2.MAPPER,
    INDEX_IO,
    OnHeapMemorySegmentWriteOutMediumFactory.instance()
  )
}
