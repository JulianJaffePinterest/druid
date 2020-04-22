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
import org.apache.druid.java.util.common.ISE
import org.apache.druid.math.expr.ExprMacroTable
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule
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

import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

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

  override def createWriter(uuid: String,
                            schema: StructType,
                            saveMode: SaveMode,
                            dataSourceOptions: DataSourceOptions): Optional[DataSourceWriter] = {
    assert(dataSourceOptions.tableName().isPresent,
      s"Must set ${DataSourceOptions.TABLE_KEY}!")
    val dataSource = dataSourceOptions.tableName().get()
    // TODO: Actually check if the specified data source already exists
    val dataSourceExists = false
    saveMode match {
      case SaveMode.Append => if (dataSourceExists) {
        throw new UnsupportedOperationException(
          "Druid does not support appending to existing dataSources, only reindexing!"
        )
      } else {
        createDataSourceWriterOptional(schema, dataSourceOptions)
      }
      case SaveMode.ErrorIfExists => throw new ISE(s"$dataSource already exists!")
      case SaveMode.Ignore => if (dataSourceExists) {
        Optional.empty[DataSourceWriter]
      } else {
        createDataSourceWriterOptional(schema, dataSourceOptions)
      }
      case SaveMode.Overwrite => createDataSourceWriterOptional(schema, dataSourceOptions)
    }
  }

  private[v2] def createDataSourceWriterOptional(
                                                  schema: StructType,
                                                  dataSourceOptions: DataSourceOptions
                                                ): Optional[DataSourceWriter] = {
    Optional.of[DataSourceWriter](new DruidDataSourceWriter)
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

  private val jacksonModules = Seq(new SketchModule)

  MAPPER.setInjectableValues(injectableValues)
  MAPPER.registerModules(jacksonModules.flatMap(_.getJacksonModules.asScala.toList).asJava)

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
