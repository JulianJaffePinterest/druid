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

import org.apache.druid.java.util.common.Intervals
import org.apache.druid.spark.utils.{DruidDataSourceOptionKeys, DruidDataWriterConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters.mapAsScalaMapConverter

class DruidDataWriterFactory(
                              schema: StructType,
                              dataSourceOptions: DataSourceOptions
                            ) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long):
  DataWriter[InternalRow] = {
    // Construct a DataSchema from the class args
    new DruidDataWriter(
      new DruidDataWriterConfig(
        dataSourceOptions.tableName().get,
        partitionId,
        -1, // Frustratingly, Spark knows how many partitions there are but doesn't tell us
        schema,
        "",
        Intervals.ETERNITY, // TODO: This needs to be passed in or constructed (segment interval)
        "",
        dataSourceOptions.getBoolean(DruidDataSourceOptionKeys.rollUpSegmentsKey, false),
        dataSourceOptions.getInt(DruidDataSourceOptionKeys.rowsPerPersistKey, 2000000),
        dataSourceOptions.get(DruidDataSourceOptionKeys.deepStorageTypeKey).orElse("local"),
        Map[String, AnyRef](),
        dataSourceOptions.asMap.asScala.toMap
      )
    )
  }
}
