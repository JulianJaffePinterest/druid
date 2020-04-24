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

import java.util.Properties

import org.apache.druid.segment.loading.DataSegmentKiller
import org.apache.druid.spark.registries.SegmentWriterRegistry
import org.apache.druid.spark.utils.{DruidDataSourceOptionKeys, DruidMetadataClient}
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters.seqAsJavaListConverter

class DruidDataSourceWriter(
                             schema: StructType,
                             dataSourceOptions: DataSourceOptions
                           ) extends DataSourceWriter {
  private lazy val metadataClient = new DruidMetadataClient(
    "",
    "",
    -1,
    "",
    "",
    "",
    new Properties()
  )

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new DruidDataWriterFactory(schema, dataSourceOptions)
  }

  // Push segment locations (from commit messages) to metadata
  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    val segments =
      writerCommitMessages.flatMap(_.asInstanceOf[DruidWriterCommitMessage].serializedSegments)
    metadataClient.publishSegments(
      segments.map(DruidDataSourceV2.MAPPER.readValue(_, classOf[DataSegment])).toList.asJava,
      DruidDataSourceV2.MAPPER)
  }

  // Clean up segments in deep storage but not in metadata
  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    val segmentKiller: DataSegmentKiller = SegmentWriterRegistry.getSegmentKiller(
      dataSourceOptions.get(DruidDataSourceOptionKeys.deepStorageTypeKey).orElse("local"),
      dataSourceOptions
    )

    val segments =
      writerCommitMessages.flatMap(_.asInstanceOf[DruidWriterCommitMessage].serializedSegments)
    segments.foreach(segment =>
      segmentKiller.killQuietly(DruidDataSourceV2.MAPPER.readValue(segment, classOf[DataSegment])))
  }
}
