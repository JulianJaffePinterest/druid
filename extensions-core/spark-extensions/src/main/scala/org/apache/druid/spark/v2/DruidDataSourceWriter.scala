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

import org.apache.druid.java.util.common.ISE
import org.apache.druid.segment.loading.DataSegmentKiller
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.registries.SegmentWriterRegistry
import org.apache.druid.spark.utils.{DruidDataSourceOptionKeys, DruidMetadataClient, Logging}
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory,
  WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters.seqAsJavaListConverter

/**
  * A DruidDataSourceWriter orchestrates writing a dataframe to Druid.
  *
  * @param schema The schema of the dataframe to be written to Druid.
  * @param dataSourceOptions A set of options to configure the DruidDataWriterFactories,
  *                          DruidDataWriters, and clients used to write a dataframe to Druid
  * @param metadataClient The client to use to read from and write to a Druid metadata server.
  */
class DruidDataSourceWriter(
                             schema: StructType,
                             dataSourceOptions: DataSourceOptions,
                             metadataClient: DruidMetadataClient
                           ) extends DataSourceWriter with Logging {

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new DruidDataWriterFactory(schema, dataSourceOptions)
  }

  /**
    * Commit the segment locations listed in WRITERCOMMITMESSAGES to a Druid metadata server.
    *
    * @param writerCommitMessages An array of messages containing sequences of serialized
    *                             DataSegments written to deep storage.
    */
  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    val segments =
      writerCommitMessages.flatMap(_.asInstanceOf[DruidWriterCommitMessage].serializedSegments)

    logInfo(s"Committing the following segments: ${segments.mkString(", ")}")

    metadataClient.publishSegments(
      segments.map(MAPPER.readValue(_, classOf[DataSegment])).toList.asJava, MAPPER)
  }

  // Clean up segments in deep storage but not in metadata
  /**
    * Clean up a failed write by deleting any segments already written to deep storage.
    *
    * @param writerCommitMessages An array of messages containing sequences of serialized
    *                             DataSegments that have already been written to deep storage.
    */
  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    val segments =
      writerCommitMessages.flatMap(_.asInstanceOf[DruidWriterCommitMessage].serializedSegments)

    logWarn(s"Aborting the following commits: ${segments.mkString(", ")}")

    val segmentKiller: DataSegmentKiller = SegmentWriterRegistry.getSegmentKiller(
      dataSourceOptions.get(DruidDataSourceOptionKeys.deepStorageTypeKey).orElse("local"),
      dataSourceOptions
    )
    segments.foreach(segment =>
      segmentKiller.killQuietly(MAPPER.readValue(segment, classOf[DataSegment])))
  }
}

object DruidDataSourceWriter {
  def apply(
             schema: StructType,
             saveMode: SaveMode,
             dataSourceOptions: DataSourceOptions
           ): Optional[DataSourceWriter] = {
    val dataSource = dataSourceOptions.tableName().get()
    val metadataClient = DruidMetadataClient(dataSourceOptions)
    val dataSourceExists = metadataClient.checkIfDataSourceExists(dataSource)
    saveMode match {
      case SaveMode.Append => if (dataSourceExists) {
        // In theory, if a caller provided an interval in the data source options, we could check
        // to see if the interval already existed or not, and only throw an error if the data source
        // already had data for the given interval. We'd want to also plumb that interval through to
        // DruidDataWriter instances to make sure they dropped rows outside of the interval.
        throw new UnsupportedOperationException(
          "Druid does not support appending to existing dataSources, only reindexing!"
        )
      } else {
        createDataSourceWriterOptional(schema, dataSourceOptions, metadataClient)
      }
      case SaveMode.ErrorIfExists => if (dataSourceExists) {
        throw new ISE(s"$dataSource already exists!")
      } else {
        createDataSourceWriterOptional(schema, dataSourceOptions, metadataClient)
      }
      case SaveMode.Ignore => if (dataSourceExists) {
        Optional.empty[DataSourceWriter]
      } else {
        createDataSourceWriterOptional(schema, dataSourceOptions, metadataClient)
      }
      case SaveMode.Overwrite =>
        createDataSourceWriterOptional(schema, dataSourceOptions, metadataClient)
    }
  }

  private[v2] def createDataSourceWriterOptional(
                                                  schema: StructType,
                                                  dataSourceOptions: DataSourceOptions,
                                                  metadataClient: DruidMetadataClient
                                                ): Optional[DataSourceWriter] = {
    Optional.of[DataSourceWriter](new DruidDataSourceWriter(
      schema, dataSourceOptions, metadataClient)
    )
  }

  private[v2] def validateDataSourceOption(dataSourceOptions: DataSourceOptions): Unit = {
    assert(dataSourceOptions.tableName().isPresent,
      s"Must set ${DataSourceOptions.TABLE_KEY}!")
    // TODO: default to derby?
    assert(dataSourceOptions.get(DruidDataSourceOptionKeys.metadataDbTypeKey).isPresent,
      s"Must set ${DruidDataSourceOptionKeys.metadataDbTypeKey}!"
    )
  }
}
