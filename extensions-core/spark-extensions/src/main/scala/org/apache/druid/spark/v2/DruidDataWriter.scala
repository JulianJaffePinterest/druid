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


import java.io.Closeable
import java.nio.file.Files
import java.util.{List => JList}

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.commons.io.FileUtils
import org.apache.druid.data.input.MapBasedInputRow
import org.apache.druid.java.util.common.io.Closer
import org.apache.druid.java.util.common.parsers.TimestampParser
import org.apache.druid.segment.{IndexIO, IndexMergerV9, IndexSpec, IndexableAdapter,
  QueryableIndexIndexableAdapter}
import org.apache.druid.segment.incremental.{IncrementalIndex, IncrementalIndexSchema}
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.segment.loading.DataSegmentPusher
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.partition.{HashBasedNumberedShardSpec, LinearShardSpec,
  NumberedShardSpec}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.joda.time.chrono.ISOChronology
import org.joda.time.{DateTime, Interval}

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter,
  seqAsJavaListConverter}
import scala.collection.mutable.ArrayBuffer

class DruidDataWriter(
                       dataSource: String,
                       partitionId: Int,
                       partitionsCount: Int,
                       schema: StructType,
                       dataSchemaSerialized: String,
                       segmentInterval: Interval,
                       shardSpecSerialized: String,
                       rollup: Boolean,
                       rowsPerPersist: Int,
                       rowsPerSegment: Long,
                       version: Option[String] = None
                     ) extends DataWriter[InternalRow] {
  private val tmpPersistDir = Files.createTempDirectory("persist").toFile
  private val tmpMergeDir = Files.createTempDirectory("merge").toFile
  private val closer = Closer.create()
  closer.register(
    new Closeable {
      override def close(): Unit = {
        FileUtils.deleteDirectory(tmpMergeDir)
        FileUtils.deleteDirectory(tmpPersistDir)
      }
    }
  )

  private val dataSchema: DataSchema =
    DruidDataSourceV2.MAPPER.readValue[DataSchema](dataSchemaSerialized,
      new TypeReference[DataSchema] {})
  private val dimensions: JList[String] =
    dataSchema.getDimensionsSpec.getDimensions.asScala.map(_.getName).asJava
  private val tsColumn: String = dataSchema.getTimestampSpec.getTimestampColumn
  private val tsColumnIndex = schema.indexOf(tsColumn)
  private val timestampParser = TimestampParser
    .createNumericTimestampParser(dataSchema.getTimestampSpec.getTimestampFormat)

  private val finalVersion = version.getOrElse(DateTime.now(ISOChronology.getInstanceUTC).toString)

  private val indexSpec: IndexSpec = new IndexSpec() // TODO: Allow configuring the IndexSpec

  // TODO: Create appropriate pusher for deep storage (may need to create on driver e.g. factory and serialize)
  private val pusher: DataSegmentPusher = null

  // TODO: rewrite this without using IncrementalIndex, because IncrementalIndex bears a lot of overhead
  //  to support concurrent querying, that is not needed in Spark
  private var incrementalIndex: IncrementalIndex[_] = createInterval()
  private val adapters: ArrayBuffer[IndexableAdapter] = ArrayBuffer.empty[IndexableAdapter]
  private var rowsProcessed: Long = 0

  override def write(row: InternalRow): Unit = {
    // Check index, flush if too many rows in memory and recreate
    if (rowsProcessed == rowsPerSegment) {
      adapters += flushIndex(incrementalIndex)
      incrementalIndex.close()
      incrementalIndex = createInterval()
      rowsProcessed = 0
    }
    incrementalIndex.add(
      incrementalIndex.formatRow(
        new MapBasedInputRow(
          // TODO: Support DateTime timestamps
          timestampParser.apply(row.getLong(tsColumnIndex)).getMillis,
          dimensions,
          // Convert to Java types that Druid knows how to handle
          schema
            .map(field => field.name -> row.get(schema.indexOf(field), field.dataType)).toMap
            .mapValues {
              case traversable: Traversable[_] => traversable.toSeq.asJava
              case x => x
            }.asJava
        )
      )
    )
    rowsProcessed += 1
  }

  private[v2] def createInterval(): IncrementalIndex[_] = {
    new IncrementalIndex.Builder()
      .setIndexSchema(
        new IncrementalIndexSchema.Builder()
          .withDimensionsSpec(dataSchema.getDimensionsSpec)
          .withQueryGranularity(
            dataSchema.getGranularitySpec.getQueryGranularity
          )
          .withMetrics(dataSchema.getAggregators: _*)
          .withTimestampSpec(dataSchema.getTimestampSpec)
          .withMinTimestamp(segmentInterval.getStartMillis)
          .withRollup(rollup)
          .build()
      )
      .setMaxRowCount(rowsPerPersist)
      .buildOnheap()
  }

  private def flushIndex(index: IncrementalIndex[_]): IndexableAdapter = {
    new QueryableIndexIndexableAdapter(
      closer.register(
        DruidDataWriter.INDEX_IO.loadIndex(
          DruidDataWriter.INDEX_MERGER_V9
            .persist(
              index,
              segmentInterval,
              tmpPersistDir,
              indexSpec,
              OnHeapMemorySegmentWriteOutMediumFactory.instance()
            )
        )
      )
    )
  }

  override def commit(): WriterCommitMessage = {
    // Return segment locations on deep storage
    if (rowsProcessed > 0) {
      adapters += flushIndex(incrementalIndex)
      incrementalIndex.close()
    }
    val specs = if (adapters.nonEmpty) {
      val finalStaticIndexer = DruidDataWriter.INDEX_MERGER_V9
      val file = finalStaticIndexer.merge(
        adapters.asJava,
        true,
        dataSchema.getAggregators,
        tmpMergeDir,
        indexSpec
      )
      val allDimensions: JList[String] = adapters
        .map(_.getDimensionNames)
        .foldLeft(Set[String]())(_ ++ _.asScala)
        .toList
        .asJava
      val shardSpec = shardSpecSerialized match {
        // TODO: Support additional shard specs
        case "hashed" =>
          new HashBasedNumberedShardSpec(partitionId,
            partitionsCount,
            null, // TODO: This should be passed in
            DruidDataSourceV2.MAPPER)
        case "linear" => new LinearShardSpec(partitionId)
        case "numbered" => new NumberedShardSpec(partitionId, partitionsCount)
        case _ =>
          throw new IllegalArgumentException("Unrecognized shard spec type " + shardSpecSerialized + "!")
      }
      val dataSegmentTemplate = new DataSegment(
        dataSource,
        segmentInterval,
        finalVersion,
        null,
        allDimensions,
        dataSchema.getAggregators.map(_.getName).toList.asJava,
        shardSpec,
        -1,
        -1L
      )
      val finalDataSegment = pusher.push(file, dataSegmentTemplate, true)
      Seq(DruidDataSourceV2.MAPPER.writeValueAsString(finalDataSegment))
    } else {
      Seq.empty
    }
    DruidWriterCommitMessage(specs)
  }

  override def abort(): Unit = {
    closer.close()
  }
}

object DruidDataWriter {
  private val INDEX_IO = new IndexIO(
    DruidDataSourceV2.MAPPER,
    () => 1000000
  )

  private val INDEX_MERGER_V9 = new IndexMergerV9(
    DruidDataSourceV2.MAPPER,
    INDEX_IO,
    // TODO: Make the segment write out medium configurable
    OnHeapMemorySegmentWriteOutMediumFactory.instance()
  )
}
