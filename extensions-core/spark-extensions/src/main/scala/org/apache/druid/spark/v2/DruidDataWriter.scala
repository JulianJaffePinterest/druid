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
import org.apache.druid.segment.data.{BitmapSerdeFactory, CompressionFactory, CompressionStrategy,
  ConciseBitmapSerdeFactory, RoaringBitmapSerdeFactory}
import org.apache.druid.segment.{IndexIO, IndexMergerV9, IndexSpec, IndexableAdapter,
  QueryableIndexIndexableAdapter}
import org.apache.druid.segment.incremental.{IncrementalIndex, IncrementalIndexSchema}
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.segment.loading.DataSegmentPusher
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory
import org.apache.druid.spark.registries.{SegmentWriterRegistry, ShardSpecRegistry}
import org.apache.druid.spark.utils.DruidDataWriterConfig
import org.apache.druid.timeline.DataSegment
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter,
  seqAsJavaListConverter}
import scala.collection.mutable.ArrayBuffer

class DruidDataWriter(config: DruidDataWriterConfig) extends DataWriter[InternalRow] {
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
    DruidDataSourceV2.MAPPER.readValue[DataSchema](config.dataSchemaSerialized,
      new TypeReference[DataSchema] {})
  private val dimensions: JList[String] =
    dataSchema.getDimensionsSpec.getDimensions.asScala.map(_.getName).asJava
  private val partitionDimensions: Option[List[String]] = config
    .properties.get(DruidDataWriterConfig.partitionDimensionsKey).map(_.split(',').toList)
  private val tsColumn: String = dataSchema.getTimestampSpec.getTimestampColumn
  private val tsColumnIndex = config.schema.indexOf(tsColumn)
  private val timestampParser = TimestampParser
    .createObjectTimestampParser(dataSchema.getTimestampSpec.getTimestampFormat)

  private val finalVersion = config.getVersion

  private val indexSpec: IndexSpec = new IndexSpec(
    DruidDataWriter.getBitmapSerde(
      config.properties.getOrElse(
        DruidDataWriterConfig.bitmapTypeKey,
        DruidDataWriter.defaultBitmapType
      ),
      config.properties.get(DruidDataWriterConfig.bitmapTypeCompressOnSerializationKey)
        .map(_.toBoolean)
        .getOrElse(RoaringBitmapSerdeFactory.DEFAULT_COMPRESS_RUN_ON_SERIALIZATION)
    ),
    DruidDataWriter.getCompressionStrategy(
      config.properties.getOrElse(
        DruidDataWriterConfig.dimensionCompressionKey,
        CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY.toString
      )
    ),
    DruidDataWriter.getCompressionStrategy(
      config.properties.getOrElse(
        DruidDataWriterConfig.metricCompressionKey,
        CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY.toString
      )
    ),
    DruidDataWriter.getLongEncodingStrategy(
      config.properties.getOrElse(
        DruidDataWriterConfig.longEncodingKey,
        CompressionFactory.DEFAULT_LONG_ENCODING_STRATEGY.toString
      )
    )
  )

  private val pusher: DataSegmentPusher = SegmentWriterRegistry.getSegmentPusher(
    config.deepStorageType, config.deepStorageProperties
  )

  // TODO: rewrite this without using IncrementalIndex, because IncrementalIndex bears a lot of overhead
  //  to support concurrent querying, that is not needed in Spark
  private var incrementalIndex: IncrementalIndex[_] = createInterval()
  private val adapters: ArrayBuffer[IndexableAdapter] = ArrayBuffer.empty[IndexableAdapter]
  private var rowsProcessed: Long = 0

  override def write(row: InternalRow): Unit = {
    // TODO: We need to create a map from time bucket to Seq[Adapter] and create segments per time
    //  bucket instead of assuming all rows fall within one segment. We also need to create multiple
    //  segments per bucket based on a max rows per segment parameter. Since we don't know how many
    //  rows we have, we'll have to fill each bucket one by one and use the final bucket for "slop"
    // Check index, flush if too many rows in memory and recreate
    if (rowsProcessed == config.rowsPerPersist) {
      adapters += flushIndex(incrementalIndex)
      incrementalIndex.close()
      incrementalIndex = createInterval()
      rowsProcessed = 0
    }
    incrementalIndex.add(
      incrementalIndex.formatRow(
        new MapBasedInputRow(
          timestampParser
            .apply(row.get(tsColumnIndex, config.schema(tsColumnIndex).dataType)).getMillis,
          dimensions,
          // Convert to Java types that Druid knows how to handle
          config.schema
            .map(field => field.name -> row.get(config.schema.indexOf(field), field.dataType)).toMap
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
          .withMinTimestamp(config.segmentInterval.getStartMillis)
          .withRollup(config.rollup)
          .build()
      )
      .setMaxRowCount(config.rowsPerPersist)
      .buildOnheap()
  }

  private def flushIndex(index: IncrementalIndex[_]): IndexableAdapter = {
    new QueryableIndexIndexableAdapter(
      closer.register(
        DruidDataWriter.INDEX_IO.loadIndex(
          DruidDataWriter.INDEX_MERGER_V9
            .persist(
              index,
              config.segmentInterval,
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
      val shardSpec = ShardSpecRegistry.createShardSpec(
        config.shardSpecSerialized,
        config.partitionId,
        config.partitionsCount,
        partitionDimensions)
      val dataSegmentTemplate = new DataSegment(
        config.dataSource,
        config.segmentInterval,
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

  private val defaultBitmapType: String = "roaring"

  def getBitmapSerde(serde: String, compressRunOnSerialization: Boolean): BitmapSerdeFactory = {
    if (serde == "concise") {
      new ConciseBitmapSerdeFactory
    } else {
      new RoaringBitmapSerdeFactory(compressRunOnSerialization)
    }
  }

  def getCompressionStrategy(strategy: String): CompressionStrategy = {
    if (CompressionStrategy.values().contains(strategy)) {
      CompressionStrategy.valueOf(strategy)
    } else {
      CompressionStrategy.DEFAULT_COMPRESSION_STRATEGY
    }
  }

  def getLongEncodingStrategy(strategy: String): CompressionFactory.LongEncodingStrategy = {
    if (CompressionFactory.LongEncodingStrategy.values().contains(strategy)) {
      CompressionFactory.LongEncodingStrategy.valueOf(strategy)
    } else {
      CompressionFactory.DEFAULT_LONG_ENCODING_STRATEGY
    }
  }
}
