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

import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling
import org.apache.druid.data.input.impl.{DimensionSchema, DimensionsSpec, DoubleDimensionSchema,
  FloatDimensionSchema, LongDimensionSchema, StringDimensionSchema, TimestampSpec}
import org.apache.druid.java.util.common.IAE
import org.apache.druid.segment.indexing.DataSchema
import org.apache.druid.spark.utils.{DruidDataSourceOptionKeys, DruidDataWriterConfig}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}
import org.apache.spark.sql.types.{ArrayType, DoubleType, FloatType, IntegerType, LongType,
  StringType, StructType}

import scala.collection.JavaConverters.{mapAsScalaMapConverter, seqAsJavaListConverter}

class DruidDataWriterFactory(
                              schema: StructType,
                              dataSourceOptions: DataSourceOptions
                            ) extends DataWriterFactory[InternalRow] {
  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long):
  DataWriter[InternalRow] = {
    // We validated in DruidDataSourceWriter that either "dimensions" or "metrics" was set
    val dimensionsArr = DruidDataWriterFactory.scalify(
      dataSourceOptions
        .get(DruidDataSourceOptionKeys.dimensionsKey)
    )
      .map(_.split(','))
      .getOrElse(Array.empty[String])
    val metricsArr = DruidDataWriterFactory.scalify(
      dataSourceOptions
        .get(DruidDataSourceOptionKeys.metricsKey)
    )
      .map(_.split(','))
      .getOrElse(Array.empty[String])
    val excludedDimensions = DruidDataWriterFactory.scalify(
      dataSourceOptions
        .get(DruidDataSourceOptionKeys.excludedDimensionsKey)
    )
      .map(_.split(','))
      .getOrElse(Array.empty[String]).toSeq

    val dimensions = if (dimensionsArr.isEmpty) {
      schema.fieldNames.filterNot(metricsArr.contains(_))
    } else {
      dimensionsArr
    }
    val metrics = if (metricsArr.isEmpty) {
      schema.fieldNames.filterNot((dimensionsArr ++ excludedDimensions).contains(_))
    } else {
      metricsArr
    }
    /*

      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("timestampSpec") @Nullable TimestampSpec timestampSpec, // can be null in old task spec
      @JsonProperty("dimensionsSpec") @Nullable DimensionsSpec dimensionsSpec, // can be null in old task spec
      @JsonProperty("metricsSpec") AggregatorFactory[] aggregators,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      @JsonProperty("transformSpec") TransformSpec transformSpec,
      @Deprecated @JsonProperty("parser") @Nullable Map<String, Object> parserMap,
      @JacksonInject ObjectMapper objectMapper
     */
    val dataSchema = new DataSchema(
      dataSourceOptions.tableName().get(),
      new TimestampSpec(
        dataSourceOptions.get(DruidDataSourceOptionKeys.timestampColumnKey).orElse("ts"),
        dataSourceOptions.get(DruidDataSourceOptionKeys.timestampFormatKey).orElse("auto"),
        null // scalastyle:ignore null
      ),
      new DimensionsSpec(
        DruidDataWriterFactory.convertStructTypeToDruidDimensionSchema(
          dimensions,
          schema
        ).asJava,
        excludedDimensions.asJava,
        null // scalastyle:ignore null
      ),
      null,
      null,
      null,
      null,
      DruidDataSourceV2.MAPPER
    )
    new DruidDataWriter(
      new DruidDataWriterConfig(
        dataSourceOptions.tableName().get,
        partitionId,
        -1, // Frustratingly, Spark knows how many partitions there are but doesn't tell us
        schema,
        DruidDataSourceV2.MAPPER.writeValueAsString(dataSchema),
        "",
        dataSourceOptions.getInt(DruidDataSourceOptionKeys.rowsPerPersistKey, 2000000),
        dataSourceOptions.get(DruidDataSourceOptionKeys.deepStorageTypeKey).orElse("local"),
        Map[String, AnyRef](),
        dataSourceOptions.asMap.asScala.toMap
      )
    )
  }
}

object DruidDataWriterFactory {
  def convertStructTypeToDruidDimensionSchema(
                                               dimensions: Seq[String],
                                               schema: StructType
                                             ): Seq[DimensionSchema] = {
    schema
      .filter(field => dimensions.contains(field.name))
      .map(field =>
        field.dataType match {
          case LongType | IntegerType => new LongDimensionSchema(field.name)
          case FloatType => new FloatDimensionSchema(field.name)
          case DoubleType => new DoubleDimensionSchema(field.name)
          case StringType | ArrayType(StringType, false) => new StringDimensionSchema(
            field.name,
            MultiValueHandling.SORTED_ARRAY, // TODO: Make this configurable
            true // TODO: Make this configurable
          )
          case _ => throw new IAE(
            "Unsure how to create dimension from column [%s] with data type [%s]",
            field.name,
            field.dataType
          )
        }
      )
  }

  // Needed to work around Java Function's type invariance
  def scalify[T](javaOptional: Optional[T]): Option[T] = {
    if (javaOptional.isPresent) Some(javaOptional.get()) else None
  }
}
