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

package org.apache.druid.spark.utils

import org.apache.druid.java.util.common.DateTimes
import org.apache.spark.sql.types.StructType

class DruidDataWriterConfig(
                             val dataSource: String,
                             val partitionId: Int,
                             val partitionsCount: Int,
                             val schema: StructType,
                             val dataSchemaSerialized: String,
                             val shardSpecSerialized: String,
                             val rowsPerPersist: Int,
                             val deepStorageType: String,
                             val deepStorageProperties: Map[String, AnyRef],
                             val properties: Map[String, String],
                             version: Option[String] = None
                           ) extends Serializable {
  def getVersion: String = {
    version.getOrElse(DateTimes.nowUtc().toString)
  }
}

object DruidDataWriterConfig {
  //
  val partitionDimensionsKey: String = "partitionDimensions"

  // IndexSpec keys
  val bitmapTypeKey: String = "bitmapType"
  val bitmapTypeCompressOnSerializationKey: String = "true"
  val dimensionCompressionKey: String = "dimensionCompression"
  val metricCompressionKey: String = "metricCompression"
  val longEncodingKey: String = "longEncoding"
}
