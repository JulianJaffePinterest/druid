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

package org.apache.druid.spark.registries

import org.apache.druid.java.util.common.IAE
import org.apache.druid.spark.utils.Logging
import org.apache.druid.spark.v2.DruidDataSourceV2
import org.apache.druid.timeline.partition.{HashBasedNumberedShardSpec, LinearShardSpec,
  NumberedShardSpec, ShardSpec}

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable

object ShardSpecRegistry extends Logging {
  private val registeredShardSpecCreationFunctions: mutable.HashMap[String,
    (Int, Int, Option[List[String]]) => ShardSpec] =  new mutable.HashMap()

  def register(
                shardSpecType: String,
               shardSpecCreationFunc: (Int, Int, Option[List[String]]) => ShardSpec
              ): Unit = {
    registeredShardSpecCreationFunctions(shardSpecType) = shardSpecCreationFunc
  }

  def registerByType(shardSpecType: String): Unit = {
    if (!registeredShardSpecCreationFunctions.contains(shardSpecType)
      && knownTypes.contains(shardSpecType)) {
      register(shardSpecType, knownTypes(shardSpecType))
    }
  }

  def createShardSpec(
                       shardSpecType: String,
                       partitionNum: Int,
                       partitionsCount: Int,
                       partitionDimensions: Option[List[String]]
                     ): ShardSpec = {
    if (!registeredShardSpecCreationFunctions.contains(shardSpecType)) {
      if (knownTypes.keySet.contains(shardSpecType)) {
        registerByType(shardSpecType)
      } else {
        throw new IAE("No registered shard spec creator function for shard spec type %s",
          shardSpecType)
      }
    }
    registeredShardSpecCreationFunctions(shardSpecType)(partitionNum, partitionsCount, partitionDimensions)
  }

  private val knownTypes: Map[String, (Int, Int, Option[List[String]]) => ShardSpec] =
    Map[String, (Int, Int, Option[List[String]]) => ShardSpec](
      "hashed" -> ((
                     partitionId: Int,
                     partitionsCount: Int,
                     partitionDimensions: Option[List[String]]) =>
        new HashBasedNumberedShardSpec(
          partitionId,
          partitionsCount,
          partitionDimensions.map(_.asJava).orNull,
        DruidDataSourceV2.MAPPER)),
      "linear" -> ((partitionId: Int, _: Int, _: Option[List[String]]) =>
        new LinearShardSpec(partitionId)),
      "numbered" -> ((partitionId: Int, partitionsCount: Int, _: Option[List[String]]) =>
        new NumberedShardSpec(partitionId, partitionsCount))
    )
}
