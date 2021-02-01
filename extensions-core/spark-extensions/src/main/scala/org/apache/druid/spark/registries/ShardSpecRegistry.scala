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
import org.apache.druid.spark.MAPPER
import org.apache.druid.timeline.partition.{HashBasedNumberedShardSpec, HashPartitionFunction,
  LinearShardSpec, NumberedShardSpec, ShardSpec}

import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.collection.mutable

object ShardSpecRegistry extends Logging {
  private val registeredShardSpecCreationFunctions: mutable.HashMap[String,
    Map[String, String] => ShardSpec] =  new mutable.HashMap()

  def register(
                shardSpecType: String,
                shardSpecCreationFunc: Map[String, String] => ShardSpec
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
                       shardSpecProperties: Map[String, String]
                     ): ShardSpec = {
    if (!registeredShardSpecCreationFunctions.contains(shardSpecType)) {
      if (knownTypes.keySet.contains(shardSpecType)) {
        registerByType(shardSpecType)
      } else {
        throw new IAE("No registered shard spec creator function for shard spec type %s",
          shardSpecType)
      }
    }
    registeredShardSpecCreationFunctions(shardSpecType)(shardSpecProperties)
  }

  private val knownTypes: Map[String, Map[String, String] => ShardSpec] =
    Map[String, Map[String, String] => ShardSpec](
      "hashed" -> ((
                   properties: Map[String, String]) =>
        // TODO: Move these property keys to a utils class
        new HashBasedNumberedShardSpec(
          properties("partitionId").toInt,
          properties.get("numPartitions").map(_.toInt).getOrElse(1),
          properties.get("bucketId").map(Integer.decode).orNull,
          properties.get("numBuckets").map(Integer.decode).orNull,
          properties.get("partitionDimensions").map(_.split(",").toList.asJava).orNull,
          properties.get("hashPartitionFunction").map(HashPartitionFunction.fromString).orNull,
          MAPPER)),
      "linear" -> ((properties: Map[String, String]) =>
        new LinearShardSpec(properties("partitionId").toInt)),
      "numbered" -> ((properties: Map[String, String]) =>
        new NumberedShardSpec(
          properties("partitionId").toInt,
          properties.get("numPartitions").map(_.toInt).getOrElse(1))
        )
    )
}
