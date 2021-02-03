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

import com.fasterxml.jackson.databind.introspect.AnnotatedClassResolver
import org.apache.druid.java.util.common.IAE
import org.apache.druid.spark.utils.Logging
import org.apache.druid.spark.MAPPER
import org.apache.druid.timeline.partition.{HashBasedNumberedShardSpec, HashPartitionFunction,
  LinearShardSpec, NumberedShardSpec, ShardSpec, SingleDimensionShardSpec}

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, seqAsJavaListConverter}
import scala.collection.mutable

object ShardSpecRegistry extends Logging {
  private val registeredShardSpecCreationFunctions: mutable.HashMap[String,
    Map[String, String] => ShardSpec] =  new mutable.HashMap()
  private val registeredShardSpecUpdateFunctions: mutable.HashMap[String, (ShardSpec, Int, Int) => ShardSpec] =
    new mutable.HashMap()
  private var shardSpecClassToTypeNameMap: Map[Class[_], String] = Map[Class[_], String]()

  def register(
                shardSpecType: String,
                shardSpecCreationFunc: Map[String, String] => ShardSpec,
                shardSpecUpdateFunc: (ShardSpec, Int, Int) => ShardSpec
              ): Unit = {
    registeredShardSpecCreationFunctions(shardSpecType) = shardSpecCreationFunc
    registeredShardSpecUpdateFunctions(shardSpecType) = shardSpecUpdateFunc
  }

  def registerByType(shardSpecType: String): Unit = {
    if (!registeredShardSpecCreationFunctions.contains(shardSpecType)
      && knownTypes.contains(shardSpecType)) {
      val funcs = knownTypes(shardSpecType)
      register(shardSpecType, funcs._1, funcs._2)
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

  def updateShardSpec(
                       baseShardSpec: ShardSpec,
                       partitionNum: Int,
                       numPartitions: Int
                     ): ShardSpec = {
    val shardSpecType = getShardSpecTypeFromClass(baseShardSpec)
    if (!registeredShardSpecUpdateFunctions.contains(shardSpecType)) {
      if (knownTypes.keySet.contains(shardSpecType)) {
        registerByType(shardSpecType)
      } else {
        throw new IAE("No registered shard spec updater function for shard spec type %s",
          shardSpecType)
      }
    }
    registeredShardSpecUpdateFunctions(shardSpecType)(baseShardSpec, partitionNum, numPartitions)
  }

  private[registries] def getShardSpecTypeFromClass(shardSpec: ShardSpec): String = {
    val clazz = shardSpec.getClass
    if (shardSpecClassToTypeNameMap.contains(clazz)) {
      shardSpecClassToTypeNameMap(clazz)
    } else {
      val introspector = MAPPER.getDeserializationConfig.getAnnotationIntrospector
      shardSpecClassToTypeNameMap = introspector.findSubtypes(
        AnnotatedClassResolver.resolveWithoutSuperTypes(MAPPER.getDeserializationConfig, classOf[ShardSpec])
      ).asScala.map(t => t.getType -> t.getName).toMap
      // Staying away from functional programming of exceptions for more mutual intelligibility with Java
      if (!shardSpecClassToTypeNameMap.contains(clazz)) {
        throw new IAE(s"Unable to determine appropriate subtype for ShardSpec class ${clazz.toString}!" +
          s" Has it been registered with Jackson?")
      }
      shardSpecClassToTypeNameMap(clazz)
    }
  }

  private val knownTypes: Map[String, (Map[String, String] => ShardSpec, (ShardSpec, Int, Int) => ShardSpec)] =
    Map[String, (Map[String, String] => ShardSpec, (ShardSpec, Int, Int) => ShardSpec)](
      "hashed" -> ((properties: Map[String, String]) =>
        // TODO: Move these property keys to a utils class
        new HashBasedNumberedShardSpec(
          properties("partitionId").toInt,
          properties.get("numPartitions").map(_.toInt).getOrElse(1),
          properties.get("bucketId").map(Integer.decode).orNull,
          properties.get("numBuckets").map(Integer.decode).orNull,
          properties.get("partitionDimensions").map(_.split(",").toList.asJava).orNull,
          properties.get("hashPartitionFunction").map(HashPartitionFunction.fromString)
            .getOrElse(HashPartitionFunction.MURMUR3_32_ABS),
          MAPPER),
        (baseSpec: ShardSpec, partitionNum: Int, numPartitions: Int) => {
          val spec = baseSpec.asInstanceOf[HashBasedNumberedShardSpec]
          new HashBasedNumberedShardSpec(
            partitionNum,
            numPartitions,
            spec.getBucketId,
            spec.getNumBuckets,
            spec.getPartitionDimensions,
            spec.getPartitionFunction,
            MAPPER
          )
        }
      ),
      "linear" -> ((properties: Map[String, String]) =>
        new LinearShardSpec(properties("partitionId").toInt),
        (_: ShardSpec, partitionNum: Int, _: Int) => new LinearShardSpec(partitionNum)
      ),
      "numbered" -> ((properties: Map[String, String]) =>
        new NumberedShardSpec(
          properties("partitionId").toInt,
          properties.get("numPartitions").map(_.toInt).getOrElse(1)),
        (_: ShardSpec, partitionNum: Int, numPartitions: Int) =>
          new NumberedShardSpec(partitionNum, numPartitions)
      ),
      "single" -> ((properties: Map[String, String]) =>
        new SingleDimensionShardSpec(
          properties("dimension"),
          properties("start"),
          properties("end"),
          properties("partitionId").toInt,
          // Explicitly constructing an Integer since Scala options and auto-boxing don't play nicely
          new Integer(properties.get("numPartitions").map(_.toInt).getOrElse(1))
        ),
        (baseSpec: ShardSpec, partitionNum: Int, numPartitions: Int) => {
          val spec = baseSpec.asInstanceOf[SingleDimensionShardSpec]
          new SingleDimensionShardSpec(
            spec.getDimension, spec.getStart, spec.getEnd, partitionNum, numPartitions
          )
        }
      )
    )
}
