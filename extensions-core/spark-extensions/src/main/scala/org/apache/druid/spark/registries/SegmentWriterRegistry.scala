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

import java.io.File

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.druid.java.util.common.{IAE, StringUtils}
import org.apache.druid.segment.loading.{DataSegmentKiller, DataSegmentPusher,
  LocalDataSegmentKiller, LocalDataSegmentPusher, LocalDataSegmentPusherConfig}
import org.apache.druid.spark.utils.{DruidDataSourceOptionKeys, Logging}
import org.apache.druid.spark.MAPPER
import org.apache.druid.storage.azure.{AzureAccountConfig, AzureCloudBlobIterableFactory,
  AzureDataSegmentConfig, AzureDataSegmentKiller, AzureDataSegmentPusher, AzureInputDataConfig,
  AzureStorage}
import org.apache.druid.storage.google.{GoogleAccountConfig, GoogleDataSegmentKiller,
  GoogleDataSegmentPusher, GoogleInputDataConfig, GoogleStorage}
import org.apache.druid.storage.hdfs.{HdfsDataSegmentKiller, HdfsDataSegmentPusher,
  HdfsDataSegmentPusherConfig}
import org.apache.druid.storage.s3.{S3DataSegmentKiller, S3DataSegmentPusher,
  S3DataSegmentPusherConfig, S3InputDataConfig, ServerSideEncryptingAmazonS3}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.sources.v2.DataSourceOptions

import scala.collection.mutable

/**
  * A registry for functions to create DataSegmentPushers and DataSegmentKillers.
  */
object SegmentWriterRegistry extends Logging {
  private val registeredSegmentPusherCreatorFunctions: mutable.HashMap[String, Map[String, String] =>
    DataSegmentPusher] = new mutable.HashMap()
  private val registeredSegmentKillerCreatorFunctions: mutable.HashMap[String, DataSourceOptions =>
    DataSegmentKiller] = new mutable.HashMap()

  def register(
                deepStorageType: String,
                segmentPusherCreatorFunc: Map[String, String] => DataSegmentPusher,
                segmentKillerCreatorFunc: DataSourceOptions => DataSegmentKiller
              ): Unit = {
    registeredSegmentPusherCreatorFunctions(deepStorageType) = segmentPusherCreatorFunc
    registeredSegmentKillerCreatorFunctions(deepStorageType) = segmentKillerCreatorFunc
  }

  def registerByType(deepStorageType: String): Unit = {
    if (!registeredSegmentPusherCreatorFunctions.contains(deepStorageType)
      && knownTypes.contains(deepStorageType)) {
      knownTypes(deepStorageType)()
    }
  }

  def getSegmentPusher(
                        deepStorageType: String,
                        properties: Map[String, String]
                      ): DataSegmentPusher = {
    if (registeredSegmentPusherCreatorFunctions.contains(deepStorageType)) {
      registeredSegmentPusherCreatorFunctions(deepStorageType)(properties)
    } else {
      throw new IAE("No registered segment pusher creation function for deep storage " +
        "type %s", deepStorageType)
    }
  }

  def getSegmentKiller(
                        deepStorageType: String,
                        properties: DataSourceOptions
                      ): DataSegmentKiller = {
    if (registeredSegmentKillerCreatorFunctions.contains(deepStorageType)) {
      registeredSegmentKillerCreatorFunctions(deepStorageType)(properties)
    } else {
      throw new IAE("No registered segment killer creation function for deep storage " +
        "type %s", deepStorageType)
    }
  }

  private val knownTypes: Map[String, () => Unit] =
    Map[String, () => Unit](
      DruidDataSourceOptionKeys.localDeepStorageTypeKey -> (
        () =>
          register(
            DruidDataSourceOptionKeys.localDeepStorageTypeKey,
            (properties: Map[String, String]) =>
              new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig() {
                // DataSourceOptions are case-insensitive, so when we use the map form we need to lowercase keys
                override def getStorageDirectory: File =
                  new File(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.localStorageDirectoryKey)))
              }),
            (dataSourceOptions: DataSourceOptions) =>
              new LocalDataSegmentKiller(new LocalDataSegmentPusherConfig() {
                override def getStorageDirectory: File =
                  new File(dataSourceOptions.get(DruidDataSourceOptionKeys.localStorageDirectoryKey).get)
              })
          )
        ),
      // TODO: Adding these Pushers and Killers as placeholders. The confs are almost certainly not
      //  serializable, and we shouldn't force users to construct them. Instead, we should allow
      //  users to pass the arguments they care about (e.g. paths, buckets, keys, etc.) and
      //  construct the internal Druid confs ourselves.
      DruidDataSourceOptionKeys.hdfsDeepStorageTypeKey -> (
        () =>
          register(
            DruidDataSourceOptionKeys.hdfsDeepStorageTypeKey,
            (properties: Map[String, String]) =>
              new HdfsDataSegmentPusher(
                MAPPER.readValue[HdfsDataSegmentPusherConfig](
                  properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.hdfsPusherConfigKey)),
                  new TypeReference[HdfsDataSegmentPusherConfig] {}
                ),
                MAPPER.readValue[Configuration](
                  properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.hdfsHadoopConfKey)),
                  new TypeReference[Configuration] {}
                ),
                MAPPER),
            (dataSourceOptions: DataSourceOptions) => new HdfsDataSegmentKiller(
              MAPPER.readValue[Configuration](
                dataSourceOptions.get(DruidDataSourceOptionKeys.hdfsHadoopConfKey).get,
                new TypeReference[Configuration] {}
              ),
              MAPPER.readValue[HdfsDataSegmentPusherConfig](
                dataSourceOptions.get(DruidDataSourceOptionKeys.hdfsPusherConfigKey).get,
                new TypeReference[HdfsDataSegmentPusherConfig] {}
              )
            ))
      ),
      DruidDataSourceOptionKeys.s3DeepStorageTypeKey -> (
        () => register(
          DruidDataSourceOptionKeys.s3DeepStorageTypeKey,
          (properties: Map[String, String]) =>
            new S3DataSegmentPusher(
              MAPPER.readValue[ServerSideEncryptingAmazonS3](
                properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3ServerSideEncryptionConfigKey)),
                new TypeReference[ServerSideEncryptingAmazonS3] {}
              ),
              MAPPER.readValue[S3DataSegmentPusherConfig](
                properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3DataSegmentPusherConfigKey)),
                new TypeReference[S3DataSegmentPusherConfig] {}
              )),
          (dataSourceOptions: DataSourceOptions) =>
            new S3DataSegmentKiller(
              MAPPER.readValue[ServerSideEncryptingAmazonS3](
                dataSourceOptions.get(DruidDataSourceOptionKeys.s3ServerSideEncryptionConfigKey).get,
                new TypeReference[ServerSideEncryptingAmazonS3] {}
              ),
              MAPPER.readValue[S3DataSegmentPusherConfig](
                dataSourceOptions.get(DruidDataSourceOptionKeys.s3DataSegmentPusherConfigKey).get,
                new TypeReference[S3DataSegmentPusherConfig] {}
              ),
              MAPPER.readValue[S3InputDataConfig](
                dataSourceOptions.get(DruidDataSourceOptionKeys.s3InputDataConfigKey).get,
                new TypeReference[S3InputDataConfig] {}
              )
            )
        )
      ),
      DruidDataSourceOptionKeys.googleDeepStorageTypeKey -> (
        () => register(
          DruidDataSourceOptionKeys.googleDeepStorageTypeKey,
          (properties: Map[String, String]) =>
          new GoogleDataSegmentPusher(
            MAPPER.readValue[GoogleStorage](
              properties.get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.googleStorageConfigKey)).toString,
              new TypeReference[GoogleStorage] {}
            ),
            MAPPER.readValue[GoogleAccountConfig](
              properties.get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.googleAccountConfigKey)).toString,
              new TypeReference[GoogleAccountConfig] {}
            )
          ),
          (dataSourceOptions: DataSourceOptions) =>
            new GoogleDataSegmentKiller(
              MAPPER.readValue[GoogleStorage](
                dataSourceOptions.get(DruidDataSourceOptionKeys.googleStorageConfigKey).get,
                new TypeReference[GoogleStorage] {}
              ),
              MAPPER.readValue[GoogleAccountConfig](
                dataSourceOptions.get(DruidDataSourceOptionKeys.googleAccountConfigKey).get,
                new TypeReference[GoogleAccountConfig] {}
              ),
              MAPPER.readValue[GoogleInputDataConfig](
                dataSourceOptions.get(DruidDataSourceOptionKeys.googleInputDataConfigKey).get,
                new TypeReference[GoogleInputDataConfig] {}
              )
            )
        )
      ),
      DruidDataSourceOptionKeys.azureDeepStorageKey -> (
        () => register(
          DruidDataSourceOptionKeys.azureDeepStorageKey,
          (properties: Map[String, String]) =>
            new AzureDataSegmentPusher(
              MAPPER.readValue[AzureStorage](
                properties.get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureStorageConfigKey)).toString,
                new TypeReference[AzureStorage] {}
              ),
              MAPPER.readValue[AzureAccountConfig](
                properties.get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureAccountConfigKey)).toString,
                new TypeReference[AzureAccountConfig] {}
              ),
              MAPPER.readValue[AzureDataSegmentConfig](
                properties.get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureDataSegmentConfigKey)).toString,
                new TypeReference[AzureDataSegmentConfig] {}
              )
            ),
          (dataSourceOptions: DataSourceOptions) =>
            new AzureDataSegmentKiller(
              MAPPER.readValue[AzureDataSegmentConfig](
                dataSourceOptions.get(DruidDataSourceOptionKeys.azureDataSegmentConfigKey).get,
                new TypeReference[AzureDataSegmentConfig] {}
              ),
              MAPPER.readValue[AzureInputDataConfig](
                dataSourceOptions.get(DruidDataSourceOptionKeys.azureInputDataConfigKey).get,
                new TypeReference[AzureInputDataConfig] {}
              ),
              MAPPER.readValue[AzureAccountConfig](
                dataSourceOptions.get(DruidDataSourceOptionKeys.azureAccountConfigKey).get,
                new TypeReference[AzureAccountConfig] {}
              ),
              MAPPER.readValue[AzureStorage](
                dataSourceOptions.get(DruidDataSourceOptionKeys.azureStorageConfigKey).get,
                new TypeReference[AzureStorage] {}
              ),
              MAPPER.readValue[AzureCloudBlobIterableFactory](
                dataSourceOptions.get(DruidDataSourceOptionKeys.azureCloudBlobIterableFactoryConfigKey).get,
                new TypeReference[AzureCloudBlobIterableFactory] {}
              )
            )
        )
      )
    )
}
