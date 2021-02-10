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

import com.microsoft.azure.storage.blob.{CloudBlobClient, ListBlobItem}
import com.microsoft.azure.storage.{StorageCredentials, StorageUri}
import org.apache.druid.java.util.common.{IAE, StringUtils}
import org.apache.druid.storage.azure.blob.{ListBlobItemHolder, ListBlobItemHolderFactory}
import org.apache.druid.storage.azure.{AzureAccountConfig, AzureCloudBlobIterable,
  AzureCloudBlobIterableFactory, AzureCloudBlobIterator, AzureCloudBlobIteratorFactory,
  AzureDataSegmentConfig, AzureInputDataConfig, AzureStorage}
import org.apache.druid.storage.google.{GoogleAccountConfig, GoogleInputDataConfig}
import org.apache.druid.storage.s3.{S3DataSegmentPusherConfig, S3InputDataConfig}
import org.apache.hadoop.conf.Configuration

import java.io.{ByteArrayInputStream, DataInputStream}
import java.lang.{Iterable => JIterable}
import java.net.URI
import scala.collection.JavaConverters.asJavaIterableConverter

object DeepStorageConstructorHelpers extends TryWithResources {

  // HDFS Storage Helpers
  def createHadoopConfiguration(properties: Map[String, String]): Configuration = {
    val conf = new Configuration()
    val confByteStream = new ByteArrayInputStream(
      StringUtils.decodeBase64String(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.hdfsHadoopConfKey)))
    )
    tryWithResources(confByteStream, new DataInputStream(confByteStream)){
      case (_, inputStream: DataInputStream) => conf.readFields(inputStream)
    }
    conf
  }

  // S3 Storage Helpers

  def createS3DataSegmentPusherConfig(properties: Map[String, String]): S3DataSegmentPusherConfig = {
    val conf = new S3DataSegmentPusherConfig
    conf.setBaseKey(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3BaseKeyKey)))
    conf.setBucket(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.bucketKey)))
    conf.setDisableAcl(
      properties.get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3DisableACLKey)).fold(false)(_.toBoolean)
    )
    val maxListingLength =
      properties.get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.maxListingLengthKey)).fold(1000)(_.toInt)
    // Although S3DataSegmentPusherConfig defaults to 1024, S3DataInputConfig requires maxListing length to be < 1000
    if (maxListingLength < 1 || maxListingLength > 1000) {
      throw new IAE("maxListingLength must be between 1 and 1000!")
    }
    conf.setMaxListingLength(maxListingLength)
    conf.setUseS3aSchema(properties
      .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3UseS3ASchemaKey)).fold(true)(_.toBoolean))
    conf
  }

  def createS3InputDataConfig(properties: Map[String, String]): S3InputDataConfig = {
    val inputDataConf = new S3InputDataConfig
    val maxListingLength =
      properties
        .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.maxListingLengthKey))
        .fold(1000)(_.toInt)
    inputDataConf.setMaxListingLength(maxListingLength)
    inputDataConf
  }

  // GCS Storage Helpers

  def createGoogleAcountConfig(properties: Map[String, String]): GoogleAccountConfig = {
    val accountConfig = new GoogleAccountConfig
    accountConfig.setBucket(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.bucketKey)))
    accountConfig.setPrefix(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.prefixKey)))
    accountConfig
  }

  def createGoogleInputDataConfig(properties: Map[String, String]): GoogleInputDataConfig = {
    val maxListingLength =
      properties
        .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.maxListingLengthKey))
        .fold(1024)(_.toInt)
    val inputConfig = new GoogleInputDataConfig
    inputConfig.setMaxListingLength(maxListingLength)
    inputConfig
  }

  // Azure Storage Helpers

  def createAzureDataSegmentConfig(properties: Map[String, String]): AzureDataSegmentConfig = {
    val dataSegmentConfig = new AzureDataSegmentConfig
    dataSegmentConfig.setContainer(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureContainerKey)))
    dataSegmentConfig.setPrefix(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.prefixKey)))
    dataSegmentConfig
  }

  def createAzureInputDataConfig(properties: Map[String, String]): AzureInputDataConfig = {
    val maxListingLength =
      properties
        .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.maxListingLengthKey))
        .fold(1024)(_.toInt)
    val inputDataConfig = new AzureInputDataConfig
    inputDataConfig.setMaxListingLength(maxListingLength)
    inputDataConfig
  }

  def createAzureAccountConfig(properties: Map[String, String]): AzureAccountConfig = {
    val accountConfig = new AzureAccountConfig
    val maxTries = properties.getOrElse(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureMaxTriesKey), "3").toInt
    accountConfig.setProtocol(
      properties.getOrElse(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureProtocolKey), "https"))
    accountConfig.setMaxTries(maxTries)
    accountConfig.setAccount(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureAccountKey)))
    accountConfig.setKey(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureKeyKey)))
    accountConfig
  }

  def createAzureStorage(properties: Map[String, String]): AzureStorage = {
    val storageCredentials = StorageCredentials.tryParseCredentials(
      properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.connectionStringKey))
    )
    val primaryUri = properties
      .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azurePrimaryStorageUriKey))
      .map(new URI(_))
      .orNull
    val secondaryUri = properties
      .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azureSecondaryStorageUriKey))
      .map(new URI(_))
      .orNull
    val storageUri = new StorageUri(primaryUri, secondaryUri)
    val cloudBlobClient = new CloudBlobClient(storageUri, storageCredentials)
    new AzureStorage(cloudBlobClient)
  }

  /**
    * I highly doubt this works, but I don't have an Azure system to test on nor do I have the familiarity with Azure
    * to be sure that testing with local mocks will actually test what I want to test.
    *
    * @param properties
    * @return
    */
  /*def createAzureCloudBlobIterableFactory(properties: Map[String, String]): AzureCloudBlobIterableFactory = {
    // Taking advantage of the fact that java.net.URIs deviates from spec and dissallow spaces to use it as a separator.
    val prefixes = properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.azurePrefixesKey))
      .split(" ")
      .map(new URI(_))
      .toIterable
      .asJava
    val maxListingLength =
      properties
        .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.maxListingLengthKey))
        .fold(1024)(_.toInt)
    val azureStorage = DeepStorageConstructorHelpers.createAzureStorage(properties)
    val accountConfig = DeepStorageConstructorHelpers.createAzureAccountConfig(properties)

    val listBlobItemHolderFactory = new ListBlobItemHolderFactory {
      override def create(blobItem: ListBlobItem): ListBlobItemHolder = new ListBlobItemHolder(blobItem)
    }

    // The constructor for AzureCloudBlobIterator is protected, so no dice here
    val azureCloudBlobIteratorFactory = new AzureCloudBlobIteratorFactory {
      override def create(ignoredPrefixes: JIterable[URI], ignoredMaxListingLength: Int): AzureCloudBlobIterator = {
        new AzureCloudBlobIterator(azureStorage, listBlobItemHolderFactory, accountConfig, prefixes, maxListingLength)
      }
    }
    (_: JIterable[URI], _: Int) => {
      new AzureCloudBlobIterable(azureCloudBlobIteratorFactory, prefixes, maxListingLength)
    }
  }*/
}
