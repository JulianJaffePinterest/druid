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

import java.io.ByteArrayInputStream
import java.util.Properties

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Suppliers
import org.apache.druid.indexer.SQLMetadataStorageUpdaterJobHandler
import org.apache.druid.java.util.common.StringUtils
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.registries.SQLConnectorRegistry
import org.apache.druid.timeline.DataSegment
import org.skife.jdbi.v2.{DBI, Handle}
import org.apache.druid.metadata.{MetadataStorageConnectorConfig, MetadataStorageTablesConfig,
  SQLMetadataConnector}
import org.apache.spark.sql.sources.v2.DataSourceOptions

import scala.collection.JavaConverters.asScalaBufferConverter

class DruidMetadataClient(
                           metadataDbType: String,
                           host: String,
                           port: Int,
                           connectUri: String,
                           user: String,
                           password: String, // TODO: Do this better
                           dbcpMap: Properties,
                           base: String = "druid"
                         ) extends Logging {
  private lazy val druidMetadataTableConfig = MetadataStorageTablesConfig.fromBase(base)
  private lazy val dbcpProperties = new Properties()
  dbcpProperties.putAll(dbcpMap)

  private lazy val connectorConfig: MetadataStorageConnectorConfig =
    new MetadataStorageConnectorConfig
    {
      override def isCreateTables: Boolean = false
      override def getHost: String = host
      override def getPort: Int = port
      override def getConnectURI: String = connectUri
      override def getUser: String = user
      override def getPassword: String = password
      override def getDbcpProperties: Properties = dbcpProperties
    }
  private lazy val connectorConfigSupplier = Suppliers.ofInstance(connectorConfig)
  private lazy val metadataTableConfigSupplier = Suppliers.ofInstance(druidMetadataTableConfig)
  private lazy val connector = buildSQLConnector()

  def getSegmentPayloads(
                           datasource: String,
                           intervalStart: Option[String],
                           intervalEnd: Option[String]
                         ): Seq[DataSegment] = {
    val dbi: DBI = connector.getDBI
    val startTime = intervalStart.getOrElse("1970-01-01T00:00:00.000Z")
    val endTime = intervalEnd.getOrElse("2100-01-01T00:00:00.000Z")
    dbi.withHandle((handle: Handle) => {
      val statement =
        s"""
          |SELECT payload FROM ${druidMetadataTableConfig.getSegmentsTable}
          |WHERE datasource = :datasource AND start >= :start AND end <= :end AND used = 1
        """.stripMargin
      val query = handle.createQuery(statement)
      val result = query
          .bind("datasource", datasource)
          .bind("start", startTime)
          .bind("end", endTime)
          .mapTo(classOf[String]).list().asScala
      result.map(m =>
        MAPPER.readValue[DataSegment](m, new TypeReference[DataSegment] {})
      )
    })
  }

  def publishSegments(
                       segments: java.util.List[DataSegment],
                       mapper: ObjectMapper
                     ): Unit = {
    val metadataStorageUpdaterJobHandler = new SQLMetadataStorageUpdaterJobHandler(connector)
    metadataStorageUpdaterJobHandler.publishSegments(druidMetadataTableConfig.getSegmentsTable,
      segments, mapper)
  }

  /**
    * This won't run in a Druid cluster, so users will need to respecify metadata connection info.
    * This also means users will need to specifically include the extension jars on their clusters.
    *
    * @return
    */
  private def buildSQLConnector(): SQLMetadataConnector = {
    SQLConnectorRegistry.create(metadataDbType, connectorConfigSupplier, metadataTableConfigSupplier)
  }
}

object DruidMetadataClient {
  def apply(dataSourceOptions: DataSourceOptions): DruidMetadataClient = {
    assert(dataSourceOptions.get(DruidDataSourceOptionKeys.metadataDbTypeKey).isPresent,
      s"Must set ${DruidDataSourceOptionKeys.metadataDbTypeKey} or provide segments directly!")
    val dbcpProperties = new Properties()
    if (dataSourceOptions.get(DruidDataSourceOptionKeys.metadataDbcpPropertiesKey).isPresent) {
      // Assuming that .store was used to serialize the original DbcpPropertiesMap to a string
      dbcpProperties.load(
        new ByteArrayInputStream(
          StringUtils.toUtf8(dataSourceOptions
            .get(DruidDataSourceOptionKeys.metadataDbcpPropertiesKey).get())
        )
      )
    }
    new DruidMetadataClient(
      dataSourceOptions.get(DruidDataSourceOptionKeys.metadataDbTypeKey).get(),
      dataSourceOptions.get(DruidDataSourceOptionKeys.metadataHostKey).orElse("localhost"),
      dataSourceOptions.getInt(DruidDataSourceOptionKeys.metadataPortKey, -1),
      dataSourceOptions.get(DruidDataSourceOptionKeys.metadataConnectUriKey).orElse(""),
      dataSourceOptions.get(DruidDataSourceOptionKeys.metadataUserKey).orElse(""),
      dataSourceOptions.get(DruidDataSourceOptionKeys.metadataPasswordKey).orElse(""),
      dbcpProperties,
      dataSourceOptions.get(DruidDataSourceOptionKeys.metadataBaseNameKey).orElse("druid")
    )
  }
}
