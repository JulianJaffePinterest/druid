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

import org.apache.druid.java.util.common.{IAE, StringUtils}
import org.apache.druid.storage.s3.{S3DataSegmentPusherConfig, S3InputDataConfig}

object DeepStorageConstructorHelpers {
  def createS3DataSegmentPusherConfig(properties: Map[String, String]): S3DataSegmentPusherConfig = {
    val conf = new S3DataSegmentPusherConfig
    conf.setBaseKey(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3BaseKeyKey)))
    conf.setBucket(properties(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3BucketKey)))
    conf.setDisableAcl(
      properties.get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3DisableACLKey)).fold(false)(_.toBoolean)
    )
    val maxListingLength =
      properties.get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3MaxListingLengthKey)).fold(1000)(_.toInt)
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
    val maxListingLength =
      properties
        .get(StringUtils.toLowerCase(DruidDataSourceOptionKeys.s3MaxListingLengthKey))
        .fold(1000)(_.toInt)
    val inputDataConf = new S3InputDataConfig
    inputDataConf.setMaxListingLength(maxListingLength)
    inputDataConf
  }
}
