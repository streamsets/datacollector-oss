/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.destination.s3;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.pipeline.stage.lib.aws.SSEConfigBean;
import com.streamsets.pipeline.stage.lib.aws.ProxyConfig;
import com.streamsets.pipeline.stage.lib.aws.TransferManagerConfig;
import com.streamsets.pipeline.stage.origin.s3.S3Config;

import java.util.List;

public class S3TargetConfigBean {

  public static final String S3_TARGET_CONFIG_BEAN_PREFIX = "s3TargetConfigBean.";
  public static final String S3_CONFIG_PREFIX = S3_TARGET_CONFIG_BEAN_PREFIX + "s3Config.";
  public static final String S3_SEE_CONFIG_PREFIX = S3_TARGET_CONFIG_BEAN_PREFIX + "sseConfig.";
  public static final String S3_TM_CONFIG_PREFIX = S3_TARGET_CONFIG_BEAN_PREFIX + "tmConfig.";

  @ConfigDefBean(groups = "S3")
  public S3Config s3Config;

  @ConfigDefBean(groups = "SSE")
  public SSEConfigBean sseConfig;

  @ConfigDefBean(groups = "ADVANCED")
  public ProxyConfig proxyConfig;

  @ConfigDefBean(groups = "ADVANCED")
  public TransferManagerConfig tmConfig;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "",
      label = "Partition Prefix",
      description = "Partition to write to. If the partition doesn't exist on Amazon S3, it will be created.",
      displayPosition = 180,
      group = "S3"
  )
  public String partitionTemplate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTC",
      label = "Data Time Zone",
      description = "Time zone to use to resolve the date time of a time-based partition prefix",
      displayPosition = 190,
      group = "S3"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String timeZoneID;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
      evaluation = ConfigDef.Evaluation.EXPLICIT,
      defaultValue = "${time:now()}",
      label = "Time Basis",
      description = "Time basis to use for a record. Enter an expression that evaluates to a datetime. To use the " +
          "processing time, enter ${time:now()}. To use field values, use '${record:value(\"<filepath>\")}'.",
      displayPosition = 200,
      group = "S3"
  )
  public String timeDriverTemplate;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    defaultValue = "sdc",
    description = "Prefix for object names that will be uploaded on Amazon S3",
    label = "Object Name Prefix",
    displayPosition = 210,
    group = "S3"
  )
  public String fileNamePrefix;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    label = "Data Format",
    displayPosition = 220,
    group = "S3"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.BOOLEAN,
    defaultValue = "false",
    label = "Compress with gzip",
    displayPosition = 230,
    group = "S3"
  )
  public boolean compress;

  @ConfigDefBean(groups = {"S3"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  public List<Stage.ConfigIssue> init(Stage.Context context, List<Stage.ConfigIssue> issues) {
    // Don't use amazon s3 client for file transfer error retries (Setting maxErrorRetries to 0)
    // (SDC will retry the file transfer based on AT_LEAST_ONCE/AT_MOST_ONCE SEMANTICS)
    s3Config.init(context, S3_CONFIG_PREFIX, proxyConfig, issues, (dataFormat == DataFormat.WHOLE_FILE)? 0 : -1);

    if(s3Config.bucket == null || s3Config.bucket.isEmpty()) {
      issues.add(
          context.createConfigIssue(
              Groups.S3.name(),
              S3_CONFIG_PREFIX + "bucket",
              Errors.S3_01
          )
      );
    } else if (!s3Config.getS3Client().doesBucketExist(s3Config.bucket)) {
      issues.add(
          context.createConfigIssue(
              Groups.S3.name(),
              S3_CONFIG_PREFIX + "bucket",
              Errors.S3_02, s3Config.bucket
          )
      );
    }

    //File prefix should not be empty for non whole file format.
    if (dataFormat != DataFormat.WHOLE_FILE && (fileNamePrefix == null || fileNamePrefix.isEmpty())) {
      issues.add(
          context.createConfigIssue(
              Groups.S3.getLabel(),
              S3TargetConfigBean.S3_TARGET_CONFIG_BEAN_PREFIX + "fileNamePrefix",
              Errors.S3_05
          )
      );
    }

    dataGeneratorFormatConfig.init(
        context,
        dataFormat,
        Groups.S3.name(),
        S3_TARGET_CONFIG_BEAN_PREFIX + "dataGeneratorFormatConfig",
        issues
    );

    if(issues.size() == 0) {
      generatorFactory = dataGeneratorFormatConfig.getDataGeneratorFactory();
    }
    return issues;
  }

  public void destroy() {
    s3Config.destroy();
  }

  private DataGeneratorFactory generatorFactory;

  public DataGeneratorFactory getGeneratorFactory() {
    return generatorFactory;
  }
}
