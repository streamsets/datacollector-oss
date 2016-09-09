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

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.configurablestage.DTarget;

@StageDef(
  version = 8,
  label = "Amazon S3",
  description = "Writes to Amazon S3",
  icon = "s3.png",
  privateClassLoader = true,
  upgrader = AmazonS3TargetUpgrader.class,
  producesEvents = true,
  onlineHelpRefUrl = "index.html#Destinations/AmazonS3.html#task_pxb_j3r_rt"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
@HideConfigs(value = {"s3TargetConfigBean.dataGeneratorFormatConfig.includeSchema"})
public class AmazonS3DTarget extends DTarget {

  @ConfigDefBean()
  public S3TargetConfigBean s3TargetConfigBean;

  @Override
  protected Target createTarget() {
    return new AmazonS3Target(s3TargetConfigBean);
  }
}
