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
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.common.InterfaceAudience;
import com.streamsets.pipeline.common.InterfaceStability;
import com.streamsets.pipeline.config.PostProcessingOptions;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public class S3PostProcessingConfig {

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "NONE",
    label = "Post Processing Option",
    description = "Action to take after processing an object",
    displayPosition = 10,
    group = "#0"
  )
  @ValueChooserModel(S3PostProcessingChooserValues.class)
  public PostProcessingOptions postProcessing;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.MODEL,
    defaultValue = "MOVE_TO_PREFIX",
    label = "Archiving Option",
    displayPosition = 20,
    group = "#0",
    dependsOn = "postProcessing",
    triggeredByValue = { "ARCHIVE" }
  )
  @ValueChooserModel(S3ArchivingOptionChooserValues.class)
  public S3ArchivingOption archivingOption;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Post Process Prefix",
    description = "Objects will be moved/copied into this prefix",
    displayPosition = 30,
    group = "#0",
    dependsOn = "postProcessing",
    triggeredByValue = { "ARCHIVE" }
  )
  public String postProcessPrefix;

  @ConfigDef(
    required = false,
    type = ConfigDef.Type.STRING,
    label = "Post Process Bucket",
    description = "Objects will be moved/copied into this bucket",
    displayPosition = 40,
    group = "#0",
    dependsOn = "archivingOption",
    triggeredByValue = { "MOVE_TO_BUCKET", "COPY_TO_BUCKET" }
  )
  public String postProcessBucket;

}
