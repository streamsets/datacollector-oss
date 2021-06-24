/*
 * ```Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.```
 */
package com.streamsets.pipeline.stage.cloudstorage.origin;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.config.PostProcessingOptionsChooserValues;

public class GcsOriginPostProcessingConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "NONE",
      label = "Post processing blob handling Option",
      description = "Action to take after blob is processed",
      displayPosition = 200,
      group = "#0"
  )
  @ValueChooserModel(PostProcessingOptionsChooserValues.class)
  public PostProcessingOptions postProcessing;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "MOVE_TO_PREFIX",
      label = "Archiving Option",
      displayPosition = 201,
      group = "#0",
      dependsOn = "postProcessing",
      triggeredByValue = { "ARCHIVE" }
  )
  @ValueChooserModel(GcsArchivingOptionChooserValues.class)
  public GcsArchivingOption archivingOption;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Prefix",
      description = "Objects will be moved/copied into this prefix",
      displayPosition = 202,
      group = "#0",
      dependsOn = "postProcessing",
      triggeredByValue = { "ARCHIVE" }
  )
  public String postProcessPrefix;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Target bucket",
      description = "Objects in will be moved/copied into this bucket",
      displayPosition = 203,
      group = "#0",
      dependsOn = "archivingOption",
      triggeredByValue = { "MOVE_TO_BUCKET", "COPY_TO_BUCKET" }
  )
  public String postProcessBucket;


}
