/*
 * Copyright 2017 StreamSets Inc.
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
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.FileRawSourcePreviewer;
import com.streamsets.pipeline.configurablestage.DPushSource;

import static com.streamsets.pipeline.config.OriginAvroSchemaSource.SOURCE;

@StageDef(
    version = 9,
    label = "Directory",
    description = "Reads files from a directory",
    icon="directory.png",
    execution = {ExecutionMode.STANDALONE, ExecutionMode.EDGE},
    recordsByRef = true,
    upgrader = SpoolDirSourceUpgrader.class,
    resetOffset = true,
    producesEvents = true,
    onlineHelpRefUrl = "index.html#Origins/Directory.html#task_gfj_ssv_yq"
)
@RawSource(rawSourcePreviewer = FileRawSourcePreviewer.class)
@ConfigGroups(Groups.class)
@HideConfigs(value = {
  "conf.dataFormatConfig.verifyChecksum",
  "conf.dataFormatConfig.avroSchemaSource"
})
@GenerateResourceBundle
public class SpoolDirDSource extends DPushSource {

  @ConfigDefBean
  public SpoolDirConfigBean conf;

  @Override
  protected PushSource createPushSource() {
    if (conf.dataFormat == DataFormat.AVRO) {
      conf.dataFormatConfig.avroSchemaSource = SOURCE;
    }
    return new SpoolDirSource(conf);
  }

}
