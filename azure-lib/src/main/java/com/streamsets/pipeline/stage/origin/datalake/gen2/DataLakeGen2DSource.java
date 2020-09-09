/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.origin.datalake.gen2;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;

import static com.streamsets.pipeline.config.OriginAvroSchemaSource.SOURCE;

@StageDef(
    version = 3,
    label = "Azure Data Lake Storage Gen2",
    description = "Reads data from Azure Data Lake Storage Gen2",
    icon = "data-lake-store-gen2.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    upgrader = DataLakeGen2SourceUpgrader.class,
    onlineHelpRefUrl ="index.html?contextID=task_sh1_d45_rhb"
)
@ConfigGroups(DataLakeGen2SourceGroups.class)
@HideConfigs(value = {
    "dataLakeGen2SourceConfigBean.hdfsUri",
    "dataLakeGen2SourceConfigBean.hdfsUser",
    "dataLakeGen2SourceConfigBean.hdfsKerberos",
    "dataLakeGen2SourceConfigBean.hdfsConfDir",
    "dataLakeGen2SourceConfigBean.hdfsConfigs",
    "conf.allowLateDirectory",
    "conf.dataFormatConfig.verifyChecksum",
    "conf.dataFormatConfig.avroSchemaSource",
    "conf.dataFormatConfig.avroSchema",
    "conf.dataFormatConfig.schemaRegistryUrls",
    "conf.dataFormatConfig.schemaLookupMode",
    "conf.dataFormatConfig.subject",
    "conf.dataFormatConfig.schemaId"
})
@GenerateResourceBundle
public class DataLakeGen2DSource extends DPushSource {

  @ConfigDefBean
  public DataLakeGen2SourceConfigBean dataLakeGen2SourceConfigBean;

  @ConfigDefBean
  public SpoolDirConfigBean conf;

  @Override
  protected PushSource createPushSource() {
    if (conf.dataFormat == DataFormat.AVRO) {
      conf.dataFormatConfig.avroSchemaSource = SOURCE;
    }

    return new DataLakeGen2Source(conf, dataLakeGen2SourceConfigBean);
  }
}
