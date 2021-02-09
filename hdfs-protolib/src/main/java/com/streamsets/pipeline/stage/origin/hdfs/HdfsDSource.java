/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.hdfs;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.FileRawSourcePreviewer;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirConfigBean;
import com.streamsets.pipeline.lib.event.FinishedFileEvent;
import com.streamsets.pipeline.lib.event.NewFileEvent;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;

import static com.streamsets.pipeline.config.OriginAvroSchemaSource.SOURCE;

@StageDef(
    version = 3,
    label = "Hadoop FS Standalone",
    description = "Reads files from a Hadoop file system",
    icon="hdfs-multithreaded.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    eventDefs = {NewFileEvent.class, FinishedFileEvent.class, NoMoreDataEvent.class},
    upgraderDef = "upgrader/HdfsDSource.yaml",
    onlineHelpRefUrl ="index.html#/datacollector/UserGuide/Origins/HDFSStandalone.html#task_l3t_sdm_hdb"
)
@RawSource(rawSourcePreviewer = FileRawSourcePreviewer.class)
@ConfigGroups(Groups.class)
@HideConfigs(value = {
    "conf.allowLateDirectory",
    "conf.dataFormatConfig.verifyChecksum",
    "conf.dataFormatConfig.avroSchemaSource"
})
@GenerateResourceBundle
public class HdfsDSource extends DPushSource {

  @ConfigDefBean
  public SpoolDirConfigBean conf;

  @ConfigDefBean
  public HdfsSourceConfigBean hdfsConf;

  @Override
  protected PushSource createPushSource() {
    if (conf.dataFormat == DataFormat.AVRO) {
      conf.dataFormatConfig.avroSchemaSource = SOURCE;
    }

    return new HdfsSource(conf, hdfsConf);
  }
}
