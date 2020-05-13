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
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;
import com.streamsets.pipeline.api.service.ServiceConfiguration;
import com.streamsets.pipeline.api.service.ServiceDependency;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.lib.event.FinishedFileEvent;
import com.streamsets.pipeline.lib.event.NewFileEvent;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;

@StageDef(
    version = 13,
    label = "Amazon S3",
    description = "Reads files from Amazon S3",
    icon="s3.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    eventDefs = {NewFileEvent.class, FinishedFileEvent.class, NoMoreDataEvent.class},
    upgrader = AmazonS3SourceUpgrader.class,
    upgraderDef = "upgrader/AmazonS3DSource.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_gfj_ssv_yq",
    services = @ServiceDependency(
      service = DataFormatParserService.class,
      configuration = {
        @ServiceConfiguration(
            name = "displayFormats",
            value = "AVRO,DELIMITED,EXCEL,JSON,LOG,PROTOBUF,SDC_JSON,TEXT,WHOLE_FILE,XML"
        )
      }
    )
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class AmazonS3DSource extends DPushSource {

  @ConfigDefBean()
  public S3ConfigBean s3ConfigBean;

  @Override
  protected PushSource createPushSource() {
    return new AmazonS3SourceImpl(s3ConfigBean);
  }
}
