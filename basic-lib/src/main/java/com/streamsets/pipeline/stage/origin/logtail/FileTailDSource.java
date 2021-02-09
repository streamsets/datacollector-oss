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
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.RawSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSource;
import com.streamsets.pipeline.config.FileRawSourcePreviewer;

@StageDef(
    version = 5,
    label = "File Tail",
    description = "Tails a file. It handles rolling files within the same directory",
    icon = "fileTail.png",
    execution = {ExecutionMode.STANDALONE, ExecutionMode.EDGE},
    outputStreams = FileTailOutputStreams.class,
    recordsByRef = true,
    upgrader = FileTailSourceUpgrader.class,
    upgraderDef = "upgrader/FileTailDSource.yaml",
    resetOffset = true,
    producesEvents = true,
    eventDefs = {StartEvent.class, EndEvent.class, ErrorEvent.class},
    onlineHelpRefUrl ="index.html?contextID=task_unq_wdw_yq"
)
@RawSource(rawSourcePreviewer = FileRawSourcePreviewer.class)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class FileTailDSource extends DSource {

  @ConfigDefBean
  public FileTailConfigBean conf;

  @Override
  protected Source createSource() {
    return new FileTailSource(conf);
  }

}
