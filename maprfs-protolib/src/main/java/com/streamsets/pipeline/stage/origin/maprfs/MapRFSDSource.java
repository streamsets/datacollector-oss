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
package com.streamsets.pipeline.stage.origin.maprfs;

import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.lib.event.FinishedFileEvent;
import com.streamsets.pipeline.lib.event.NewFileEvent;
import com.streamsets.pipeline.lib.event.NoMoreDataEvent;
import com.streamsets.pipeline.stage.origin.hdfs.HdfsDSource;

@StageDef(
    version = 1,
    label = "MapR FS Standalone",
    description = "Reads files from a MapR filesystem",
    icon="mapr_xd-multithreaded.png",
    execution = ExecutionMode.STANDALONE,
    recordsByRef = true,
    resetOffset = true,
    producesEvents = true,
    eventDefs = {NewFileEvent.class, FinishedFileEvent.class, NoMoreDataEvent.class},
    upgraderDef = "upgrader/MapRFSDSource.yaml",
    onlineHelpRefUrl ="index.html#/datacollector/UserGuide/Origins/MapRFSStandalone.html#task_tpv_kqc_mdb"
)
@GenerateResourceBundle
public class MapRFSDSource extends HdfsDSource {
  @Override
  protected PushSource createPushSource() {
    // Since we're inheriting the configuration from usual HDFS origin, we don't have a way to override the default value in the
    // annotation and hence the default value is kind of "hidden" and supplied here.
    if (hdfsConf.hdfsUri == null || hdfsConf.hdfsUri.isEmpty()) {
      hdfsConf.hdfsUri = "maprfs:///";
    }

    return super.createPushSource();
  }
}
