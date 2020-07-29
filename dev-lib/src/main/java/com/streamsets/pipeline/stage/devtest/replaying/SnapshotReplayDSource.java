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
package com.streamsets.pipeline.stage.devtest.replaying;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DSource;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "Dev Snapshot Replaying",
    description = "Play snapshots as source records",
    execution = ExecutionMode.STANDALONE,
    icon = "dev.png",
    upgrader = SnapshotReplaySourceUpgrader.class,
    upgraderDef = "upgrader/SnapshotReplayDSource.yaml",
    onlineHelpRefUrl = "index.html#Pipeline_Design/DevStages.html"
)
@ConfigGroups(value = SnapshotReplaySourceGroups.class)
public class SnapshotReplayDSource extends DSource {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Snapshot File Path",
      evaluation = ConfigDef.Evaluation.IMPLICIT,
      displayPosition = 20,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "REPLAY"
  )
  public String snapshotFilePath;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      label = "Snapshot Stage Instance Name",
      description = "The stage instance name from which to build records (ex: DevRawDataSource_01).  Leave blank to" +
          " simply use the first set of records in the snapshot file.",
      displayPosition = 30,
      displayMode = ConfigDef.DisplayMode.BASIC,
      group = "REPLAY"
  )
  public String stageInstanceName;

  @Override
  protected Source createSource() {
    return new SnapshotReplaySource(snapshotFilePath, stageInstanceName);
  }
}
