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
package com.streamsets.pipeline.stage.processor.delay;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageBehaviorFlags;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.lib.util.ThreadUtil;

import java.util.Iterator;

@GenerateResourceBundle
@StageDef(
    version = 2,
    label = "Delay",
    description = "Allows you to delay any records passing through it by a given number of milliseconds",
    icon="delay.png",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH

    },
    flags = StageBehaviorFlags.PASSTHROUGH,
    upgraderDef = "upgrader/DelayProcessor.yaml",
    onlineHelpRefUrl ="index.html?contextID=task_jh5_qxf_wbb"
)
public class DelayProcessor extends SingleLaneRecordProcessor {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "1000",
      label = "Delay Between Batches",
      description = "Milliseconds to wait before sending records to next stage",
      displayMode = ConfigDef.DisplayMode.BASIC,
      min = 0,
      max = Integer.MAX_VALUE
  )
  public int delay;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Skip Delay on Empty Batch",
      description = "Skips the configured delay for empty batches",
      displayMode = ConfigDef.DisplayMode.BASIC
  )
  public boolean skipDelayOnEmptyBatch;

  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    Iterator<Record> records = batch.getRecords();
    if (delay > 0 && (records.hasNext() || !skipDelayOnEmptyBatch)) {
      ThreadUtil.sleep(delay);
    }
    while (records.hasNext()) {
      process(records.next(), batchMaker);
    }
  }

  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    batchMaker.addRecord(record);
  }

}
