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
package com.streamsets.pipeline.stage.destination;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.Utils;

import java.util.Iterator;

@GenerateResourceBundle
@StageDef(
    version = 1,
    label = "To Event",
    description = "It echoes root field from records as a body of an event. For development purpose only.",
    execution = {
        ExecutionMode.STANDALONE,
        ExecutionMode.CLUSTER_BATCH,
        ExecutionMode.CLUSTER_YARN_STREAMING,
        ExecutionMode.CLUSTER_MESOS_STREAMING,
        ExecutionMode.EDGE,
        ExecutionMode.EMR_BATCH
    },
    icon="dev.png",
    producesEvents = true,
    upgraderDef = "upgrader/ToEventTarget.yaml",
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Pipeline_Design/DevStages.html"
)
public class ToEventTarget extends BaseTarget {
  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> it = batch.getRecords();
    int counter = 1;
    while(it.hasNext()) {
      String recordSourceId = Utils.format("event:{}:{}:{}", "event-target", 1, counter++);
      EventRecord event = getContext().createEventRecord("event-target", 1, recordSourceId);
      event.set(it.next().get());
      getContext().toEvent(event);
    }
  }
}
