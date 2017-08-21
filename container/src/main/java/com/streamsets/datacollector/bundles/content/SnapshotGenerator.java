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
package com.streamsets.datacollector.bundles.content;

import com.streamsets.datacollector.bundles.BundleContentGenerator;
import com.streamsets.datacollector.bundles.BundleContentGeneratorDef;
import com.streamsets.datacollector.bundles.BundleContext;
import com.streamsets.datacollector.bundles.BundleWriter;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.store.PipelineInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.PipelineException;

import java.io.IOException;
import java.io.InputStream;

@BundleContentGeneratorDef(
  name = "Snapshots",
  description = "Pipeline snapshots.",
  version = 1,
  enabledByDefault = false
)
public class SnapshotGenerator implements BundleContentGenerator {

  @Override
  public void generateContent(BundleContext context, BundleWriter writer) throws IOException {
    PipelineStoreTask store = context.getPipelineStore();
    SnapshotStore snapshotStore = context.getSnapshotStore();

    try {
      for(PipelineInfo pipelineInfo : store.getPipelines()) {
        String name = pipelineInfo.getPipelineId();
        String rev = pipelineInfo.getLastRev();

        for(SnapshotInfo snapshotInfo : snapshotStore.getSummaryForPipeline(name, rev)) {
          writer.writeJson(name + "/" + snapshotInfo.getId() + "/info.json", BeanHelper.wrapSnapshotInfoNewAPI(snapshotInfo));

          // Do not write unfinished snapshots
          if(snapshotInfo.isInProgress()) {
            continue;
          }

          try(InputStream snapshotStream = snapshotStore.get(name, rev, snapshotInfo.getId()).getOutput()) {
            if(snapshotStream != null) {
              writer.write(name + "/" + snapshotInfo.getId() + "/output.json", snapshotStream);
            }
          }
        }
      }
    } catch (PipelineException e) {
      throw new IOException("Can't load pipelines", e);
    }
  }
}
