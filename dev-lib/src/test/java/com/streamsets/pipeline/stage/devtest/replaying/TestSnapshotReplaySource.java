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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Test;

import java.util.List;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class TestSnapshotReplaySource {

  @Test
  public void testReplayingSource() throws StageException {
    final String outputLane = "output";

    SnapshotReplaySource origin = new SnapshotReplaySource(
        null,
        TestSnapshotReplaySource.class.getResourceAsStream("sample_snapshot.json"),
        "DevRawDataSource_01"
    );

    SourceRunner runner = new SourceRunner.Builder(SnapshotReplayDSource.class, origin)
        .addOutputLane(outputLane)
        .build();

    runner.runInit();

    try {
      StageRunner.Output output = runner.runProduce(null, 1);
      List<Record> records = output.getRecords().get(outputLane);
      assertThat(records, hasSize(1));
      assertThat(output.getNewOffset(), equalTo("0"));
    } finally {
      runner.runDestroy();
    }
  }


}
