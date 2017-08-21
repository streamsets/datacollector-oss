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
package ${groupId}.stage.origin.sample;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestSampleSource {
  private static final int MAX_BATCH_SIZE = 5;

  @Test
  public void testOrigin() throws Exception {
    SourceRunner runner = new SourceRunner.Builder(SampleDSource.class)
        .addConfiguration("config", "value")
        .addOutputLane("lane")
        .build();

    try {
      runner.runInit();

      final String lastSourceOffset = null;
      StageRunner.Output output = runner.runProduce(lastSourceOffset, MAX_BATCH_SIZE);
      Assert.assertEquals("5", output.getNewOffset());
      List<Record> records = output.getRecords().get("lane");
      Assert.assertEquals(5, records.size());
      Assert.assertTrue(records.get(0).has("/fieldName"));
      Assert.assertEquals("Some Value", records.get(0).get("/fieldName").getValueAsString());

    } finally {
      runner.runDestroy();
    }
  }

}
