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
package com.streamsets.pipeline.stage.devtest;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestRandomSource {

  @Test
  public void testRandomSource() throws Exception{
    SourceRunner runner = new SourceRunner.Builder(RandomSource.class)
      .addConfiguration("maxRecordsToGenerate", 1000)
      .addConfiguration("fields", "a,b")
      .addConfiguration("delay", 0)
      .addOutputLane("a")
      .build();
    runner.runInit();
    try {
      StageRunner.Output output = runner.runProduce(null, 25);
      List<Record> records = output.getRecords().get("a");
      Assert.assertTrue(records.size() <= 25);
      if (!records.isEmpty()) {
        Assert.assertNotNull(records.get(0).get("/a"));
        Assert.assertNotNull(records.get(0).get("/b"));
        Assert.assertEquals(Field.Type.INTEGER, records.get(0).get("/a").getType());
        Assert.assertEquals(Field.Type.INTEGER, records.get(0).get("/b").getType());
      }
    } finally {
      runner.runDestroy();
    }
  }

}
