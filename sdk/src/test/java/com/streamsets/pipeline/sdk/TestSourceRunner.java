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
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import org.junit.Assert;
import org.junit.Test;

public class TestSourceRunner {

  @StageDef(
    version = 1,
    label = "Test",
    onlineHelpRefUrl = ""
  )
  public static class DummySource extends BaseSource {
    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      Assert.assertEquals("lastOffset", lastSourceOffset);
      Assert.assertEquals(10, maxBatchSize);
      batchMaker.addRecord(getContext().createRecord("a"));
      return "newOffset";
    }
  }

  @StageDef(
    version = 1,
    label = "Test",
    onlineHelpRefUrl = ""
  )
  public static class DummySourceOffsetCommitter extends BaseSource implements OffsetCommitter {
    boolean committed;

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      Assert.assertEquals("lastOffset", lastSourceOffset);
      Assert.assertEquals(10, maxBatchSize);
      batchMaker.addRecord(getContext().createRecord("a"));
      return "newOffset";
    }

    @Override
    public void commit(String offset) throws StageException {
      committed = true;
    }
  }

  @Test(expected = RuntimeException.class)
  public void testBuilderNoOutput() throws Exception {
    DummySource stage = new DummySource();
    SourceRunner.Builder builder = new SourceRunner.Builder(DummySource.class, stage);
    builder.build();
  }

  @Test
  public void testInitProduceDestroy() throws Exception {
    DummySource stage = new DummySource();
    SourceRunner.Builder builder = new SourceRunner.Builder(DummySource.class, stage).addOutputLane("a");
    SourceRunner runner = builder.build();
    try {
      runner.runInit();
      runner.runProduce("lastOffset", 10);
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidProduce1() throws Exception {
    DummySource stage = new DummySource();
    SourceRunner.Builder builder = new SourceRunner.Builder(DummySource.class, stage).addOutputLane("a");
    SourceRunner runner = builder.build();
    try {
      runner.runProduce("lastOffset", 10);
    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = RuntimeException.class)
  public void testInvalidProduce2() throws Exception {
    DummySource stage = new DummySource();
    SourceRunner.Builder builder = new SourceRunner.Builder(DummySource.class, stage).addOutputLane("a");
    SourceRunner runner = builder.build();
    runner.runInit();
    runner.runDestroy();
    runner.runProduce("lastOffset", 10);
  }

  @Test
  public void testProduce() throws Exception {
    DummySource stage = new DummySource();
    SourceRunner.Builder builder = new SourceRunner.Builder(DummySource.class, stage).addOutputLane("a");
    SourceRunner runner = builder.build();
    try {
      runner.runInit();
      StageRunner.Output output = runner.runProduce("lastOffset", 10);
      Assert.assertNotNull(output);
      Assert.assertEquals("newOffset", output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().size());
      Assert.assertTrue(output.getRecords().containsKey("a"));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertNotNull(output.getRecords().get("a").get(0));
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testOffsetCommitterSource() throws Exception {
    DummySourceOffsetCommitter stage = new DummySourceOffsetCommitter();
    SourceRunner.Builder builder = new SourceRunner.Builder(DummySource.class, stage).addOutputLane("a");
    SourceRunner runner = builder.build();
    try {
      runner.runInit();
      Assert.assertFalse(stage.committed);
      StageRunner.Output output = runner.runProduce("lastOffset", 10);
      Assert.assertTrue(stage.committed);
      Assert.assertNotNull(output);
      Assert.assertEquals("newOffset", output.getNewOffset());
      Assert.assertEquals(1, output.getRecords().size());
      Assert.assertTrue(output.getRecords().containsKey("a"));
      Assert.assertEquals(1, output.getRecords().get("a").size());
      Assert.assertNotNull(output.getRecords().get("a").get(0));
    } finally {
      runner.runDestroy();
    }
  }

}
