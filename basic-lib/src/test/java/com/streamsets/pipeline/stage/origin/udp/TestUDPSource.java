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
package com.streamsets.pipeline.stage.origin.udp;

import com.google.common.collect.Lists;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DatagramMode;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TestUDPSource extends BaseUDPSourceTest {
  private SourceRunner runner;
  private TUDPSource source;

  @StageDef(
    version = 1,
    label = "Test stage",
    onlineHelpRefUrl = ""
  )
  public static class TUDPSource extends UDPSource {

    boolean produceCalled;

    public TUDPSource(UDPSourceConfigBean conf) {
      super(conf);
    }

    @Override
    public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
      produceCalled = true;
      return super.produce(lastSourceOffset, maxBatchSize, batchMaker);
    }
  }

  @Test
  public void testPrivilegedPort() throws Exception {
    List<String> ports = Lists.newArrayList("514", "10000");

    UDPSourceConfigBean conf = new UDPSourceConfigBean();
    conf.dataFormat = DatagramMode.RAW_DATA;
    conf.collectdCharset = UTF8;
    conf.syslogCharset = UTF8;
    conf.rawDataCharset = UTF8;
    conf.ports = ports;
    conf.batchSize = 1000;

    TUDPSource source = new TUDPSource(conf);
    SourceRunner runner = new SourceRunner.Builder(TUDPSource.class, source).addOutputLane("lane").build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    if (System.getProperty("user.name").equals("root")) {
      Assert.assertEquals(0, issues.size());
    } else {
      Assert.assertEquals(1, issues.size());
    }
  }

  @Override
  protected void initializeRunner(UDPSourceConfigBean conf, int numThreads) throws StageException {
    source = new TUDPSource(conf);
    runner = new SourceRunner.Builder(TUDPSource.class, source).addOutputLane("lane").build();
    runner.runInit();
  }

  @Override
  protected void runProduce(DatagramMode dataFormat, List<String> ports) throws Exception {

    StageRunner.Output output = runner.runProduce(null, 6);
    Assert.assertTrue(source.produceCalled);
    List<Record> records = getOutputRecords(output);
    assertRecordsInBatch(dataFormat, 0, records);

    output = runner.runProduce(null, 14);
    Assert.assertTrue(source.produceCalled);
    records = getOutputRecords(output);
    assertRecordsInBatch(dataFormat, 6, records);

    output = runner.runProduce(null, 1);
    Assert.assertTrue(source.produceCalled);
    records = getOutputRecords(output);
    assertRecordsInBatch(dataFormat, 20, records);

    Assert.assertEquals(String.valueOf(records), 0, records.size());
  }

  @Override
  protected void destroyRunner() throws StageException {
    runner.runDestroy();
  }
}
