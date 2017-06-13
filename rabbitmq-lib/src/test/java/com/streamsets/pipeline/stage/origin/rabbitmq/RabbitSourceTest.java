/**
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
package com.streamsets.pipeline.stage.origin.rabbitmq;

import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseStage;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.ArrayList;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

@PrepareForTest(RabbitSource.class)
public class RabbitSourceTest extends BaseRabbitStageTest{

  @Before
  public void setup() { this.conf = new RabbitSourceConfigBean();}

  @Override
  protected StageRunner newStageRunner(String outputLane) {
    return new SourceRunner.Builder(RabbitDSource.class, (Source) this.stage).addOutputLane("output").build();
  }

  @Override
  protected BaseStage newStage() {
    return new RabbitSource((RabbitSourceConfigBean) this.conf);
  }

  @Test(expected = StageException.class)
  public void testFailIfAutomaticRecoveryDisabled() throws Exception {
    super.testFailIfAutomaticRecoveryDisabled();

    doReturn(new ArrayList<Stage.ConfigIssue>()).when((RabbitSource)stage).init();

    PowerMockito.doReturn(false).when(stage, "isConnected");

    ((SourceRunner)runner).runProduce(null, 1000);
    runner.runDestroy();
  }

  @Test
  public void testContinueIfAutomaticRecoveryEnabled() throws Exception {
    ((RabbitSourceConfigBean)conf).basicConfig.maxWaitTime = 0; // Set this low so that we don't slow down the test.

    stage = PowerMockito.spy(newStage());

    doReturn(new ArrayList<Stage.ConfigIssue>()).when((RabbitSource)stage).init();

    PowerMockito.doReturn(false).when(stage, "isConnected");

    this.runner = newStageRunner("output");

    runner.runInit();

    StageRunner.Output output = ((SourceRunner)runner).runProduce(null, 1000);
    assertTrue(output.getRecords().get("output").isEmpty());

    runner.runDestroy();
  }

}
