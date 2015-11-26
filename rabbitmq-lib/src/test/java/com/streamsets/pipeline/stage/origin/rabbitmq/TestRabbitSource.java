/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RabbitSource.class)
public class TestRabbitSource {
  private static final Logger LOG = LoggerFactory.getLogger(TestRabbitSource.class);

  private RabbitConfigBean conf;
  private RabbitSource source;
  private SourceRunner runner;

  @Before
  public void setUp() {
    conf = new RabbitConfigBean();
  }

  @Test
  public void testWrongUriScheme() throws StageException {
    conf.uri = "blah://host:port";

    source = new RabbitSource(conf);
    runner = new SourceRunner.Builder(RabbitDSource.class, source).addOutputLane("output").build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1L, issues.size());
    assertTrue(checkIssue(issues.get(0), Errors.RABBITMQ_03.getCode(), "IllegalArgumentException"));
  }

  @Test
  public void testUriWithInvalidSyntax() throws StageException {
    conf.uri = "amqp://?sdfHsdl:#%$";

    source = new RabbitSource(conf);
    runner = new SourceRunner.Builder(RabbitDSource.class, source).addOutputLane("output").build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1L, issues.size());
    assertTrue(checkIssue(issues.get(0), Errors.RABBITMQ_03.getCode(), "URISyntaxException"));
  }

  @Test
  public void testTimeout() throws StageException {
    conf.uri = "amqp://localhost";

    source = new RabbitSource(conf);
    runner = new SourceRunner.Builder(RabbitDSource.class, source).addOutputLane("output").build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1L, issues.size());
    assertTrue(checkIssue(issues.get(0), Errors.RABBITMQ_01.getCode(), "ConnectException"));
  }

  @Test(expected = StageException.class)
  public void testFailIfAutomaticRecoveryDisabled() throws Exception {
    conf.advanced.automaticRecoveryEnabled = false;

    source = PowerMockito.spy(new RabbitSource(conf));

    doReturn(new ArrayList<Stage.ConfigIssue>()).when(source).init();
    PowerMockito.doReturn(false).when(source, "isConnected");

    runner = new SourceRunner.Builder(RabbitDSource.class, source).addOutputLane("output").build();
    runner.runInit();
    runner.runProduce(null, 1000);
    runner.runDestroy();
  }

  @Test
  public void testContinueIfAutomaticRecoveryEnabled() throws Exception {
    conf.basicConfig.maxWaitTime = 0; // Set this low so that we don't slow down the test.
    source = PowerMockito.spy(new RabbitSource(conf));

    doReturn(new ArrayList<Stage.ConfigIssue>()).when(source).init();
    PowerMockito.doReturn(false).when(source, "isConnected");

    runner = new SourceRunner.Builder(RabbitDSource.class, source).addOutputLane("output").build();
    runner.runInit();

    StageRunner.Output output = runner.runProduce(null, 1000);
    assertTrue(output.getRecords().get("output").isEmpty());
    
    runner.runDestroy();
  }

  private boolean checkIssue(Stage.ConfigIssue issue, String... expected) {
    String issueText = issue.toString();
    LOG.debug(issueText);

    for (String expectedText : expected) {
      if (!issueText.contains(expectedText)) {
        return false;
      }
    }
    return true;
  }
}
