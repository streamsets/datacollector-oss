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
package com.streamsets.pipeline.stage.origin.rabbitmq;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseStage;
import com.streamsets.pipeline.lib.rabbitmq.config.BaseRabbitConfigBean;
import com.streamsets.pipeline.lib.rabbitmq.config.Errors;
import com.streamsets.pipeline.sdk.StageRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public abstract class BaseRabbitStageTest {
  private static final Logger LOG = LoggerFactory.getLogger(BaseRabbitStageTest.class);

  protected BaseRabbitConfigBean conf;
  protected BaseStage stage;
  protected StageRunner runner;

  protected abstract StageRunner newStageRunner(String outputLane);
  protected abstract BaseStage newStage();

  protected void initStageAndRunner(String outputLane) {
    this.stage = newStage();
    this.runner = newStageRunner(outputLane);
  }

  protected boolean checkIssue(Stage.ConfigIssue issue, String... expected) {
    String issueText = issue.toString();
    LOG.debug(issueText);

    for (String expectedText : expected) {
      if (!issueText.contains(expectedText)) {
        return false;
      }
    }
    return true;
  }

  @Test
  public void testWrongUriScheme() throws StageException {
    conf.uri = "blah://host:port";

    initStageAndRunner("output");

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1L, issues.size());
    assertTrue(checkIssue(issues.get(0), Errors.RABBITMQ_03.getCode(), "IllegalArgumentException"));
  }

  @Test
  public void testUriWithInvalidSyntax() throws StageException {
    conf.uri = "amqp://?sdfHsdl:#%$";

    initStageAndRunner("output");

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1L, issues.size());
    assertTrue(checkIssue(issues.get(0), Errors.RABBITMQ_03.getCode(), "IllegalArgumentException"));
  }

  @Test
  public void testTimeout() throws StageException {
    conf.uri = "amqp://localhost";

    initStageAndRunner("output");

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1L, issues.size());
    assertTrue(checkIssue(issues.get(0), Errors.RABBITMQ_01.getCode(), "ConnectException"));
  }

  public void testFailIfAutomaticRecoveryDisabled() throws Exception {
    conf.advanced.automaticRecoveryEnabled = false;
    stage = PowerMockito.spy(newStage());
    runner = newStageRunner("output");
    runner.runInit();
  }
}
