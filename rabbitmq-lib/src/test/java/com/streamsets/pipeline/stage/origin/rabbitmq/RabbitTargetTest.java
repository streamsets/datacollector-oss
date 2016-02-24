/**
 * Copyright 2016 StreamSets Inc.
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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseStage;
import com.streamsets.pipeline.lib.rabbitmq.config.Errors;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.rabbitmq.RabbitDTarget;
import com.streamsets.pipeline.stage.destination.rabbitmq.RabbitTarget;
import com.streamsets.pipeline.stage.destination.rabbitmq.RabbitTargetConfigBean;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

@PrepareForTest(RabbitTarget.class)
public class RabbitTargetTest extends BaseRabbitStageTest{

  @Before
  public void setup() {
    this.conf = new RabbitTargetConfigBean();
  }


  @Override
  protected StageRunner newStageRunner(String outputLane) {
    return new TargetRunner.Builder(RabbitDTarget.class, (Target) this.stage).build();
  }

  @Override
  protected BaseStage newStage() {
    return new RabbitTarget((RabbitTargetConfigBean) this.conf);
  }

  @Test
  public void testInvalidExpirationForAMQPProperties() throws Exception {
    RabbitTargetConfigBean config = ((RabbitTargetConfigBean)this.conf);

    config.basicPropertiesConfig.setAMQPMessageProperties = true;
    config.basicPropertiesConfig.expiration = -1;

    initStageAndRunner("output");

    List<Stage.ConfigIssue> issues = this.runner.runValidateConfigs();

    assertEquals(1L, issues.size());
    assertTrue(checkIssue(issues.get(0), Errors.RABBITMQ_09.getCode()));

  }

  @Test
  public void testInvalidTimeStampForAMQPProperties() throws Exception {
    RabbitTargetConfigBean config = ((RabbitTargetConfigBean)this.conf);

    config.basicPropertiesConfig.setAMQPMessageProperties = true;
    config.basicPropertiesConfig.setCurrentTime = false;
    config.basicPropertiesConfig.timestamp = -1L;

    initStageAndRunner("output");

    List<Stage.ConfigIssue> issues = this.runner.runValidateConfigs();

    assertEquals(1L, issues.size());
    assertTrue(checkIssue(issues.get(0), Errors.RABBITMQ_09.getCode()));

  }

  @Test(expected = StageException.class)
  public void testFailIfAutomaticRecoveryDisabled() throws Exception {
    super.testFailIfAutomaticRecoveryDisabled();

    doReturn(new ArrayList<Stage.ConfigIssue>()).when((RabbitTarget)stage).init();

    PowerMockito.doReturn(false).when(stage, "isConnected");

    ((TargetRunner)runner).runWrite(Collections.<Record>emptyList());
    runner.runDestroy();
  }
}
