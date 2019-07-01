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

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.BaseStage;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.rabbitmq.config.Errors;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.rabbitmq.RabbitDTarget;
import com.streamsets.pipeline.stage.destination.rabbitmq.RabbitTarget;
import com.streamsets.pipeline.stage.destination.rabbitmq.RabbitTargetConfigBean;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
    config.basicPropertiesConfig.setExpiration = true;
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


  private void checkMessageDeliverySemantics(
      final RabbitTargetConfigBean config,
      int expectedNumberOfDeliveries,
      List<Record> records
  ) throws Exception{
    final AtomicInteger noOfDeliveries = new AtomicInteger(0);
    PowerMockito.replace(
        MemberMatcher.method(
            RabbitTarget.class,
            "handleDelivery"
        )
    ).with(
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            noOfDeliveries.incrementAndGet();
            //call the real getOffsetsLag private method
            return null;
          }
        }
    );


    stage = PowerMockito.spy(newStage());
    doReturn(new ArrayList<Stage.ConfigIssue>()).when((RabbitTarget)stage).init();
    runner = newStageRunner("output");

    config.dataFormat = DataFormat.JSON;
    config.dataFormatConfig.jsonMode = JsonMode.MULTIPLE_OBJECTS;
    config.dataFormatConfig.init(runner.getContext(), config.dataFormat, "", "", new ArrayList<Stage.ConfigIssue>());
    Whitebox.setInternalState(stage, "generatorFactory", config.dataFormatConfig.getDataGeneratorFactory());


    runner.runInit();
    ((TargetRunner) runner).runWrite(records);
    Assert.assertEquals(expectedNumberOfDeliveries, noOfDeliveries.get());
    runner.runDestroy();
  }

  @Test
  public void testSingleMessagePerBatchAndRecord() throws Exception {
    final RabbitTargetConfigBean config = ((RabbitTargetConfigBean)this.conf);

    Map<String, Field> map = new LinkedHashMap<>();
    map.put("char", Field.create(Field.Type.CHAR, 'c'));
    map.put("int", Field.create(Field.Type.INTEGER, 1));
    map.put("string", Field.create(Field.Type.STRING, "string"));

    Record record1 = RecordCreator.create("s", "s:1");
    record1.set(Field.create(map));
    Record record2 = RecordCreator.create("s", "s:2");
    record1.set(Field.create(map));
    Record record3 = RecordCreator.create("s", "s:3");
    record1.set(Field.create(map));


    //Run with singleMessagePerBatch
    config.singleMessagePerBatch = true;
    checkMessageDeliverySemantics(config, 1, Arrays.asList(record1, record2, record3));

    //Run with singleMessagePerRecord
    config.singleMessagePerBatch = false;
    checkMessageDeliverySemantics(config, 3, Arrays.asList(record1, record2, record3));
  }
}
