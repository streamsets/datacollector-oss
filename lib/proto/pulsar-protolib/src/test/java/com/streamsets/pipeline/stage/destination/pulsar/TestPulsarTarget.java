/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.pipeline.stage.destination.pulsar;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.pulsar.config.PulsarErrors;
import com.streamsets.pipeline.stage.Utils.TestUtilsPulsar;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static com.streamsets.pipeline.stage.Utils.TestUtilsPulsar.getStageConfigIssues;
import static com.streamsets.pipeline.stage.Utils.TestUtilsPulsar.getTargetConfig;

public class TestPulsarTarget {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testPulsarTargetConstructorSuccess() {
    PulsarTarget pulsarTarget = new PulsarTarget(getTargetConfig(), new PulsarMessageProducerFactoryImpl());
  }

  @Test(expected = NullPointerException.class)
  public void testPulsarTargetConstructorPulsarTargetConfigNull() {
    PulsarTarget pulsarTarget = new PulsarTarget(null, new PulsarMessageProducerFactoryImpl());
  }

  @Test(expected = NullPointerException.class)
  public void testPulsarTargetConstructorPulsarMessageProducerFactoryNull() {
    PulsarTarget pulsarTarget = new PulsarTarget(getTargetConfig(), null);
  }

  @Test
  public void testInitNoIssues() {
    PulsarMessageProducer pulsarMessageProducer = Mockito.mock(PulsarMessageProducer.class);
    Mockito.when(pulsarMessageProducer.init(Mockito.any())).thenReturn(Collections.emptyList());

    PulsarMessageProducerFactory pulsarMessageProducerFactory = Mockito.mock(PulsarMessageProducerFactory.class);
    Mockito.when(pulsarMessageProducerFactory.create(Mockito.any(), Mockito.any()))
        .thenReturn(pulsarMessageProducer);

    PulsarTarget pulsarTarget = new PulsarTarget(getTargetConfig(), pulsarMessageProducerFactory);
    List<Stage.ConfigIssue> issues = pulsarTarget.init();
    Assert.assertTrue(issues.isEmpty());
  }

  @Test
  public void testInitWithIssues() {
    PulsarMessageProducer pulsarMessageProducer = Mockito.mock(PulsarMessageProducer.class);
    Mockito.when(pulsarMessageProducer.init(Mockito.any())).thenReturn(getStageConfigIssues());

    PulsarMessageProducerFactory pulsarMessageProducerFactory = Mockito.mock(PulsarMessageProducerFactory.class);
    Mockito.when(pulsarMessageProducerFactory.create(Mockito.any(), Mockito.any()))
        .thenReturn(pulsarMessageProducer);

    PulsarTarget pulsarTarget = new PulsarTarget(getTargetConfig(), pulsarMessageProducerFactory);
    List<Stage.ConfigIssue> issues = pulsarTarget.init();
    Assert.assertTrue(!issues.isEmpty());
  }

  @Test
  public void testWriteNullBatchSuccess() {
    PulsarMessageProducer pulsarMessageProducer = Mockito.mock(PulsarMessageProducer.class);
    Mockito.when(pulsarMessageProducer.init(Mockito.any())).thenReturn(Collections.emptyList());

    PulsarMessageProducerFactory pulsarMessageProducerFactory = Mockito.mock(PulsarMessageProducerFactory.class);
    Mockito.when(pulsarMessageProducerFactory.create(Mockito.any(), Mockito.any()))
        .thenReturn(pulsarMessageProducer);

    PulsarTarget pulsarTarget = new PulsarTarget(getTargetConfig(), pulsarMessageProducerFactory);
    List<Stage.ConfigIssue> issues = pulsarTarget.init();
    Assert.assertTrue(issues.isEmpty());

    try {
      pulsarTarget.write(null);
    } catch (StageException e) {
      Assert.fail();
    }
  }

  @Test
  public void testWriteBatchSuccess() {
    PulsarMessageProducer pulsarMessageProducer = Mockito.mock(PulsarMessageProducer.class);
    Mockito.when(pulsarMessageProducer.init(Mockito.any())).thenReturn(Collections.emptyList());

    PulsarMessageProducerFactory pulsarMessageProducerFactory = Mockito.mock(PulsarMessageProducerFactory.class);
    Mockito.when(pulsarMessageProducerFactory.create(Mockito.any(), Mockito.any()))
        .thenReturn(pulsarMessageProducer);

    PulsarTarget pulsarTarget = new PulsarTarget(getTargetConfig(), pulsarMessageProducerFactory);
    List<Stage.ConfigIssue> issues = pulsarTarget.init();
    Assert.assertTrue(issues.isEmpty());

    try {
      Batch batch = TestUtilsPulsar.getBatch();
      pulsarTarget.write(batch);
    } catch (StageException e) {
      Assert.fail();
    }
  }

  @Test(expected = StageException.class)
  public void testWriteStageException() throws StageException {
    PulsarMessageProducer pulsarMessageProducer = Mockito.mock(PulsarMessageProducer.class);
    Mockito.when(pulsarMessageProducer.init(Mockito.any())).thenReturn(Collections.emptyList());
    try {
      Mockito.doThrow(new StageException(PulsarErrors.PULSAR_00, "dummyValue1", "dummyValue2")).when
          (pulsarMessageProducer).put(Mockito.any());
    } catch (StageException e) {
      Assert.fail();
    }

    PulsarMessageProducerFactory pulsarMessageProducerFactory = Mockito.mock(PulsarMessageProducerFactory.class);
    Mockito.when(pulsarMessageProducerFactory.create(Mockito.any(), Mockito.any()))
        .thenReturn(pulsarMessageProducer);

    PulsarTarget pulsarTarget = new PulsarTarget(getTargetConfig(), pulsarMessageProducerFactory);
    List<Stage.ConfigIssue> issues = pulsarTarget.init();
    Assert.assertTrue(issues.isEmpty());

    pulsarTarget.write(null);
    Assert.fail();
  }

  @Test
  public void testDestroyPulsarMessageProducerNotNull() {
    PulsarMessageProducer pulsarMessageProducer = Mockito.mock(PulsarMessageProducer.class);
    Mockito.when(pulsarMessageProducer.init(Mockito.any())).thenReturn(Collections.emptyList());

    PulsarMessageProducerFactory pulsarMessageProducerFactory = Mockito.mock(PulsarMessageProducerFactory.class);
    Mockito.when(pulsarMessageProducerFactory.create(Mockito.any(), Mockito.any()))
        .thenReturn(pulsarMessageProducer);

    PulsarTarget pulsarTarget = new PulsarTarget(getTargetConfig(), pulsarMessageProducerFactory);
    List<Stage.ConfigIssue> issues = pulsarTarget.init();
    Assert.assertTrue(issues.isEmpty());
    pulsarTarget.destroy();
  }

}
