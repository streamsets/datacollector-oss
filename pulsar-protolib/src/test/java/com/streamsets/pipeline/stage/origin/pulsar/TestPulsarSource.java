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

package com.streamsets.pipeline.stage.origin.pulsar;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseStage;
import com.streamsets.pipeline.lib.pulsar.config.PulsarErrors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;

import static com.streamsets.pipeline.stage.Utils.TestUtilsPulsar.getBasicConfig;
import static com.streamsets.pipeline.stage.Utils.TestUtilsPulsar.getStageConfigIssues;
import static com.streamsets.pipeline.stage.Utils.TestUtilsPulsar.getMessageConfig;
import static com.streamsets.pipeline.stage.Utils.TestUtilsPulsar.getSourceConfig;

public class TestPulsarSource {

  PulsarSource pulsarSource;
  PulsarMessageConsumer pulsarMessageConsumerMock;
  PulsarMessageConsumerFactory pulsarMessageConsumerFactoryMock;
  PulsarMessageConverter pulsarMessageConverterMock;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    pulsarSource = null;
    pulsarMessageConsumerMock = null;
    pulsarMessageConsumerFactoryMock = null;
    pulsarMessageConverterMock = null;
  }

  @Test
  public void testPulsarSourceConstructorSuccess() {
    pulsarSource = new PulsarSource(getBasicConfig(),getSourceConfig(), new PulsarMessageConsumerFactoryImpl(),
        new PulsarMessageConverterImpl(getMessageConfig()));
  }

  @Test(expected = NullPointerException.class)
  public void testPulsarSourceConstructorPulsarSourceConfigNull() {
    pulsarSource = new PulsarSource(getBasicConfig(),null, new PulsarMessageConsumerFactoryImpl(),
        new PulsarMessageConverterImpl(getMessageConfig()));
  }

  @Test(expected = NullPointerException.class)
  public void testPulsarSourceConstructorPulsarMessageConsumerFactoryNull() {
    pulsarSource = new PulsarSource(getBasicConfig(),getSourceConfig(), null,
        new PulsarMessageConverterImpl(getMessageConfig()));
  }

  @Test(expected = NullPointerException.class)
  public void testPulsarSourceConstructorPulsarMessageConverterNull() {
    pulsarSource = new PulsarSource(getBasicConfig(),getSourceConfig(), new PulsarMessageConsumerFactoryImpl(),
        null);
  }

  @Test
  public void testInitNoIssues() throws Exception {
    createPulsarSourceNoIssues();
    Assert.assertEquals(0, pulsarSource.init().size());
  }

  @Test
  public void testInitWithPulsarMessageConverterIssues() throws Exception {
    createPulsarSourceNoIssues();
    Mockito.when(pulsarMessageConverterMock.init(Mockito.any())).thenReturn(getStageConfigIssues());
    Assert.assertTrue(!pulsarSource.init().isEmpty());
  }

  @Test
  public void testInitWithPulsarMessageConsumerIssues() throws Exception {
    createPulsarSourceNoIssues();
    Mockito.when(pulsarMessageConsumerMock.init(Mockito.any())).thenReturn(getStageConfigIssues());
    Assert.assertTrue(!pulsarSource.init().isEmpty());
  }

  @Test
  public void testProduceSuccess() throws Exception {
    createPulsarSourceNoIssues();
    pulsarSource.init();

    try {
      Mockito.when(pulsarMessageConsumerMock.take(Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(25);
      String result = pulsarSource.produce("lastSourceOffset", 2000,
          Mockito.mock(BatchMaker.class));
      Assert.assertTrue(result.isEmpty());
    } catch (StageException e) {
      Assert.fail();
    }

  }

  @Test(expected = StageException.class)
  public void testProduceStageException() throws Exception {
    createPulsarSourceNoIssues();
    pulsarSource.init();

    Mockito.when(pulsarMessageConsumerMock.take(Mockito.any(), Mockito.any(), Mockito.anyInt())).thenThrow(
        new StageException(PulsarErrors.PULSAR_08, "dummyValue1", "dummyValue2"));
    String result = pulsarSource.produce("lastSourceOffset", 2000,
        Mockito.mock(BatchMaker.class));
    String[] splittedResult = result.contains(":::")? result.split(":::") : result.split("::");
    Assert.assertEquals(25, Integer.valueOf(splittedResult[1]).intValue());

  }

  @Test
  public void testCommitSuccess() throws Exception {
    createPulsarSourceNoIssues();
    pulsarSource.init();
    try {
      pulsarSource.commit("offset");
    } catch (StageException e) {
      e.printStackTrace();
    }
  }

  @Test(expected = StageException.class)
  public void testCommitStageException() throws Exception {
    createPulsarSourceNoIssues();
    pulsarSource.init();

    Mockito.doThrow(new StageException(PulsarErrors.PULSAR_07, "dummyValue1", "dummyValue2", "dummyValue3"))
        .when(pulsarMessageConsumerMock).ack();
    pulsarSource.commit("offset");
  }

  @Test
  public void testDestroyPulsarMessageConsumerNotNull() throws Exception {
    createPulsarSourceNoIssues();
    pulsarSource.init();
    pulsarSource.destroy();
  }

  @Test
  public void testDestroyPulsarMessageConsumerNull() throws Exception {
    createPulsarSourceNoIssues();
    pulsarSource.init();
    pulsarSource.setPulsarMessageConsumer(null);
    pulsarSource.destroy();
  }

  private void createPulsarSourceNoIssues() throws Exception {
    pulsarMessageConverterMock = Mockito.mock(PulsarMessageConverter.class);
    pulsarMessageConsumerFactoryMock = Mockito.mock(PulsarMessageConsumerFactory.class);
    pulsarMessageConsumerMock = Mockito.mock(PulsarMessageConsumer.class);

    Source.Context context = Mockito.mock(Source.Context.class);
    Mockito.when(context.isPreview()).thenReturn(false);

    Mockito.when(pulsarMessageConsumerFactoryMock.create(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(pulsarMessageConsumerMock);
    Mockito.when(pulsarMessageConverterMock.init(Mockito.any())).thenReturn(Collections.emptyList());
    Mockito.when(pulsarMessageConsumerMock.init(Mockito.any())).thenReturn(Collections.emptyList());

    pulsarSource = new PulsarSource(getBasicConfig(), getSourceConfig(), pulsarMessageConsumerFactoryMock,
        pulsarMessageConverterMock);

    Field contextField = BaseStage.class.getDeclaredField("context");
    contextField.setAccessible(true);
    contextField.set(pulsarSource, context);
  }

}
