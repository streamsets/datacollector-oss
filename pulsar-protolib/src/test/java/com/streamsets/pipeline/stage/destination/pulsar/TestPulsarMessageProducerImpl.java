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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.service.dataformats.DataFormatGeneratorService;
import com.streamsets.pipeline.api.service.dataformats.DataGenerator;
import com.streamsets.pipeline.stage.Utils.TestUtilsPulsar;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class TestPulsarMessageProducerImpl {

  private Target.Context context;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    context = Mockito.mock(Target.Context.class);
  }

  @Test
  public void testPulsarMessageProducerImplConstructorSuccess() {

    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(Mockito.mock(
        DataFormatGeneratorService.class));
    PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(TestUtilsPulsar.getTargetConfig(),
        context
    );
  }

  @Test(expected = NullPointerException.class)
  public void testPulsarMessageProducerImplConstructorPulsarTargetConfigNull() {
    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(Mockito.mock(
        DataFormatGeneratorService.class));
    PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(null, context);
  }

  @Test(expected = NullPointerException.class)
  public void testPulsarMessageProducerImplConstructorContextNull() {
    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(Mockito.mock(
        DataFormatGeneratorService.class));
    PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(TestUtilsPulsar.getTargetConfig(),
        null
    );
  }

  @Test(expected = NullPointerException.class)
  public void testPulsarMessageProducerImplConstructorDataFormatGeneratorServiceNull() {
    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(null);
    PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(TestUtilsPulsar.getTargetConfig(),
        context
    );
  }

  @Test
  public void testInitNoIssues() {
    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(Mockito.mock(
        DataFormatGeneratorService.class));
    Mockito.when(context.createELEval(Mockito.any())).thenReturn(Mockito.mock(ELEval.class));

    PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(TestUtilsPulsar.getTargetConfig(),
        context
    );
    List<Stage.ConfigIssue> issues = pulsarMessageProducer.init(context);
    Assert.assertTrue(issues.isEmpty());
  }

  @Test
  public void testInitWithPulsarClientIssue() {
    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(Mockito.mock(
        DataFormatGeneratorService.class));
    Mockito.when(context.createELEval(Mockito.any())).thenReturn(Mockito.mock(ELEval.class));

    // prepare PulsarTargetConfig
    PulsarTargetConfig targetConfig = TestUtilsPulsar.getTargetConfig();
    targetConfig.securityConfig.caCertPem = null;
    targetConfig.securityConfig.tlsEnabled = true;

    PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(targetConfig, context);
    List<Stage.ConfigIssue> issues = pulsarMessageProducer.init(context);
    Assert.assertTrue(!issues.isEmpty());
  }

  @Test
  public void testPutBatchNull() {
    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(Mockito.mock(
        DataFormatGeneratorService.class));
    Mockito.when(context.createELEval(Mockito.any())).thenReturn(Mockito.mock(ELEval.class));

    PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(TestUtilsPulsar.getTargetConfig(),
        context
    );
    List<Stage.ConfigIssue> issues = pulsarMessageProducer.init(context);
    Assert.assertTrue(issues.isEmpty());

    try {
      pulsarMessageProducer.put(null);
    } catch (StageException e) {
      Assert.fail();
    }
  }

  @Test
  public void testPutBatchRecords() {
    DataFormatGeneratorService dataFormatGeneratorService = Mockito.mock(DataFormatGeneratorService.class);
    try {
      Mockito.when(dataFormatGeneratorService.getGenerator(Mockito.any()))
             .thenReturn(Mockito.mock(DataGenerator.class));
    } catch (IOException e) {
      Assert.fail();
    }
    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(dataFormatGeneratorService);
    Mockito.when(context.createELEval(Mockito.any())).thenReturn(Mockito.mock(ELEval.class));
    Mockito.when(context.createELVars()).thenReturn(Mockito.mock(ELVars.class));

    try {
      PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(TestUtilsPulsar.getTargetConfig(),
          context
      );
      List<Stage.ConfigIssue> issues = pulsarMessageProducer.init(context);
      Assert.assertTrue(issues.isEmpty());

      // Create TypedMessageBuilder mock
      TypedMessageBuilder typedMessageBuilderMock = Mockito.mock(TypedMessageBuilder.class);
      Mockito.when(typedMessageBuilderMock.key(Mockito.anyString())).thenReturn(typedMessageBuilderMock);
      Mockito.when(typedMessageBuilderMock.value(Mockito.any())).thenReturn(typedMessageBuilderMock);

      // Create Producer mock
      Producer producerMock = Mockito.mock(Producer.class);
      Mockito.when(producerMock.newMessage()).thenReturn(typedMessageBuilderMock);

      //modify message producers mock
      LoadingCache<String, Producer> messageProducersMock = Mockito.mock(LoadingCache.class);
      Mockito.when(messageProducersMock.get(Mockito.any())).thenReturn(producerMock);
      pulsarMessageProducer.setMessageProducers(messageProducersMock);

      Batch batch = TestUtilsPulsar.getBatch();
      pulsarMessageProducer.put(batch);
    } catch (StageException e) {
      Assert.fail();
    } catch (ExecutionException e) {
      Assert.fail();
    }
  }

  @Test(expected = StageException.class)
  public void testPutStageException() throws StageException {
    DataFormatGeneratorService dataFormatGeneratorService = Mockito.mock(DataFormatGeneratorService.class);

    try {
      Mockito.when(dataFormatGeneratorService.getGenerator(Mockito.any())).thenThrow(new IOException(
          "put stage exception test"));
    } catch (IOException e) {
      Assert.fail();
    }

    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(dataFormatGeneratorService);
    Mockito.when(context.createELEval(Mockito.any())).thenReturn(Mockito.mock(ELEval.class));
    Mockito.when(context.createELVars()).thenReturn(Mockito.mock(ELVars.class));

    PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(TestUtilsPulsar.getTargetConfig(),
        context
    );
    List<Stage.ConfigIssue> issues = pulsarMessageProducer.init(context);
    Assert.assertTrue(issues.isEmpty());

    Batch batch = TestUtilsPulsar.getBatch();
    pulsarMessageProducer.put(batch);
  }

  @Test
  public void testCloseSuccess() {
    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(Mockito.mock(
        DataFormatGeneratorService.class));
    Mockito.when(context.createELEval(Mockito.any())).thenReturn(Mockito.mock(ELEval.class));

    PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(TestUtilsPulsar.getTargetConfig(),
        context
    );
    List<Stage.ConfigIssue> issues = pulsarMessageProducer.init(context);
    Assert.assertTrue(issues.isEmpty());

    pulsarMessageProducer.close();
  }

  @Test
  public void testCloseProducerPulsarClientException() {
    DataFormatGeneratorService dataFormatGeneratorService = Mockito.mock(DataFormatGeneratorService.class);
    try {
      Mockito.when(dataFormatGeneratorService.getGenerator(Mockito.any()))
             .thenReturn(Mockito.mock(DataGenerator.class));
    } catch (IOException e) {
      Assert.fail();
    }
    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(dataFormatGeneratorService);
    Mockito.when(context.createELEval(Mockito.any())).thenReturn(Mockito.mock(ELEval.class));
    Mockito.when(context.createELVars()).thenReturn(Mockito.mock(ELVars.class));

    try {
      PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(
          TestUtilsPulsar.getTargetConfig(),
          context
      );
      List<Stage.ConfigIssue> issues = pulsarMessageProducer.init(context);
      Assert.assertTrue(issues.isEmpty());

      //modify message producers
      LoadingCache<String, Producer> messageProducersTest = CacheBuilder.newBuilder().expireAfterAccess(15,
          TimeUnit.MINUTES
      ).build(new CacheLoader<String, Producer>() {
        @Override
        public Producer load(String key) throws Exception {
          Producer producerMock = Mockito.mock(Producer.class);
          Mockito.doThrow(new PulsarClientException("test pulsar message producer close producer pulsar client " +
              "exception")).when(producerMock).close();
          return producerMock;
        }
      });
      messageProducersTest.get("fakeGetProducer");
      pulsarMessageProducer.setMessageProducers(messageProducersTest);

      pulsarMessageProducer.close();
    } catch (ExecutionException e) {
      Assert.fail();
    }
  }

  @Test
  public void testCloseClientPulsarClientException() {
    DataFormatGeneratorService dataFormatGeneratorService = Mockito.mock(DataFormatGeneratorService.class);
    try {
      Mockito.when(dataFormatGeneratorService.getGenerator(Mockito.any()))
             .thenReturn(Mockito.mock(DataGenerator.class));
    } catch (IOException e) {
      Assert.fail();
    }
    Mockito.when(context.getService(DataFormatGeneratorService.class)).thenReturn(dataFormatGeneratorService);
    Mockito.when(context.createELEval(Mockito.any())).thenReturn(Mockito.mock(ELEval.class));
    Mockito.when(context.createELVars()).thenReturn(Mockito.mock(ELVars.class));

    try {
      PulsarMessageProducerImpl pulsarMessageProducer = new PulsarMessageProducerImpl(
          TestUtilsPulsar.getTargetConfig(),
          context
      );
      List<Stage.ConfigIssue> issues = pulsarMessageProducer.init(context);
      Assert.assertTrue(issues.isEmpty());

      PulsarClient pulsarClientMock = Mockito.mock(PulsarClient.class);
      Mockito.doThrow(new PulsarClientException("test pulsar message producer close pulsar client close pulsar client" +
          " " +
          "exception")).when(pulsarClientMock).close();
      pulsarMessageProducer.setPulsarClient(pulsarClientMock);

      pulsarMessageProducer.close();
    }  catch (PulsarClientException e) {
      Assert.fail();
    }
  }

}
