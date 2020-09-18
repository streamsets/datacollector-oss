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
import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.pulsar.config.PulsarErrors;
import com.streamsets.pipeline.stage.Utils.TestUtilsPulsar;
import com.streamsets.pipeline.stage.origin.lib.BasicConfig;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.regex.Pattern;

@RunWith(PowerMockRunner.class)
@PrepareForTest(PulsarClient.class)
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestPulsarMessageConsumerImpl {

  private Source.Context contextMock;
  private ClientBuilder pulsarClientBuilderMock;
  private PulsarMessageConsumerImpl pulsarMessageConsumerImplMock;
  private PulsarClient pulsarClientMock;
  private PulsarMessageConverter pulsarMessageConverterMock;
  private ConsumerBuilder consumerBuilderMock;
  private Consumer messageConsumerMock;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUpBefore() {
    pulsarMessageConsumerImplMock = null;
    contextMock = null;
    pulsarClientMock = null;
    consumerBuilderMock = null;
    pulsarMessageConverterMock = null;
    messageConsumerMock = null;
  }

  @Test
  public void testPulsarMessageConsumerImplConstructorSuccess() {
    pulsarMessageConsumerImplMock = new PulsarMessageConsumerImpl(Mockito.mock(BasicConfig.class),
        Mockito.mock(PulsarSourceConfig.class),
        Mockito.mock(PulsarMessageConverter.class)
    );
  }

  @Test
  public void testInitNoIssues() {
    createPulsarMessageConsumerImplNoIssues();

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());
  }

  @Test
  public void testInitWithPulsarClientCreationIssues() {
    createPulsarMessageConsumerImplNoIssues();

    Mockito.when(contextMock.createConfigIssue(Mockito.anyString(),
        Mockito.anyString(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any()
    )).thenReturn(getConfigIssue(PulsarErrors.PULSAR_00));


    try {
      Mockito.when(pulsarClientBuilderMock.build()).thenThrow(new PulsarClientException("pulsar client builder" +
          "build exception"));
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking pulsarClientBuilderMock.build method in testInitWithPulsarClientCreationIssues");
    }

    pulsarMessageConsumerImplMock = new PulsarMessageConsumerImpl(TestUtilsPulsar.getBasicConfig(),
        TestUtilsPulsar.getSourceConfig(),
        pulsarMessageConverterMock
    );

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(PulsarErrors.PULSAR_00.getCode().equals(issues.get(0).toString()));
  }

  @Test
  public void testInitWithPulsarClientSubscribeOneTopicIssues() {
    createPulsarMessageConsumerImplNoIssues();

    Mockito.when(contextMock.createConfigIssue(Mockito.anyString(),
        Mockito.anyString(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any()
    ))
           .thenReturn(getConfigIssue(PulsarErrors.PULSAR_10));

    try {
      Mockito.when(consumerBuilderMock.subscribe()).thenThrow(new PulsarClientException("Consumer builder subscribe" +
          " exception"));
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking ConsumerBuilder.subscribe method in testInitWithPulsarClientSubscribeOneTopicIssues");
    }

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(PulsarErrors.PULSAR_10.getCode().equals(issues.get(0).toString()));
  }

  @Test
  public void testInitWithPulsarClientSubscribeTopicsListIssues() {
    PulsarSourceConfig customPulsarSourceConfig = TestUtilsPulsar.getSourceConfig();
    customPulsarSourceConfig.pulsarTopicsSelector = PulsarTopicsSelector.TOPICS_LIST;
    customPulsarSourceConfig.topicsList = TestUtilsPulsar.getTopicsList();

    createPulsarMessageConsumerImplNoIssues(customPulsarSourceConfig);

    Mockito.when(contextMock.createConfigIssue(Mockito.anyString(),
        Mockito.anyString(),
        Mockito.any(),
        Mockito.any(),
        Mockito.any()
    )).thenReturn(getConfigIssue(PulsarErrors.PULSAR_06));

    try {
      Mockito.when(consumerBuilderMock.subscribe()).thenThrow(new PulsarClientException("Consumer builder subscribe" +
          " exception"));
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking ConsumerBuilder.subscribe method in " +
          "testInitWithPulsarClientSubscribeTopicsListIssues");
    }

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(PulsarErrors.PULSAR_06.getCode().equals(issues.get(0).toString()));
  }

  @Test
  public void testTakeSuccessOneTopic() throws StageException {
    createPulsarMessageConsumerImplNoIssues();

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any())).thenReturn(null);
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking Consumer.receive method in testTakeSuccessOneTopic");
    }

    Assert.assertEquals(0, pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1));
    Assert.assertNull(pulsarMessageConsumerImplMock.getLastSentButNotACKMessage());

    try {
      Mockito.verify(messageConsumerMock, Mockito.atLeast(1)).receive(Mockito.anyInt(), Mockito.any());
      Mockito.verify(pulsarMessageConverterMock, Mockito.times(0)).convert(Mockito.any(),
          Mockito.any(),
          Mockito.any(),
          Mockito.any()
      );
    } catch (PulsarClientException e) {
      Assert.fail("Error verifying number of calls to Consumer.receive method in testTakeSuccessOneTopic");
    }

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any()))
             .thenReturn(TestUtilsPulsar.getPulsarMessage());
      Mockito.when(pulsarMessageConverterMock.convert(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
             .thenReturn(1);
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking Consumer.receive method in testTakeSuccessOneTopic");
    }

    Assert.assertEquals(1, pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1));
    Assert.assertEquals(TestUtilsPulsar.getPulsarMessage().getMessageId(),
        pulsarMessageConsumerImplMock.getLastSentButNotACKMessage().getMessageId()
    );

    try {
      Mockito.verify(messageConsumerMock, Mockito.atLeast(1)).receive(Mockito.anyInt(), Mockito.any());
      Mockito.verify(pulsarMessageConverterMock, Mockito.times(1)).convert(Mockito.any(),
          Mockito.any(),
          Mockito.any(),
          Mockito.any()
      );
    } catch (PulsarClientException e) {
      Assert.fail("Error verifying number of calls to Consumer.receive method in testTakeSuccessOneTopic");
    }
  }

  @Test
  public void testTakeSuccessOneTopicFailoverSubscription() throws StageException {
    PulsarSourceConfig customPulsarSourceConfig = TestUtilsPulsar.getSourceConfig();
    customPulsarSourceConfig.subscriptionType = PulsarSubscriptionType.FAILOVER;

    createPulsarMessageConsumerImplNoIssues(customPulsarSourceConfig);

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any()))
             .thenReturn(TestUtilsPulsar.getPulsarMessage());
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking Consumer.receive method in testTakeSuccessOneTopicFailoverSubscription");
    }
    Mockito.when(pulsarMessageConverterMock.convert(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
           .thenReturn(1);

    Assert.assertEquals(1, pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1));
    Assert.assertNull(pulsarMessageConsumerImplMock.getLastSentButNotACKMessage());
    Assert.assertEquals(1, pulsarMessageConsumerImplMock.getSentButNotACKMessages().size());

    try {
      Mockito.verify(messageConsumerMock, Mockito.atLeast(1)).receive(Mockito.anyInt(), Mockito.any());
      Mockito.verify(pulsarMessageConverterMock, Mockito.times(1)).convert(Mockito.any(),
          Mockito.any(),
          Mockito.any(),
          Mockito.any()
      );
    } catch (PulsarClientException e) {
      Assert.fail("Error verifying number of calls to Consumer.receive method in " +
          "testTakeSuccessOneTopicFailoverSubscription");
    }
  }

  @Test
  public void testTakeSuccessTopicsList() throws StageException {
    PulsarSourceConfig customPulsarSourceConfig = TestUtilsPulsar.getSourceConfig();
    customPulsarSourceConfig.pulsarTopicsSelector = PulsarTopicsSelector.TOPICS_LIST;
    customPulsarSourceConfig.topicsList = TestUtilsPulsar.getTopicsList();

    createPulsarMessageConsumerImplNoIssues(customPulsarSourceConfig);

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any())).thenReturn(null);
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking Consumer.receive method in testTakeSuccessTopicsList");
    }

    Assert.assertEquals(0, pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1));
    Assert.assertEquals(0, pulsarMessageConsumerImplMock.getSentButNotACKMessages().size());

    try {
      Mockito.verify(messageConsumerMock, Mockito.atLeast(1)).receive(Mockito.anyInt(), Mockito.any());
      Mockito.verify(pulsarMessageConverterMock, Mockito.times(0)).convert(Mockito.any(),
          Mockito.any(),
          Mockito.any(),
          Mockito.any()
      );
    } catch (PulsarClientException e) {
      Assert.fail("Error verifying number of calls to Consumer.receive method in testTakeSuccessTopicsList");
    }

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any()))
             .thenReturn(TestUtilsPulsar.getPulsarMessage());
      Mockito.when(pulsarMessageConverterMock.convert(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
             .thenReturn(1);
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking Consumer.receive method in testTakeSuccessTopicsList");
    }

    Assert.assertEquals(1, pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1));
    Assert.assertEquals(1, pulsarMessageConsumerImplMock.getSentButNotACKMessages().size());
    Assert.assertEquals(TestUtilsPulsar.getPulsarMessage().getMessageId(),
        pulsarMessageConsumerImplMock.getSentButNotACKMessages().get(0).getMessageId()
    );

    try {
      Mockito.verify(messageConsumerMock, Mockito.atLeast(1)).receive(Mockito.anyInt(), Mockito.any());
      Mockito.verify(pulsarMessageConverterMock, Mockito.times(1)).convert(Mockito.any(),
          Mockito.any(),
          Mockito.any(),
          Mockito.any()
      );
    } catch (PulsarClientException e) {
      Assert.fail("Error verifying number of calls to Consumer.receive method in testTakeSuccessTopicsList");
    }
  }

  @Test
  public void testTakeSuccessTopicsPattern() throws StageException {
    PulsarSourceConfig customPulsarSourceConfig = TestUtilsPulsar.getSourceConfig();
    customPulsarSourceConfig.pulsarTopicsSelector = PulsarTopicsSelector.TOPICS_PATTERN;

    createPulsarMessageConsumerImplNoIssues(customPulsarSourceConfig);

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any())).thenReturn(null);
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking Consumer.receive method in testTakeSuccessTopicsPattern");
    }

    Assert.assertEquals(0, pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1));
    Assert.assertEquals(0, pulsarMessageConsumerImplMock.getSentButNotACKMessages().size());

    try {
      Mockito.verify(messageConsumerMock, Mockito.atLeast(1)).receive(Mockito.anyInt(), Mockito.any());
      Mockito.verify(pulsarMessageConverterMock, Mockito.times(0)).convert(Mockito.any(),
          Mockito.any(),
          Mockito.any(),
          Mockito.any()
      );
    } catch (PulsarClientException e) {
      Assert.fail("Error verifying number of calls to Consumer.receive method in testTakeSuccessTopicsPattern");
    }

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any()))
             .thenReturn(TestUtilsPulsar.getPulsarMessage());
      Mockito.when(pulsarMessageConverterMock.convert(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
             .thenReturn(1);
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking Consumer.receive method in testTakeSuccessTopicsPattern");
    }

    Assert.assertEquals(1, pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1));
    Assert.assertEquals(1, pulsarMessageConsumerImplMock.getSentButNotACKMessages().size());
    Assert.assertEquals(TestUtilsPulsar.getPulsarMessage().getMessageId(),
        pulsarMessageConsumerImplMock.getSentButNotACKMessages().get(0).getMessageId()
    );

    try {
      Mockito.verify(messageConsumerMock, Mockito.atLeast(1)).receive(Mockito.anyInt(), Mockito.any());
      Mockito.verify(pulsarMessageConverterMock, Mockito.times(1)).convert(Mockito.any(),
          Mockito.any(),
          Mockito.any(),
          Mockito.any()
      );
    } catch (PulsarClientException e) {
      Assert.fail("Error verifying number of calls to Consumer.receive method in testTakeSuccessTopicsList");
    }
  }

  @Test
  public void testTakeStageExceptionMessageConsumerReceive() {
    createPulsarMessageConsumerImplNoIssues();

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any())).thenThrow(new PulsarClientException(
          "Pulsar client exception in testTakeStageExceptionMessageConsumerReceive"));
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking Consumer.receive method in testTakeStageExceptionMessageConsumerReceive");
    }

    try {
      pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1);
      Assert.fail();
    } catch (StageException e) {
      Assert.assertTrue(e.toString().contains(PulsarErrors.PULSAR_08.getCode()));
    }
  }

  @Test(expected = StageException.class)
  public void testTakeStageExceptionPulsarMessageConverterConvert() throws StageException {
    createPulsarMessageConsumerImplNoIssues();

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any()))
             .thenReturn(TestUtilsPulsar.getPulsarMessage());
      Mockito.when(pulsarMessageConverterMock.convert(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
             .thenThrow(new StageException(PulsarErrors.PULSAR_09, "messageId", "param1", "param2"));
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking Consumer.receive method in testTakeStageExceptionPulsarMessageConverterConvert");
    }
    pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1);
    Assert.fail();
  }

  @Test
  public void ackSuccessOneTopic() throws PulsarClientException {
    createPulsarMessageConsumerImplNoIssues();

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any()))
             .thenReturn(TestUtilsPulsar.getPulsarMessage());
      Mockito.when(pulsarMessageConverterMock.convert(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
             .thenReturn(1);
      pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1);
    } catch (PulsarClientException | StageException e) {
      Assert.fail("Error mocking Consumer.receive method in ackSuccessOneTopic");
    }

    Assert.assertNotNull(pulsarMessageConsumerImplMock.getLastSentButNotACKMessage());

    try {
      pulsarMessageConsumerImplMock.ack();
    } catch (StageException e) {
      Assert.fail("Ack threw an unexpected StageException");
    }

    Mockito.verify(messageConsumerMock, Mockito.times(1)).acknowledgeCumulative(Mockito.any(MessageId.class));
    Assert.assertNull(pulsarMessageConsumerImplMock.getLastSentButNotACKMessage());
  }

  @Test
  public void ackSuccessOneTopicFailoverSubscription() throws PulsarClientException {
    PulsarSourceConfig customPulsarSourceConfig = TestUtilsPulsar.getSourceConfig();
    customPulsarSourceConfig.subscriptionType = PulsarSubscriptionType.FAILOVER;

    createPulsarMessageConsumerImplNoIssues(customPulsarSourceConfig);

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any()))
             .thenReturn(TestUtilsPulsar.getPulsarMessage());
      Mockito.when(pulsarMessageConverterMock.convert(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
             .thenReturn(1);
      pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1);
    } catch (PulsarClientException | StageException e) {
      Assert.fail("Error mocking Consumer.receive method in ackSuccessOneTopicFailoverSubscription");
    }

    Assert.assertNull(pulsarMessageConsumerImplMock.getLastSentButNotACKMessage());
    Assert.assertEquals(1, pulsarMessageConsumerImplMock.getSentButNotACKMessages().size());

    try {
      pulsarMessageConsumerImplMock.ack();
    } catch (StageException e) {
      Assert.fail("Ack threw an unexpected StageException");
    }

    Mockito.verify(messageConsumerMock, Mockito.times(1)).acknowledge(Mockito.any(MessageId.class));
    Assert.assertNull(pulsarMessageConsumerImplMock.getLastSentButNotACKMessage());
    Assert.assertEquals(0, pulsarMessageConsumerImplMock.getSentButNotACKMessages().size());
  }

  @Test
  public void ackSuccessTopicsList() throws PulsarClientException {
    PulsarSourceConfig customPulsarSourceConfig = TestUtilsPulsar.getSourceConfig();
    customPulsarSourceConfig.pulsarTopicsSelector = PulsarTopicsSelector.TOPICS_LIST;
    customPulsarSourceConfig.topicsList = TestUtilsPulsar.getTopicsList();

    createPulsarMessageConsumerImplNoIssues(customPulsarSourceConfig);

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any()))
             .thenReturn(TestUtilsPulsar.getPulsarMessage());
      Mockito.when(pulsarMessageConverterMock.convert(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
             .thenReturn(1);
      pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1);
    } catch (PulsarClientException | StageException e) {
      Assert.fail("Error mocking Consumer.receive method in ackSuccessTopicsList");
    }

    Assert.assertFalse(pulsarMessageConsumerImplMock.getSentButNotACKMessages().isEmpty());

    try {
      pulsarMessageConsumerImplMock.ack();
    } catch (StageException e) {
      Assert.fail("Ack threw an unexpected StageException");
    }

    Mockito.verify(messageConsumerMock, Mockito.times(1)).acknowledge(Mockito.any(MessageId.class));
    Assert.assertTrue(pulsarMessageConsumerImplMock.getSentButNotACKMessages().isEmpty());
  }

  @Test
  public void ackSuccessTopicsPattern() throws PulsarClientException {
    PulsarSourceConfig customPulsarSourceConfig = TestUtilsPulsar.getSourceConfig();
    customPulsarSourceConfig.pulsarTopicsSelector = PulsarTopicsSelector.TOPICS_PATTERN;

    createPulsarMessageConsumerImplNoIssues(customPulsarSourceConfig);

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any()))
             .thenReturn(TestUtilsPulsar.getPulsarMessage());
      Mockito.when(pulsarMessageConverterMock.convert(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
             .thenReturn(1);
      pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1);
    } catch (PulsarClientException | StageException e) {
      Assert.fail("Error mocking Consumer.receive method in ackSuccessTopicsPattern");
    }

    Assert.assertFalse(pulsarMessageConsumerImplMock.getSentButNotACKMessages().isEmpty());

    try {
      pulsarMessageConsumerImplMock.ack();
    } catch (StageException e) {
      Assert.fail("Ack threw an unexpected StageException");
    }

    Mockito.verify(messageConsumerMock, Mockito.times(1)).acknowledge(Mockito.any(MessageId.class));
    Assert.assertTrue(pulsarMessageConsumerImplMock.getSentButNotACKMessages().isEmpty());
  }

  @Test(expected = StageException.class)
  public void ackStageExceptionOneTopic() throws StageException {
    createPulsarMessageConsumerImplNoIssues();

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any()))
             .thenReturn(TestUtilsPulsar.getPulsarMessage());
      Mockito.when(pulsarMessageConverterMock.convert(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
             .thenReturn(1);
      pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1);
    } catch (PulsarClientException | StageException e) {
      Assert.fail("Error mocking Consumer.receive method in ackSuccessOneTopic");
    }

    Assert.assertNotNull(pulsarMessageConsumerImplMock.getLastSentButNotACKMessage());

    try {
      Mockito.doThrow(new PulsarClientException("Expected Pulsar" +
          " client exception in messageConsumerMock" +
          ".acknowledge")).when(messageConsumerMock).acknowledgeCumulative(Mockito.any(MessageId.class));
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking Consumer.receive method in ackSuccessOneTopic");
    }
    pulsarMessageConsumerImplMock.ack();
    Assert.fail();
  }

  @Test(expected = StageException.class)
  public void ackStageExceptionTopicsList() throws StageException {
    PulsarSourceConfig customPulsarSourceConfig = TestUtilsPulsar.getSourceConfig();
    customPulsarSourceConfig.pulsarTopicsSelector = PulsarTopicsSelector.TOPICS_LIST;
    customPulsarSourceConfig.topicsList = TestUtilsPulsar.getTopicsList();

    createPulsarMessageConsumerImplNoIssues(customPulsarSourceConfig);

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    try {
      Mockito.when(messageConsumerMock.receive(Mockito.anyInt(), Mockito.any()))
             .thenReturn(TestUtilsPulsar.getPulsarMessage());
      Mockito.when(pulsarMessageConverterMock.convert(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
             .thenReturn(1);
      pulsarMessageConsumerImplMock.take(Mockito.mock(BatchMaker.class), contextMock, 1);
    } catch (PulsarClientException | StageException e) {
      Assert.fail("Error mocking Consumer.receive method in ackStageExceptionTopicsList");
    }

    Assert.assertFalse(pulsarMessageConsumerImplMock.getSentButNotACKMessages().isEmpty());

    try {
      Mockito.doThrow(new PulsarClientException("Expected Pulsar" +
          " client exception in messageConsumerMock" +
          ".acknowledge")).when(messageConsumerMock).acknowledge(Mockito.any(MessageId.class));
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking Consumer.receive method in ackStageExceptionTopicsList");
    }

    pulsarMessageConsumerImplMock.ack();
    Assert.fail();

    Assert.assertTrue(pulsarMessageConsumerImplMock.getSentButNotACKMessages().isEmpty());
  }

  @Test
  public void close() {
    createPulsarMessageConsumerImplNoIssues();

    List<Stage.ConfigIssue> issues = pulsarMessageConsumerImplMock.init(contextMock);
    Assert.assertEquals(0, issues.size());

    pulsarMessageConsumerImplMock.close();
  }

  private void createPulsarMessageConsumerImplNoIssues() {
    createPulsarMessageConsumerImplNoIssues(null);
  }

  private void createPulsarMessageConsumerImplNoIssues(PulsarSourceConfig customPulsarSourceConfig) {
    contextMock = Mockito.mock(Source.Context.class);
    pulsarClientBuilderMock = Mockito.mock(ClientBuilder.class);
    pulsarClientMock = Mockito.mock(PulsarClient.class);
    pulsarMessageConverterMock = Mockito.mock(PulsarMessageConverter.class);
    messageConsumerMock = Mockito.mock(Consumer.class);
    consumerBuilderMock = Mockito.mock(ConsumerBuilder.class);

    Mockito.when(contextMock.createConfigIssue(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any()))
           .thenReturn(Mockito.mock(ConfigIssue.class));

    Mockito.when(consumerBuilderMock.properties(Mockito.any())).thenReturn(consumerBuilderMock);
    Mockito.when(consumerBuilderMock.topic(Mockito.any())).thenReturn(consumerBuilderMock);
    Mockito.when(consumerBuilderMock.topics(Mockito.any())).thenReturn(consumerBuilderMock);
    Mockito.when(consumerBuilderMock.subscriptionName(Mockito.any())).thenReturn(consumerBuilderMock);
    Mockito.when(consumerBuilderMock.consumerName(Mockito.any())).thenReturn(consumerBuilderMock);
    Mockito.when(consumerBuilderMock.receiverQueueSize(Mockito.anyInt())).thenReturn(consumerBuilderMock);
    Mockito.when(consumerBuilderMock.subscriptionType(Mockito.any())).thenReturn(consumerBuilderMock);
    Mockito.when(consumerBuilderMock.subscriptionInitialPosition(Mockito.any())).thenReturn(consumerBuilderMock);
    Mockito.when(consumerBuilderMock.topicsPattern(Mockito.any(Pattern.class))).thenReturn(consumerBuilderMock);
    Mockito.when(consumerBuilderMock.patternAutoDiscoveryPeriod(Mockito.anyInt())).thenReturn(consumerBuilderMock);
    Mockito.when(consumerBuilderMock.priorityLevel(Mockito.anyInt())).thenReturn(consumerBuilderMock);
    Mockito.when(consumerBuilderMock.readCompacted(Mockito.anyBoolean())).thenReturn(consumerBuilderMock);

    try {
      Mockito.when(consumerBuilderMock.subscribe()).thenReturn(messageConsumerMock);
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking ConsumerBuilder.subscribe method");
    }
    Mockito.when(pulsarClientMock.newConsumer()).thenReturn(consumerBuilderMock);

    try {
      Mockito.when(pulsarClientBuilderMock.serviceUrl(Mockito.any())).thenReturn(pulsarClientBuilderMock);
      Mockito.when(pulsarClientBuilderMock.keepAliveInterval(Mockito.anyInt(), Mockito.any())).thenReturn(
          pulsarClientBuilderMock);
      Mockito.when(pulsarClientBuilderMock.operationTimeout(Mockito.anyInt(), Mockito.any())).thenReturn(
          pulsarClientBuilderMock);
      Mockito.when(pulsarClientBuilderMock.build()).thenReturn(pulsarClientMock);
    } catch (PulsarClientException e) {
      Assert.fail("Error mocking ClientBuilder.build method");
    }

    PowerMockito.mockStatic(PulsarClient.class);
    BDDMockito.given(PulsarClient.builder()).willReturn(pulsarClientBuilderMock);

    pulsarMessageConsumerImplMock = new PulsarMessageConsumerImpl(TestUtilsPulsar.getBasicConfig(),
        customPulsarSourceConfig != null ? customPulsarSourceConfig : TestUtilsPulsar.getSourceConfig(),
        pulsarMessageConverterMock
    );
  }

  private ConfigIssue getConfigIssue(PulsarErrors pulsarError) {
    return new ConfigIssue() {
      @Override
      public String toString() {
        return pulsarError.getCode();
      }
    };
  }

}
