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
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.stage.Utils.TestUtilsPulsar;
import com.streamsets.pipeline.stage.origin.lib.MessageConfig;
import com.streamsets.pipeline.support.service.ServiceErrors;
import com.streamsets.pipeline.support.service.ServicesUtil;
import org.apache.pulsar.client.api.Message;
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

@RunWith(PowerMockRunner.class)
@PrepareForTest(ServicesUtil.class)
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestPulsarMessageConverterImpl {

  private Source.Context contextMock;
  private MessageConfig messageConfigMock;
  private PulsarMessageConverterImpl pulsarMessageConverterImplMock;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUpBefore() {
    messageConfigMock = null;
    contextMock = null;
    pulsarMessageConverterImplMock = null;
  }

  @Test
  public void testInit() {
    pulsarMessageConverterImplMock = new PulsarMessageConverterImpl(Mockito.mock(MessageConfig.class));
    pulsarMessageConverterImplMock.init(Mockito.mock(Source.Context.class));
  }

  @Test
  public void testConvertSuccessPayloadNull() {
    initPulsarMessageConverterImplMock();

    Message messageMock = Mockito.mock(Message.class);
    Mockito.when(messageMock.getData()).thenReturn(new byte[0]);

    int count = -1;

    try {
      count = pulsarMessageConverterImplMock.convert(Mockito.mock(BatchMaker.class),
          contextMock,
          "1286:0:-1:0",
          messageMock
      );
    } catch (StageException e) {
      Assert.fail();
    }

    Assert.assertEquals(0, count);
  }

  @Test
  public void testConvertSuccessPayloadNotNull() throws StageException {
    initPulsarMessageConverterImplMock();

    PowerMockito.mockStatic(ServicesUtil.class);
    BDDMockito.given(ServicesUtil.parseAll(Mockito.any(), Mockito.any(),
        Mockito.anyBoolean(), Mockito.any(), Mockito.any())).willReturn(TestUtilsPulsar.getRecordsList());

    int count = pulsarMessageConverterImplMock.convert(Mockito.mock(BatchMaker.class),
        contextMock,
        TestUtilsPulsar.getPulsarMessage().getMessageId().toString(),
        TestUtilsPulsar.getPulsarMessage()
    );

    Assert.assertEquals(5, count);

  }

  @Test
  public void testConvertStageExceptionDiscard() throws StageException {
    initPulsarMessageConverterImplMock();

    Mockito.when(contextMock.getOnErrorRecord()).thenReturn(OnRecordError.DISCARD);

    PowerMockito.mockStatic(ServicesUtil.class);
    BDDMockito.given(ServicesUtil.parseAll(Mockito.any(), Mockito.any(),
        Mockito.anyBoolean(), Mockito.any(), Mockito.any())).willThrow(new StageException(ServiceErrors
        .SERVICE_ERROR_001, TestUtilsPulsar.getPulsarMessage().getMessageId().toString(), "convert stage exception " +
        "discard", new Exception("convert stage exception discard")));

    int count = pulsarMessageConverterImplMock.convert(Mockito.mock(BatchMaker.class),
        contextMock,
        TestUtilsPulsar.getPulsarMessage().getMessageId().toString(),
        TestUtilsPulsar.getPulsarMessage()
    );

    Assert.assertEquals(0, count);
  }

  @Test
  public void testConvertStageExceptionToError() throws StageException {
    initPulsarMessageConverterImplMock();

    Mockito.when(contextMock.getOnErrorRecord()).thenReturn(OnRecordError.TO_ERROR);

    PowerMockito.mockStatic(ServicesUtil.class);
    BDDMockito.given(ServicesUtil.parseAll(Mockito.any(), Mockito.any(),
        Mockito.anyBoolean(), Mockito.any(), Mockito.any())).willThrow(new StageException(ServiceErrors
        .SERVICE_ERROR_001, TestUtilsPulsar.getPulsarMessage().getMessageId().toString(), "convert stage exception " +
        "discard", new Exception("convert stage exception discard")));

    int count = pulsarMessageConverterImplMock.convert(Mockito.mock(BatchMaker.class),
        contextMock,
        TestUtilsPulsar.getPulsarMessage().getMessageId().toString(),
        TestUtilsPulsar.getPulsarMessage()
    );

    Assert.assertEquals(0, count);
  }

  @Test
  public void testConvertStageExceptionStopPipeline() {
    initPulsarMessageConverterImplMock();

    Mockito.when(contextMock.getOnErrorRecord()).thenReturn(OnRecordError.STOP_PIPELINE);

    PowerMockito.mockStatic(ServicesUtil.class);
    try {
      BDDMockito.given(ServicesUtil.parseAll(Mockito.any(), Mockito.any(),
          Mockito.anyBoolean(), Mockito.any(), Mockito.any())).willThrow(new StageException(ServiceErrors
          .SERVICE_ERROR_001, TestUtilsPulsar.getPulsarMessage().getMessageId().toString(), "convert stage exception " +
          "discard", new Exception("convert stage exception discard")));
    } catch (StageException e) {
      Assert.fail("Failed to mock ServicesUtil.parseAll in testConvertStageExceptionStopPipeline");
    }

    int count = -1;

    try {
      count = pulsarMessageConverterImplMock.convert(Mockito.mock(BatchMaker.class),
          contextMock,
          TestUtilsPulsar.getPulsarMessage().getMessageId().toString(),
          TestUtilsPulsar.getPulsarMessage()
      );
      Assert.fail();
    } catch (StageException e) {
      Assert.assertEquals(ServiceErrors.SERVICE_ERROR_001.getCode(), e.getErrorCode().getCode());
    }

    Assert.assertEquals(-1, count);
  }

  private void initPulsarMessageConverterImplMock() {
    messageConfigMock = Mockito.mock(MessageConfig.class);
    contextMock = Mockito.mock(Source.Context.class);

    messageConfigMock.produceSingleRecordPerMessage = false;

    pulsarMessageConverterImplMock = new PulsarMessageConverterImpl(messageConfigMock);
    pulsarMessageConverterImplMock.init(contextMock);
  }
}
