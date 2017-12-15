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
package com.streamsets.pipeline.stage.origin.httpserver;

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import java.io.InputStream;
import java.util.List;

public class TestPushHttpReceiver {

  @Before
  @After
  public void beforeAndAfter() {
    System.clearProperty(PushHttpReceiver.MAXREQUEST_SYS_PROP);
  }

  @Test
  public void testInit() {
    HttpConfigs httpConfigs = Mockito.mock(HttpConfigs.class);
    DataParserFormatConfig dataConfigs = Mockito.mock(DataParserFormatConfig.class);
    Stage.Context context = ContextInfoCreator.createSourceContext("i", true, OnRecordError.DISCARD, ImmutableList.of
        ("a"));
    PushHttpReceiver receiver = new PushHttpReceiver(httpConfigs, 0, dataConfigs);
    try {
      Assert.assertTrue(receiver.init(context).isEmpty());
      Assert.assertEquals(0, receiver.getMaxRequestSize());
    } finally {
      receiver.destroy();
    }
    receiver = new PushHttpReceiver(httpConfigs, 100, dataConfigs);
    try {
      Assert.assertTrue(receiver.init(context).isEmpty());
      Assert.assertEquals(100 * 1000 * 1000, receiver.getMaxRequestSize());
    } finally {
      receiver.destroy();
    }
    receiver = new PushHttpReceiver(httpConfigs, 101, dataConfigs);
    try {
      List<Stage.ConfigIssue> issues = receiver.init(context);
      Assert.assertEquals(1, issues.size());
      Assert.assertTrue(issues.get(0).toString().contains(Errors.HTTP_SERVER_PUSH_00.name()));
    } finally {
      receiver.destroy();
    }
    System.setProperty(PushHttpReceiver.MAXREQUEST_SYS_PROP, "101");
    receiver = new PushHttpReceiver(httpConfigs, 101, dataConfigs);
    try {
      Assert.assertTrue(receiver.init(context).isEmpty());
      Assert.assertEquals(101 * 1000 * 1000, receiver.getMaxRequestSize());
    } finally {
      receiver.destroy();
    }
  }

    @Test
  public void testGetters() throws Exception {
    HttpConfigs httpConfigs = Mockito.mock(HttpConfigs.class);
    Mockito.when(httpConfigs.getAppId()).thenReturn(() -> "id");

    DataParserFactory parserFactory = Mockito.mock(DataParserFactory.class);
    DataParserFormatConfig dataConfigs = Mockito.mock(DataParserFormatConfig.class);
    Mockito.when(dataConfigs.getParserFactory()).thenReturn(parserFactory);

    PushHttpReceiver receiver = new PushHttpReceiver(httpConfigs, 1, dataConfigs);

    PushSource.Context context = Mockito.mock(PushSource.Context.class);

    Assert.assertTrue(receiver.init(context).isEmpty());
    Assert.assertEquals(context, receiver.getContext());
    Assert.assertEquals(parserFactory, receiver.getParserFactory());

    Assert.assertEquals("id", receiver.getAppId().get());
    Mockito.verify(httpConfigs, Mockito.times(1)).getAppId();

    Assert.assertEquals("/", receiver.getUriPath());

    Assert.assertEquals(1 * 1000 * 1000, receiver.getMaxRequestSize());
  }

  @Test
  public void testValidate() throws Exception {
    PushHttpReceiver receiver = new PushHttpReceiver(null, 1, null);

    Assert.assertTrue(receiver.validate(null, null));
  }

  @Test
  public void testProcess() throws Exception {
    PushHttpReceiver receiver = new PushHttpReceiver(null, 1, null);
    receiver = Mockito.spy(receiver);

    InputStream is = Mockito.mock(InputStream.class);

    PushSource.Context context = Mockito.mock(PushSource.Context.class);
    BatchContext batchContext = Mockito.mock(BatchContext.class);
    BatchMaker batchMaker = Mockito.mock(BatchMaker.class);
    Mockito.when(batchContext.getBatchMaker()).thenReturn(batchMaker);
    Mockito.when(context.startBatch()).thenReturn(batchContext);
    Mockito.doReturn(context).when(receiver).getContext();

    DataParserFactory parserFactory = Mockito.mock(DataParserFactory.class);
    DataParser parser = Mockito.mock(DataParser.class);
    Record record = Mockito.mock(Record.class);
    Mockito.when(record.getHeader()).thenReturn(Mockito.mock(Record.Header.class));
    Mockito.when(parser.parse()).thenReturn(record).thenReturn(null);
    Mockito.when(parserFactory.getParser(Mockito.anyString(), Mockito.any(InputStream.class), Mockito.eq("0")))
        .thenReturn(parser);

    Mockito.doReturn(parserFactory).when(receiver).getParserFactory();

    receiver.process(Mockito.mock(HttpServletRequest.class), is);

    Mockito.verify(receiver, Mockito.times(1)).createBoundInputStream(Mockito.eq(is));
    Mockito.verify(receiver, Mockito.times(1)).getMaxRequestSize();
    Mockito.verify(receiver, Mockito.times(2)).getContext(); //startBatch&processBatch

    Mockito.verify(context, Mockito.times(1)).startBatch();

    Mockito.verify(receiver, Mockito.times(1)).getParserFactory();
    Mockito.verify(parser, Mockito.times(2)).parse();

    Mockito.verify(batchMaker, Mockito.times(1)).addRecord(Mockito.eq(record));

    Mockito.verify(context, Mockito.times(1)).processBatch(Mockito.eq(batchContext));
  }

}
