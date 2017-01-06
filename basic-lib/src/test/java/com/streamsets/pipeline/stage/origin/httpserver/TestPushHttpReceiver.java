/**
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.httpserver;

import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.http.HttpConfigs;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.origin.lib.DataParserFormatConfig;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.InputStream;

public class TestPushHttpReceiver {

  @Test
  public void testGetters() {
    HttpConfigs httpConfigs = Mockito.mock(HttpConfigs.class);
    Mockito.when(httpConfigs.getAppId()).thenReturn("id");

    DataParserFactory parserFactory = Mockito.mock(DataParserFactory.class);
    DataParserFormatConfig dataConfigs = Mockito.mock(DataParserFormatConfig.class);
    Mockito.when(dataConfigs.getParserFactory()).thenReturn(parserFactory);

    PushHttpReceiver receiver = new PushHttpReceiver(httpConfigs, 1, dataConfigs);

    PushSource.Context context = Mockito.mock(PushSource.Context.class);

    Assert.assertTrue(receiver.init(context).isEmpty());
    Assert.assertEquals(context, receiver.getContext());
    Assert.assertEquals(parserFactory, receiver.getParserFactory());

    Assert.assertEquals("id", receiver.getAppId());
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
    Mockito.when(parser.parse()).thenReturn(record).thenReturn(null);
    Mockito
        .when(parserFactory.getParser(Mockito.anyString(), Mockito.any(InputStream.class), Mockito.eq("0")))
        .thenReturn(parser);

    Mockito.doReturn(parserFactory).when(receiver).getParserFactory();

    receiver.process(null, is);

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
