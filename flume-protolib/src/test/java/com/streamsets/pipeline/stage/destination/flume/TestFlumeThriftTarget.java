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
package com.streamsets.pipeline.stage.destination.flume;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.RecordReader;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.FlumeTestUtil;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.testing.NetworkUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.ThriftSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestFlumeThriftTarget {

  private ThriftSource source;
  private Channel ch;
  private int port;

  @Before
  public void setUp() throws Exception {
    port = NetworkUtils.getRandomPort();
    source = new ThriftSource();
    ch = new MemoryChannel();
    Configurables.configure(ch, new Context());

    Context context = new Context();
    context.put("port", String.valueOf(port));
    context.put("bind", "localhost");
    Configurables.configure(source, context);

    List<Channel> channels = new ArrayList<>();
    channels.add(ch);
    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);
    source.setChannelProcessor(new ChannelProcessor(rcs));
    source.start();
  }

  @After
  public void tearDown(){
    source.stop();
    ch.stop();
  }

  @Test
  public void testWriteStringRecords() throws StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = false;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createFlumeConfig(
        false,                      // backOff
        1,                          // batchSize
        ClientType.THRIFT,
        1000,                       // connection timeout
        ImmutableMap.of("h1", "localhost:" + port),
        HostSelectionStrategy.RANDOM,
        -1,                          // maxBackOff
        1,                          // maxRetryAttempts
        1000,                       // requestTimeout
        false,                      // singleEventPerBatch
        0
      ),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createStringRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    for(Record r : logRecords) {
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      Assert.assertEquals(r.get().getValueAsString(), new String(event.getBody()).trim());
      Assert.assertTrue(event.getHeaders().containsKey("charset"));
      Assert.assertEquals("UTF-8", event.getHeaders().get("charset"));
      transaction.commit();
      transaction.close();
    }
  }

  @Test
  public void testWriteStringRecordsFromJSON() throws InterruptedException, StageException, IOException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/name";
    dataGeneratorFormatConfig.textEmptyLineIfNull = false;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createFlumeConfig(
        false,                      // backOff
        1,                          // batchSize
        ClientType.THRIFT,
        1000,                       // connection timeout
        ImmutableMap.of("h1", "localhost:" + port),
        HostSelectionStrategy.RANDOM,
        -1,                          // maxBackOff
        1,                          // maxRetryAttempts
        1000,                       // requestTimeout
        false,                      // singleEventPerBatch
        0
      ),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    for(Record r : logRecords) {
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      Assert.assertEquals(r.get().getValueAsMap().get("name").getValueAsString(), new String(event.getBody()).trim());
      Assert.assertTrue(event.getHeaders().containsKey("charset"));
      Assert.assertEquals("UTF-8", event.getHeaders().get("charset"));
      transaction.commit();
      transaction.close();
    }
  }

  @Test
  public void testWriteStringRecordsFromJSON2() throws InterruptedException, StageException, IOException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/lastStatusChange";
    dataGeneratorFormatConfig.textEmptyLineIfNull = false;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createFlumeConfig(
        false,                      // backOff
        1,                          // batchSize
        ClientType.THRIFT,
        1000,                       // connection timeout
        ImmutableMap.of("h1", "localhost:" + port),
        HostSelectionStrategy.RANDOM,
        -1,                          // maxBackOff
        1,                          // maxRetryAttempts
        1000,                       // requestTimeout
        false,                      // singleEventPerBatch
        0
      ),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    for(Record r : logRecords) {
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      Assert.assertEquals(r.get().getValueAsMap().get("lastStatusChange").getValueAsString(), new String(event.getBody()).trim());
      Assert.assertTrue(event.getHeaders().containsKey("charset"));
      Assert.assertEquals("UTF-8", event.getHeaders().get("charset"));
      transaction.commit();
      transaction.close();
    }
  }

  @Test
  public void testWriteStringRecordsFromJSON3() throws InterruptedException, StageException, IOException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/"; //MAP
    dataGeneratorFormatConfig.textEmptyLineIfNull = false;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createFlumeConfig(
        false,                      // backOff
        1,                          // batchSize
        ClientType.THRIFT,
        1000,                       // connection timeout
        ImmutableMap.of("h1", "localhost:" + port),
        HostSelectionStrategy.RANDOM,
        -1,                          // maxBackOff
        1,                          // maxRetryAttempts
        1000,                       // requestTimeout
        false,                      // singleEventPerBatch
        0
      ),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget)
      .setOnRecordError(OnRecordError.TO_ERROR).build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);

    //All records must be sent to error
    Assert.assertEquals(logRecords.size(), targetRunner.getErrorRecords().size());

    targetRunner.runDestroy();

    Transaction transaction = ch.getTransaction();
    transaction.begin();
    Event event = ch.take();
    Assert.assertNull(event);
    transaction.commit();
    transaction.close();

  }

  @Test
  public void testWriteJsonRecords() throws InterruptedException, StageException, IOException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createFlumeConfig(
        false,                      // backOff
        1,                          // batchSize
        ClientType.THRIFT,
        1000,                       // connection timeout
        ImmutableMap.of("h1", "localhost:" + port),
        HostSelectionStrategy.RANDOM,
        -1,                          // maxBackOff
        1,                          // maxRetryAttempts
        1000,                       // requestTimeout
        false,                      // singleEventPerBatch
        0
      ),
      DataFormat.SDC_JSON,
      dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createJsonRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    ContextExtensions ctx = (ContextExtensions) ContextInfoCreator.createTargetContext("", false, OnRecordError.TO_ERROR);
    for(Record r : logRecords) {
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      ByteArrayInputStream bais = new ByteArrayInputStream(event.getBody());
      RecordReader rr = ctx.createRecordReader(bais, 0, Integer.MAX_VALUE);
      Assert.assertEquals(r, rr.readRecord());
      Assert.assertTrue(event.getHeaders().containsKey("charset"));
      Assert.assertEquals("UTF-8", event.getHeaders().get("charset"));
      rr.close();
      transaction.commit();
      transaction.close();
    }
  }

  @Test
  public void testWriteCsvRecords() throws InterruptedException, StageException, IOException {

    //Test DELIMITED is - "2010,NLDS1,PHI,NL,CIN,NL,3,0,0"
    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.csvFileFormat = CsvMode.CSV;
    dataGeneratorFormatConfig.csvHeader = CsvHeader.NO_HEADER;
    dataGeneratorFormatConfig.csvReplaceNewLines = false;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createFlumeConfig(
        false,                      // backOff
        1,                          // batchSize
        ClientType.THRIFT,
        1000,                       // connection timeout
        ImmutableMap.of("h1", "localhost:" + port),
        HostSelectionStrategy.RANDOM,
        0,                          // maxBackOff
        0,                          // maxRetryAttempts
        1000,                       // requestTimeout
        false,                      // singleEventPerBatch
        0
      ),
      DataFormat.DELIMITED,
      dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

    targetRunner.runInit();
    List<Record> logRecords = FlumeTestUtil.createCsvRecords();
    targetRunner.runWrite(logRecords);
    targetRunner.runDestroy();

    for(Record r : logRecords) {
      Transaction transaction = ch.getTransaction();
      transaction.begin();
      Event event = ch.take();
      Assert.assertNotNull(event);
      transaction.commit();
      transaction.close();
    }
  }
}
