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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.FlumeTestUtil;
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
import org.apache.flume.source.AvroSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestFlumeLoadBalancingTarget {

  private static final int NUM_HOSTS = 3;
  private static final int NUM_BATCHES = 5;
  private static final List<Integer> ports = new ArrayList<>(NUM_HOSTS);
  private static final Map<String, String> flumeHosts = new HashMap<>(NUM_HOSTS);
  private List<AvroSource> sources;
  private List<Channel> chs;

  @BeforeClass
  public static void setUpClass() throws Exception {
    for (int i = 0; i < NUM_HOSTS; i++) {
      int port = NetworkUtils.getRandomPort();
      ports.add(port);
      flumeHosts.put("h" + i + 1, "localhost:" + port);
    }
  }
  @Before
  public void setUp() {
    sources = new ArrayList<>(NUM_HOSTS);
    chs = new ArrayList<>(NUM_HOSTS);

    for(int i = 0; i < NUM_HOSTS; i++) {
      AvroSource source = new AvroSource();
      Channel channel = new MemoryChannel();
      Configurables.configure(channel, new Context());

      Context context = new Context();
      context.put("port", String.valueOf(ports.get(i)));
      context.put("bind", "localhost");
      Configurables.configure(source, context);

      List<Channel> channels = new ArrayList<>();
      channels.add(channel);
      ChannelSelector rcs = new ReplicatingChannelSelector();
      rcs.setChannels(channels);
      source.setChannelProcessor(new ChannelProcessor(rcs));

      sources.add(source);
      chs.add(channel);

      source.start();
    }

  }

  @After
  public void tearDown(){
    for(int i = 0; i < NUM_HOSTS; i++) {
      sources.get(i).stop();
      chs.get(i).stop();
    }
  }

  @Test
  public void testFlumeConfiguration() throws StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = false;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createFlumeConfig(
        true,                      // backOff
        100,                          // batchSize
        ClientType.AVRO_LOAD_BALANCING,
        2000,                       // connection timeout
        flumeHosts,
        HostSelectionStrategy.ROUND_ROBIN,
        -1,                          // maxBackOff
        1,                          // maxRetryAttempts
        2000,                       // requestTimeout
        false,                      // singleEventPerBatch
        0
      ),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );
    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

    try {
      targetRunner.runInit();
      Assert.fail("Expected Stage Exception as configuration maxBackOff is not valid");
    } catch (StageException e) {

    }
  }

  @Test
  public void testWriteStringRecordsRoundRobin() throws StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = false;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createFlumeConfig(
        false,                      // backOff
        100,                          // batchSize
        ClientType.AVRO_LOAD_BALANCING,
        2000,                       // connection timeout
        flumeHosts,
        HostSelectionStrategy.ROUND_ROBIN,
        -1,                          // maxBackOff
        1,                          // maxRetryAttempts
        2000,                       // requestTimeout
        false,                      // singleEventPerBatch
        0
      ),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

    targetRunner.runInit();
    List<List<Record>> logRecords = new ArrayList<>(NUM_HOSTS);
    for(int i = 0; i < NUM_HOSTS; i++) {
      logRecords.add(FlumeTestUtil.createStringRecords());
    }

    for(int i = 0; i < NUM_HOSTS; i++) {
      targetRunner.runWrite(logRecords.get(i));
    }
    targetRunner.runDestroy();

    for(int i = 0;i < logRecords.size(); i++) {
      Channel channel = chs.get(i % NUM_HOSTS);
      List<Record> records = logRecords.get(i);
      for(int j = 0; j < records.size(); j++) {
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        Event event = channel.take();
        Assert.assertNotNull(event);
        Assert.assertEquals(records.get(j).get().getValueAsString(), new String(event.getBody()).trim());
        Assert.assertTrue(event.getHeaders().containsKey("charset"));
        Assert.assertEquals("UTF-8", event.getHeaders().get("charset"));
        transaction.commit();
        transaction.close();
      }
    }
  }

  @Test
  public void testWriteStringRecordsRandom() throws StageException {

    DataGeneratorFormatConfig dataGeneratorFormatConfig = new DataGeneratorFormatConfig();
    dataGeneratorFormatConfig.textFieldPath = "/";
    dataGeneratorFormatConfig.textEmptyLineIfNull = false;

    FlumeTarget flumeTarget = FlumeTestUtil.createFlumeTarget(
      FlumeTestUtil.createFlumeConfig(
        false,                      // backOff
        100,                          // batchSize
        ClientType.AVRO_LOAD_BALANCING,
        2000,                       // connection timeout
        flumeHosts,
        HostSelectionStrategy.RANDOM,
        -1,                          // maxBackOff
        1,                          // maxRetryAttempts
        2000,                       // requestTimeout
        false,                      // singleEventPerBatch
        0
      ),
      DataFormat.TEXT,
      dataGeneratorFormatConfig
    );

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class, flumeTarget).build();

    targetRunner.runInit();

    List<List<Record>> logRecords = new ArrayList<>(NUM_BATCHES);
    for(int i = 0; i < NUM_BATCHES; i++) {
      logRecords.add(FlumeTestUtil.createStringRecords());
    }
    int totalRecords = 0;
    for(int i = 0; i < NUM_BATCHES; i++) {
      targetRunner.runWrite(logRecords.get(i));
      totalRecords += logRecords.get(i).size();
    }
    targetRunner.runDestroy();

    int totalFlumeEvents = 0;
    for(Channel channel : chs) {
      Transaction transaction = channel.getTransaction();
      transaction.begin();
      while(channel.take() != null) {
        totalFlumeEvents++;
      }
      transaction.commit();
      transaction.close();
    }

    Assert.assertEquals(totalRecords, totalFlumeEvents);
  }

}
