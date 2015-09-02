/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.destination.flume;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.FlumeTestUtil;
import com.streamsets.pipeline.sdk.TargetRunner;
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
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestFlumeLoadBalancingTarget {

  private static final int NUM_HOSTS = 3;
  private static final int NUM_BATCHES = 5;
  private static final int PORT = 9050;
  private List<AvroSource> sources;
  private List<Channel> chs;

  @Before
  public void setUp() {
    sources = new ArrayList<>(NUM_HOSTS);
    chs = new ArrayList<>(NUM_HOSTS);

    for(int i = 0; i < NUM_HOSTS; i++) {
      AvroSource source = new AvroSource();
      Channel channel = new MemoryChannel();
      Configurables.configure(channel, new Context());

      Context context = new Context();
      context.put("port", String.valueOf(PORT + i));
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

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");
    flumeHostsConfig.put("h2", "localhost:9051");
    flumeHostsConfig.put("h3", "localhost:9052");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_LOAD_BALANCING)
      .addConfiguration("batchSize", 100)
      .addConfiguration("backOff", true)
      .addConfiguration("maxBackOff", -1)
      .addConfiguration("hostSelectionStrategy", HostSelectionStrategy.ROUND_ROBIN)
      .addConfiguration("connectionTimeout", 2000)
      .addConfiguration("requestTimeout", 2000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .build();

    try {
      targetRunner.runInit();
      Assert.fail("Expected Stage Exception as configuration maxBackOff is not valid");
    } catch (StageException e) {

    }
  }

  @Test
  public void testWriteStringRecordsRoundRobin() throws StageException {

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");
    flumeHostsConfig.put("h2", "localhost:9051");
    flumeHostsConfig.put("h3", "localhost:9052");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_LOAD_BALANCING)
      .addConfiguration("batchSize", 100)
      .addConfiguration("backOff", false)
      .addConfiguration("maxBackOff", 0)
      .addConfiguration("hostSelectionStrategy", HostSelectionStrategy.ROUND_ROBIN)
      .addConfiguration("connectionTimeout", 2000)
      .addConfiguration("requestTimeout", 2000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8")
      .addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .build();

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

    Map<String, String> flumeHostsConfig = new HashMap<>();
    flumeHostsConfig.put("h1", "localhost:9050");
    flumeHostsConfig.put("h2", "localhost:9051");
    flumeHostsConfig.put("h3", "localhost:9052");

    TargetRunner targetRunner = new TargetRunner.Builder(FlumeDTarget.class)
      .addConfiguration("flumeHostsConfig", flumeHostsConfig)
      .addConfiguration("clientType", ClientType.AVRO_LOAD_BALANCING)
      .addConfiguration("batchSize", 100)
      .addConfiguration("backOff", false)
      .addConfiguration("maxBackOff", 0)
      .addConfiguration("hostSelectionStrategy", HostSelectionStrategy.RANDOM)
      .addConfiguration("connectionTimeout", 2000)
      .addConfiguration("requestTimeout", 2000)
      .addConfiguration("dataFormat", DataFormat.TEXT)
      .addConfiguration("textFieldPath", "/")
      .addConfiguration("textEmptyLineIfNull", true)
      .addConfiguration("singleEventPerBatch", false)
      .addConfiguration("charset", "UTF-8").addConfiguration("maxRetryAttempts", 1)
      .addConfiguration("waitBetweenRetries", 0)
      .build();

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
