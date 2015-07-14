/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.flume.standalone;

import com.google.common.io.Resources;
import com.streamsets.pipeline.base.TestPipelineOperationsStandalone;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AvroSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestFlumeDestinationPipelineOperations extends TestPipelineOperationsStandalone {

  private static AvroSource source;
  private static Channel ch;
  private static ExecutorService executorService;

  @BeforeClass
  public static void beforeClass() throws Exception {
    //setup flume to write to
    source = new AvroSource();
    ch = new MemoryChannel();
    Configurables.configure(ch, new Context());

    Context context = new Context();
    //This should match whats present in the pipeline.json file
    context.put("port", String.valueOf(9050));
    context.put("bind", "localhost");
    Configurables.configure(source, context);

    List<Channel> channels = new ArrayList<>();
    channels.add(ch);
    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);
    source.setChannelProcessor(new ChannelProcessor(rcs));
    source.start();
    TestPipelineOperationsStandalone.beforeClass(getPipelineJson());

    //read from flume memory channel every second, otherwise the channel fills up and there is no more data coming in
    executorService = Executors.newSingleThreadExecutor();
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        Transaction transaction;
        while (true) {
          transaction = ch.getTransaction();
          transaction.begin();
          ch.take();
          transaction.commit();
          transaction.close();
        }
      }
    });
  }

  @AfterClass
  public static void afterClass() throws Exception {
    executorService.shutdownNow();
    source.stop();
    ch.stop();
    TestPipelineOperationsStandalone.afterClass();
  }

  private static String getPipelineJson() throws Exception {
    URI uri = Resources.getResource("flume_destination_pipeline_operations.json").toURI();
    String pipelineJson =  new String(Files.readAllBytes(Paths.get(uri)), StandardCharsets.UTF_8);
    return pipelineJson;
  }

  @Override
  protected String getPipelineName() {
    return "flume_destination_pipeline";
  }

  @Override
  protected String getPipelineRev() {
    return "0";
  }

  @Override
  protected void postPipelineStart() {

  }

}
