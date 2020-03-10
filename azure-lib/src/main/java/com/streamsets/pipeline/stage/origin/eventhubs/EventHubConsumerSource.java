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
package com.streamsets.pipeline.stage.origin.eventhubs;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;
import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.IEventProcessorFactory;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.lib.eventhubs.Errors;
import com.streamsets.pipeline.lib.eventhubs.EventHubCommon;
import com.streamsets.pipeline.lib.eventhubs.EventHubConfigBean;
import com.streamsets.pipeline.lib.eventhubs.Groups;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class EventHubConsumerSource implements PushSource, IEventProcessorFactory,
    Consumer<ExceptionReceivedEventArgs> {

  private static final Logger LOG = LoggerFactory.getLogger(EventHubConsumerSource.class);
  private final EventHubConfigBean commonConf;
  private EventHubConsumerConfigBean consumerConfigBean;
  private EventHubCommon eventHubCommon;
  private DataParserFactory parserFactory;
  private Context context;
  private AtomicLong counter = new AtomicLong();
  private BlockingQueue<Exception> errorQueue;
  private List<Exception> errorList;

  EventHubConsumerSource(EventHubConfigBean commonConf, EventHubConsumerConfigBean consumerConfigBean) {
    this.commonConf = commonConf;
    this.consumerConfigBean = consumerConfigBean;
    eventHubCommon = new EventHubCommon(commonConf);
  }

  @Override
  public int getNumberOfThreads() {
    return consumerConfigBean.maxThreads;
  }

  @Override
  public List<ConfigIssue> init(Info info, Context context) {
    List<ConfigIssue> issues = new ArrayList<>();
    this.context = context;

    consumerConfigBean.dataFormatConfig.stringBuilderPoolSize = getNumberOfThreads();
    consumerConfigBean.dataFormatConfig.init(
        context,
        consumerConfigBean.dataFormat,
        Groups.DATA_FORMAT.name(),
        "dataFormatConfig",
        DataFormatConstants.MAX_OVERRUN_LIMIT,
        issues
    );
    parserFactory = consumerConfigBean.dataFormatConfig.getParserFactory();
    errorQueue = new ArrayBlockingQueue<>(100);
    errorList = new ArrayList<>(100);

    // validate the connection info
    if(issues.size() == 0) {
      try {
        EventHubClient ehClient = eventHubCommon.createEventHubClient("event-hub-consumer-pool-%d");
        EventHubRuntimeInformation ehInfo = ehClient.getRuntimeInformation().get();
        ehClient.close().get();
      } catch (Exception e) {
        issues.add(context.createConfigIssue(
            Groups.EVENT_HUB.toString(),
            EventHubCommon.CONF_NAME_SPACE,
            Errors.EVENT_HUB_02,
            e.getMessage()
        ));
      }
    }

    return issues;
  }

  @Override
  public void produce(Map<String, String> map, int maxBatchSize) throws StageException {
    List<EventProcessorHost> eventProcessorHostList = new ArrayList<>();
    try {
      ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder()
          .setNamespaceName(commonConf.namespaceName)
          .setEventHubName(commonConf.eventHubName)
          .setSasKey(commonConf.sasKey.get())
          .setSasKeyName(commonConf.sasKeyName);

      String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=" +
          consumerConfigBean.storageAccountName + ";AccountKey=" + consumerConfigBean.storageAccountKey.get();

      for (int i = 0; i < consumerConfigBean.maxThreads; i++) {
        EventProcessorHost eventProcessorHost = new EventProcessorHost(
            consumerConfigBean.hostNamePrefix + i,
            commonConf.eventHubName,
            consumerConfigBean.consumerGroup,
            eventHubConnectionString.toString(),
            storageConnectionString,
            consumerConfigBean.storageContainerName
        );

        EventProcessorOptions eventProcessorOptions = new EventProcessorOptions();
        eventProcessorOptions.setExceptionNotification(this);
        eventProcessorHost.registerEventProcessorFactory(this, eventProcessorOptions);

        eventProcessorHostList.add(eventProcessorHost);
      }

      while (!context.isStopped()) {
        dispatchHttpReceiverErrors(100);
      }
    } catch(Exception ex) {
      throw new StageException(Errors.EVENT_HUB_02, ex, ex);
    } finally {
      eventProcessorHostList.forEach(eventProcessorHost -> {
        CompletableFuture<Void> hostShutdown = eventProcessorHost.unregisterEventProcessor();
        try {
          hostShutdown.get();
        } catch (InterruptedException | ExecutionException ex) {
          LOG.error(Errors.EVENT_HUB_03.getMessage(), ex.toString(), ex);
        }
      });
    }
  }

  private void dispatchHttpReceiverErrors(long intervalMillis) {
    if (intervalMillis > 0) {
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException ignored) {
      }
    }
    // report errors  reported by the HttpReceiverServer
    errorList.clear();
    errorQueue.drainTo(errorList);
    for (Exception exception : errorList) {
      context.reportError(exception);
    }
  }

  @Override
  public void destroy() {
  }

  @Override
  public IEventProcessor createEventProcessor(PartitionContext partitionContext) throws Exception {
    return new EventHubProcessor(context, parserFactory, errorQueue, counter);
  }

  @Override
  public void accept(ExceptionReceivedEventArgs ex) {
    LOG.error("Host " + ex.getHostname() + " received general error notification during " + ex.getAction() + ": " +
        ex.getException().toString());
    errorQueue.offer(ex.getException());
  }
}
