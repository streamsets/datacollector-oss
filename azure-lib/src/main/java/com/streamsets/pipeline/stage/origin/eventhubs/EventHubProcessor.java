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

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.PartitionContext;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class EventHubProcessor implements IEventProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(EventHubProcessor.class);
  private final PushSource.Context pushSourceContext;
  private final AtomicLong counter;
  private final BlockingQueue<Exception> errorQueue;
  private final DataParserFactory dataParserFactory;

  EventHubProcessor(
      PushSource.Context pushSourceContext,
      DataParserFactory dataParserFactory,
      BlockingQueue<Exception> errorQueue,
      AtomicLong counter
  ) {
    this.pushSourceContext = pushSourceContext;
    this.dataParserFactory = dataParserFactory;
    this.errorQueue = errorQueue;
    this.counter = counter;
  }

  @Override
  public void onOpen(PartitionContext context) throws Exception {
    LOG.debug("Partition " + context.getPartitionId() + " is opening");
  }

  @Override
  public void onClose(PartitionContext context, CloseReason reason) throws Exception {
    LOG.debug("Partition " + context.getPartitionId() + " is closing for reason " + reason.toString());
  }

  @Override
  public void onError(PartitionContext context, Throwable error) {
    LOG.debug("Partition " + context.getPartitionId() + " onError: " + error.toString());
  }

  @Override
  public void onEvents(PartitionContext context, Iterable<EventData> messages) throws Exception {
    BatchContext batchContext = pushSourceContext.startBatch();
    final EventData[] lasEventData = new EventData[]{null};
    if (messages != null) {
      messages.forEach(eventData -> {
        lasEventData[0] = eventData;
        List<Record> records = new ArrayList<>();
        String requestId = System.currentTimeMillis() + "." + counter.getAndIncrement();
        try (DataParser parser = dataParserFactory.getParser(requestId, eventData.getBytes())) {
          Record parsedRecord = parser.parse();
          while (parsedRecord != null) {
            records.add(parsedRecord);
            parsedRecord = parser.parse();
          }
          for (Record record : records) {
            batchContext.getBatchMaker().addRecord(record);
          }
        } catch (Exception ex) {
          errorQueue.offer(ex);
          LOG.warn("Error while processing request payload from: {}", ex.toString(), ex);
        }
      });
    }
    if (pushSourceContext.processBatch(batchContext) && lasEventData[0] != null && !pushSourceContext.isPreview()) {
      context.checkpoint(lasEventData[0]);
    }
  }
}
