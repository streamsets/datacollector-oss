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

package com.streamsets.pipeline.stage.pubsub.lib;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MessageReceiverImpl implements MessageReceiver {
  private static final Logger LOG = LoggerFactory.getLogger(MessageReceiverImpl.class);
  private final PushSource.Context context;
  private final int maxBatchSize;
  private final DataParserFactory parserFactory;

  private BatchContext batchContext;
  private BatchMaker batchMaker;
  private int currentBatchSize;

  public MessageReceiverImpl(
      PushSource.Context context,
      DataParserFactory parserFactory,
      int maxBatchSize,
      int batchFlushInterval
  ) {
    LOG.info("Created new MessageReceiver");
    this.context = context;
    this.parserFactory = parserFactory;
    this.maxBatchSize = maxBatchSize;
    batchContext = context.startBatch();
    batchMaker = batchContext.getBatchMaker();
    currentBatchSize = 0;

    SafeScheduledExecutorService executorService = new SafeScheduledExecutorService(1, "MessageReceiverFlusher");
    executorService.scheduleWithFixedDelay(() -> flush(true), batchFlushInterval, batchFlushInterval, TimeUnit.MILLISECONDS);
  }
  @Override
  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
    // handle incoming message (parse, etc)
    LOG.info("Id : " + message.getMessageId());
    LOG.info("Data : " + message.getData().toStringUtf8());

    try {
      DataParser parser = parserFactory.getParser(message.getMessageId(), message.getData().toByteArray());
      Record r;
      while ((r = parser.parse()) != null) {
        // Populate attributes
        Record.Header header = r.getHeader();
        message.getAttributesMap().forEach(header::setAttribute);

        batchMaker.addRecord(r);
      }
    } catch (DataParserException | IOException e) {
      context.reportError(e);
    }

    if (context.isPreview()) {
      LOG.debug("Sending nack b/c preview mode is set");
      consumer.nack();
    } else {
      // ack/nack
      consumer.ack();
    }

    flush(false);
  }

  private synchronized void flush(boolean force) {
    if (currentBatchSize < maxBatchSize && !force) {
      return;
    }

    context.processBatch(batchContext);
    batchContext = context.startBatch();
    batchMaker = batchContext.getBatchMaker();
    currentBatchSize = 0;
  }
}
