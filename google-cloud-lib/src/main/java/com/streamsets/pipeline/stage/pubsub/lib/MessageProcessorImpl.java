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
import com.google.pubsub.v1.PubsubMessage;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** {@inheritDoc} */
public class MessageProcessorImpl implements MessageProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(MessageProcessorImpl.class);
  private static final String MIME_GRPC = "application/grpc+proto";

  private final SynchronousQueue<MessageReplyConsumerBundle> queue;
  private final List<AckReplyConsumer> pendingAcks = new LinkedList<>();
  private final PushSource.Context context;
  private final int batchSize;
  private final DataParserFactory parserFactory;
  private final long maxWaitTime;
  private final Clock clock;
  private final Consumer<? super AckReplyConsumer> reply;

  private volatile boolean stop;
  private BatchContext batchContext;
  private BatchMaker batchMaker;
  private int currentRecordCount = 0;
  private Instant maxBatchEndTime;

  public MessageProcessorImpl(
      PushSource.Context context,
      int batchSize,
      long maxWaitTime,
      DataParserFactory parserFactory,
      SynchronousQueue<MessageReplyConsumerBundle> queue // NOSONAR
  ) {
    this(context, batchSize, maxWaitTime, parserFactory, queue, Clock.systemUTC());
  }

  public MessageProcessorImpl(
      PushSource.Context context,
      int batchSize,
      long maxWaitTime,
      DataParserFactory parserFactory,
      SynchronousQueue<MessageReplyConsumerBundle> queue, // NOSONAR
      Clock clock
  ) {
    this.context = context;
    this.batchSize = batchSize;
    this.maxWaitTime = maxWaitTime;
    this.parserFactory = parserFactory;
    this.queue = queue;
    this.clock = clock;

    if (context.isPreview()) {
      reply = AckReplyConsumer::nack;
    } else {
      reply = AckReplyConsumer::ack;
    }
  }

  /** {@inheritDoc} */
  @Override
  public void stop() {
    this.stop = true;
  }

  @Override
  public Void call() throws Exception {
    batchContext = context.startBatch();
    batchMaker = batchContext.getBatchMaker();
    maxBatchEndTime = Instant.now(clock).plusMillis(maxWaitTime);
    currentRecordCount = 0;

    while (!stop) {
      Optional<MessageReplyConsumerBundle> bundle = Optional.empty();
      try {
        if (Instant.now(clock).isAfter(maxBatchEndTime) || currentRecordCount >= batchSize) {
          flush();
        }
        bundle = Optional.ofNullable(queue.poll(100, TimeUnit.MILLISECONDS));

        LOG.trace("Processing received message");
        bundle.ifPresent(b -> {
          parseMessage(b.getMessage());
          pendingAcks.add(b.getConsumer());
        });
      } catch (InterruptedException e) {
        bundle.ifPresent(b -> b.getConsumer().nack());
        Thread.currentThread().interrupt();
      }
    }
    flush();
    LOG.info("Record processor stopped.");
    return null;
  }

  private void flush() {
    LOG.trace("Flushing batch of size {}", currentRecordCount);
    // complete batch
    context.processBatch(batchContext);
    // reply to all messages
    pendingAcks.forEach(reply);
    pendingAcks.clear();

    // start new batch
    batchContext = context.startBatch();
    batchMaker = batchContext.getBatchMaker();
    currentRecordCount = 0;
    maxBatchEndTime = Instant.now(clock).plusMillis(maxWaitTime);
  }

  private void parseMessage(PubsubMessage message) {
    LOG.trace("parseMessage called");
    try (DataParser parser = parserFactory.getParser(message.getMessageId(), message.getData().toByteArray())) {
      Record r;
      while ((r = tryParse(parser, message)) != null) {
        setHeaders(message, r);

        batchMaker.addRecord(r);
        ++currentRecordCount;
      }
    } catch (DataParserException | IOException e) {
      LOG.error(Errors.PUBSUB_05.getMessage(), e.toString(), e);
      // Create a raw record of the gRPC message data, set attributes as headers, and use the messageId for the recordId
      Record errorRecord = context.createRecord(message.getMessageId(), message.getData().toByteArray(), MIME_GRPC);
      setHeaders(message, errorRecord);
      context.reportError(new OnRecordErrorException(errorRecord, Errors.PUBSUB_05, e.toString()));
    }
  }

  private static void setHeaders(PubsubMessage message, Record r) {
    // Populate attributes
    Record.Header header = r.getHeader();
    message.getAttributesMap().forEach(header::setAttribute);
  }

  private Record tryParse(DataParser parser, PubsubMessage message) throws DataParserException, IOException {
    do {
      try {
        return parser.parse();
      } catch (RecoverableDataParserException e) {
        // Propagate partially parsed record to error stream
        Record r = e.getUnparsedRecord();
        setHeaders(message, r);
        LOG.error(Errors.PUBSUB_05.getMessage(), e.toString(), e);
        context.reportError(new OnRecordErrorException(r, Errors.PUBSUB_05, e.toString()));

        // We'll simply continue reading past this recoverable error
      }
    } while(true);
  }
}
