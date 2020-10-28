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

package com.streamsets.pipeline.stage.origin.sqs;

import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.DeliveryGuarantee;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.api.service.dataformats.RecoverableDataParserException;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SqsConsumerWorkerCallable implements Callable<Exception> {

  private static final Logger LOG = LoggerFactory.getLogger(SqsConsumerWorkerCallable.class);

  private static final String SQS_ATTRIBUTE_PREFIX = "sqs.";
  private static final String SQS_REGION_ATTRIBUTE = SQS_ATTRIBUTE_PREFIX + "region";
  private static final String SQS_MESSAGE_BODY_ATTRIBUTE = SQS_ATTRIBUTE_PREFIX + "body";
  private static final String SQS_MESSAGE_BODY_MD5_ATTRIBUTE = SQS_ATTRIBUTE_PREFIX + "bodyMd5";
  private static final String SQS_MESSAGE_ID_ATTRIBUTE = SQS_ATTRIBUTE_PREFIX + "messageId";
  private static final String SQS_QUEUE_NAME_PREFIX_ATTRIBUTE = SQS_ATTRIBUTE_PREFIX + "queueNamePrefix";
  private static final String SQS_QUEUE_URL_ATTRIBUTE = SQS_ATTRIBUTE_PREFIX + "queueUrl";
  private static final String SQS_MESSAGE_ATTRIBUTE_PREFIX = SQS_ATTRIBUTE_PREFIX + "messageAttr.";
  private static final String SQS_MESSAGE_ATTRIBUTE_MD5_ATTRIBUTE = SQS_ATTRIBUTE_PREFIX + "messageAttrMd5";

  private final AmazonSQSAsync sqsAsync;
  private final PushSource.Context context;
  private final Map<String, String> queueUrlToNamePrefix;
  private final int numMessagesPerRequest;
  private final long maxBatchWaitTimeMs;
  private final int maxBatchSize;
  private final String awsRegionLabel;
  private final SqsAttributesOption sqsAttributesOption;
  private final ErrorRecordHandler errorRecordHandler;
  private final int pollWaitTimeSeconds;
  private final Set<String> messageAttributeNames;

  private final Multimap<String, Message> commitQueueUrlsToMessages = HashMultimap.create();
  private BatchContext batchContext;
  private long lastBatchStartTimestamp;
  private int batchRecordCount;

  SqsConsumerWorkerCallable(
      AmazonSQSAsync sqsAsync,
      PushSource.Context context,
      Map<String, String> queueUrlToNamePrefix,
      int numMessagesPerRequest,
      long maxBatchWaitTimeMs,
      int maxBatchSize,
      String awsRegionLabel,
      SqsAttributesOption sqsAttributesOption,
      ErrorRecordHandler errorRecordHandler,
      int pollWaitTimeSeconds,
      Collection<String> messageAttributeNames
  ) {
    Utils.checkState(!queueUrlToNamePrefix.isEmpty(), "queueUrlToNamePrefix must be non-empty");
    this.sqsAsync = sqsAsync;
    this.context = context;
    this.queueUrlToNamePrefix = new HashMap<>();
    Optional.of(queueUrlToNamePrefix).ifPresent(this.queueUrlToNamePrefix::putAll);
    this.numMessagesPerRequest = numMessagesPerRequest;
    this.maxBatchWaitTimeMs = maxBatchWaitTimeMs;
    this.maxBatchSize = maxBatchSize;
    this.awsRegionLabel = awsRegionLabel;
    this.sqsAttributesOption = sqsAttributesOption;
    this.errorRecordHandler = errorRecordHandler;
    this.pollWaitTimeSeconds = pollWaitTimeSeconds;
    this.messageAttributeNames = new HashSet<>();
    if (messageAttributeNames != null) {
      this.messageAttributeNames.addAll(messageAttributeNames);
    }
  }

  @Override
  public Exception call() throws Exception {
    Exception terminatingException = null;
    final AmazonSQSAsync asyncConsumer = sqsAsync;

    final ArrayBlockingQueue<String> urlQueue = new ArrayBlockingQueue<>(queueUrlToNamePrefix.size(),
        false,
        queueUrlToNamePrefix.keySet()
    );

    while (!context.isStopped() && terminatingException == null) {
      String nextQueueUrl = null;
      try {
        nextQueueUrl = urlQueue.take();
        final ReceiveMessageRequest receiveRequest = new ReceiveMessageRequest().withMaxNumberOfMessages(
            numMessagesPerRequest).withQueueUrl(nextQueueUrl);
        if (pollWaitTimeSeconds > 0) {
          receiveRequest.setWaitTimeSeconds(pollWaitTimeSeconds);
        }
        if (!messageAttributeNames.isEmpty()) {
          receiveRequest.setMessageAttributeNames(messageAttributeNames);
        }
        Future<ReceiveMessageResult> resultFuture = asyncConsumer.receiveMessageAsync(receiveRequest);

        ReceiveMessageResult result = resultFuture.get();
        for (Message message : result.getMessages()) {
          final String recordId = getRecordId(message, nextQueueUrl);
          DataParser parser = null;
          try {
            parser = context.getService(DataFormatParserService.class).getParser(recordId, message.getBody());
            Record record = null;
            do {
              try {
                record = parser.parse();
              } catch (RecoverableDataParserException e) {
                // log the error and keep trying to parse this message
                LOG.error(Errors.SQS_04.getMessage(), e.getMessage(), e);
                terminatingException = new StageException(Errors.SQS_04, e.getMessage(), e);
              } catch (DataParserException e) {
                // log the error and stop trying to parse this message
                LOG.error(Errors.SQS_07.getMessage(), e.getMessage(), e);
                errorRecordHandler.onError(Errors.SQS_07, e.getMessage(), e);
                break;
              }
            } while (record == null);

            setSqsAttributesOnRecord(message, record, nextQueueUrl, queueUrlToNamePrefix.get(nextQueueUrl));

            startBatchIfNeeded();
            batchContext.getBatchMaker().addRecord(record);
            commitQueueUrlsToMessages.put(nextQueueUrl, message);

            if (++batchRecordCount >= maxBatchSize) {
              cycleBatch();
            }
          } catch (DataParserException e) {
            LOG.error(Errors.SQS_05.getMessage(), e.getMessage(), e);
            terminatingException = new StageException(Errors.SQS_05, e.getMessage(), e);
            break;
          } finally {
            if (parser != null) {
              parser.close();
            }
          }
        }

        boolean batchMaxTimeElapsed = Clock.systemUTC().millis() > lastBatchStartTimestamp + maxBatchWaitTimeMs;
        if (batchMaxTimeElapsed) {
          cycleBatch();
        }
      } catch (InterruptedException e) {
        LOG.error("InterruptedException trying to get SQS messages: {}", e.getMessage(), e);
        Thread.currentThread().interrupt();
        break;
      } finally {
        if (nextQueueUrl != null) {
          urlQueue.put(nextQueueUrl);
        }
      }
    }
    flushBatch();
    Optional.ofNullable(asyncConsumer).ifPresent(AmazonSQSAsync::shutdown);
    return terminatingException;
  }

  private void setSqsAttributesOnRecord(Message message, Record record, String queueUrl, String queueNamePrefix) {
    final Record.Header header = record.getHeader();

    switch (sqsAttributesOption) {
      case ALL:
        header.setAttribute(SQS_QUEUE_URL_ATTRIBUTE, queueUrl);
        Optional.of(message.getMessageAttributes()).ifPresent(attrs -> attrs.forEach((name, val) -> {
          final String stringValue = val.getStringValue();
          if (stringValue != null) {
            header.setAttribute(SQS_MESSAGE_ATTRIBUTE_PREFIX + name, stringValue);
          }
        }));
        final String body = message.getBody();
        if (body != null) {
          header.setAttribute(SQS_MESSAGE_BODY_ATTRIBUTE, body);
        }
        final String bodyMd5 = message.getMD5OfBody();
        if (bodyMd5 != null) {
          header.setAttribute(SQS_MESSAGE_BODY_MD5_ATTRIBUTE, bodyMd5);
        }
        final String attrsMd5 = message.getMD5OfMessageAttributes();
        if (attrsMd5 != null) {
          header.setAttribute(SQS_MESSAGE_ATTRIBUTE_MD5_ATTRIBUTE, attrsMd5);
        }
        // fall through
      case BASIC:
        header.setAttribute(SQS_MESSAGE_ID_ATTRIBUTE, message.getMessageId());
        header.setAttribute(SQS_QUEUE_NAME_PREFIX_ATTRIBUTE, queueNamePrefix);
        header.setAttribute(SQS_REGION_ATTRIBUTE, awsRegionLabel);
        break;
      case NONE:
        // empty block
        break;
    }
  }

  private void cycleBatch() {
    batchFlushHelper(true);
  }

  private void flushBatch() {
    batchFlushHelper(false);
  }

  private void batchFlushHelper(boolean startNew) {
    if (batchContext != null) {
      if (!context.isPreview() && context.getDeliveryGuarantee() == DeliveryGuarantee.AT_MOST_ONCE) {
        commitMessages();
      }

      boolean batchSuccessful = context.processBatch(batchContext);

      if (!context.isPreview() &&
          batchSuccessful &&
          context.getDeliveryGuarantee() == DeliveryGuarantee.AT_LEAST_ONCE) {
        commitMessages();
      }
    }
    batchRecordCount = 0;
    if (startNew && !context.isStopped()) {
      batchContext = context.startBatch();
      lastBatchStartTimestamp = Clock.systemUTC().millis();
    }
  }

  private void commitMessages() {
    for (final String queueUrl : commitQueueUrlsToMessages.keySet()) {
      try {
        Map<String, DeleteMessageBatchRequestEntry> deleteRequestEntries = new HashMap<>();
        for (final Message message : commitQueueUrlsToMessages.get(queueUrl)) {
          deleteRequestEntries.put(
              message.getMessageId(),
              new DeleteMessageBatchRequestEntry()
                  .withReceiptHandle(message.getReceiptHandle())
                  .withId(message.getMessageId())
          );
          if (deleteRequestEntries.size() >= numMessagesPerRequest) {
            sendDeleteMessageBatchRequest(queueUrl, deleteRequestEntries.values());
            deleteRequestEntries.clear();
          }
        }
        if (!deleteRequestEntries.isEmpty()) {
          sendDeleteMessageBatchRequest(queueUrl, deleteRequestEntries.values());
        }
      } catch (final InterruptedException ex) {
        LOG.error(
            "InterruptedException trying to delete SQS messages with IDs {} in queue {}",
            getPendingDeleteMessageIds(queueUrl),
            queueUrl,
            ex
        );
        Thread.currentThread().interrupt();
        break;
      }
    }
    commitQueueUrlsToMessages.clear();
  }

  private void sendDeleteMessageBatchRequest(
      String queueUrl, Collection<DeleteMessageBatchRequestEntry> deleteRequestEntries
  ) throws InterruptedException {
    DeleteMessageBatchRequest deleteRequest = new DeleteMessageBatchRequest().withQueueUrl(queueUrl).withEntries(
        deleteRequestEntries);
    Future<DeleteMessageBatchResult> deleteResultFuture = sqsAsync.deleteMessageBatchAsync(deleteRequest);
    try {
      DeleteMessageBatchResult deleteResult = deleteResultFuture.get();
      if (deleteResult.getFailed() != null) {
        deleteResult.getFailed().forEach(failed -> LOG.error(
            "Failed to delete message ID {} from queue {} with code {}, sender fault {}",
            failed.getId(),
            queueUrl,
            failed.getCode(),
            failed.getSenderFault()
        ));
      }
      if (LOG.isDebugEnabled() && deleteResult.getSuccessful() != null) {
        deleteResult.getSuccessful().forEach(success -> LOG.debug("Successfully deleted message ID {} from queue {}",
            success.getId(),
            queueUrl
        ));
      }
    } catch (ExecutionException e) {
      String messageIds = getPendingDeleteMessageIds(queueUrl);
      LOG.error(Errors.SQS_08.getMessage(), messageIds, queueUrl, e.getMessage(), e);
      throw new StageException(Errors.SQS_08, messageIds, queueUrl, e.getMessage(), e);
    }
  }

  private String getPendingDeleteMessageIds(String queueUrl) {
    StringBuilder messageIds = new StringBuilder();
    commitQueueUrlsToMessages.get(queueUrl).forEach(message -> {
      if (messageIds.length() > 0) {
        messageIds.append(",");
      }
      messageIds.append(message.getMessageId());
    });
    return messageIds.toString();
  }

  private void startBatchIfNeeded() {
    if (batchContext == null) {
      cycleBatch();
    }
  }

  private String getRecordId(Message message, String queueUrl) {
    return String.format("SqsConsumer::%s::%s::%s", awsRegionLabel, queueUrl, message.getMessageId());
  }
}
