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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ToErrorContext;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.lib.aws.AWSRegions;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class SqsConsumer extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(SqsConsumer.class);
  private static final String SQS_CONFIG_PREFIX = "sqsConfig.";
  private static final String SQS_DATA_FORMAT_CONFIG_PREFIX = SQS_CONFIG_PREFIX + "dataFormatConfig.";
  private static final int ONE_MB = 1024 * 1024;
  private static final String SQS_THREAD_PREFIX = "SQS Consumer Worker - ";
  private final SqsConsumerConfigBean conf;
  private final BlockingQueue<Throwable> error = new SynchronousQueue<>();

  private DataParserFactory parserFactory;
  private ExecutorService executorService;

  private ClientConfiguration clientConfiguration;
  private AWSCredentialsProvider credentials;
  private final Map<String, String> queueUrlToPrefix = new HashMap<>();

  public SqsConsumer(SqsConsumerConfigBean conf) {
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    if (conf.region == AWSRegions.OTHER && (conf.endpoint == null || conf.endpoint.isEmpty())) {
      issues.add(getContext().createConfigIssue(
          Groups.SQS.name(),
          SQS_CONFIG_PREFIX + "endpoint",
          Errors.SQS_01
      ));

      return issues;
    }

    conf.dataFormatConfig.stringBuilderPoolSize = getNumberOfThreads();

    if (issues.isEmpty()) {
      conf.dataFormatConfig.init(
          getContext(),
          conf.dataFormat,
          Groups.SQS.name(),
          SQS_DATA_FORMAT_CONFIG_PREFIX,
          ONE_MB,
          issues
      );

      parserFactory = conf.dataFormatConfig.getParserFactory();
    }

    try {
      clientConfiguration = AWSUtil.getClientConfiguration(conf.proxyConfig);
    } catch (StageException e) {
      issues.add(getContext().createConfigIssue(
          Groups.SQS.name(),
          SQS_CONFIG_PREFIX + "proxyConfig",
          Errors.SQS_10,
          e.getMessage(),
          e
      ));
      return issues;
    }
    try {
      credentials = AWSUtil.getCredentialsProvider(conf.awsConfig);
    } catch (StageException e) {
      issues.add(getContext().createConfigIssue(
          Groups.SQS.name(),
          SQS_CONFIG_PREFIX + "awsConfig",
          Errors.SQS_11,
          e.getMessage(),
          e
      ));
      return issues;
    }

    AmazonSQS validationClient = AmazonSQSClientBuilder.standard().withRegion(conf.region.getLabel())
        .withClientConfiguration(clientConfiguration).withCredentials(credentials).build();

    for (int i = 0; i < conf.queuePrefixes.size(); i++) {
      final String queueNamePrefix = conf.queuePrefixes.get(i);
      ListQueuesResult result = validationClient.listQueues(new ListQueuesRequest(queueNamePrefix));
      if (LOG.isDebugEnabled()) {
        LOG.debug("ListQueuesResult for prefix {}: {}", queueNamePrefix, result);
      }
      if (result.getQueueUrls().size() == 0) {
        //TODO: set index in issue when API-138 is implemented
        issues.add(getContext().createConfigIssue(
            Groups.SQS.name(),
            SQS_CONFIG_PREFIX + "queuePrefixes",
            Errors.SQS_02,
            queueNamePrefix
        ));
      }
      result.getQueueUrls().forEach(url -> queueUrlToPrefix.put(url, queueNamePrefix));
    }

    if (queueUrlToPrefix.isEmpty()) {
      issues.add(getContext().createConfigIssue(
          Groups.SQS.name(),
          SQS_CONFIG_PREFIX + "queuePrefixes",
          Errors.SQS_09
      ));
    }

    return issues;
  }

  private AmazonSQSAsync buildAsyncClient() {
    final AmazonSQSAsyncClientBuilder builder = AmazonSQSAsyncClientBuilder.standard();
    builder.setRegion(conf.region.getLabel());
    builder.setCredentials(credentials);
    builder.setClientConfiguration(clientConfiguration);
    return builder.build();
  }

  @Override
  public void destroy() {
    Optional.ofNullable(executorService).ifPresent(ExecutorService::shutdownNow);
    super.destroy();
  }

  @Override
  public int getNumberOfThreads() {
    return conf.numThreads;
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
    try {
      final int numThreads = getNumberOfThreads();
      executorService = new SafeScheduledExecutorService(numThreads, SQS_THREAD_PREFIX);

      ExecutorCompletionService<Exception> completionService = new ExecutorCompletionService<>(executorService);

      IntStream.range(0, numThreads).forEach(threadNumber -> {
        final List<String> threadQueueUrls = getQueueUrlsForThread(new ArrayList<>(queueUrlToPrefix.keySet()), threadNumber, numThreads);
        final Map<String, String> threadQueueUrlsToNames = new HashMap<>();
        threadQueueUrls.forEach(url -> threadQueueUrlsToNames.put(url, queueUrlToPrefix.get(url)));
        if (threadQueueUrlsToNames.isEmpty()) {
          if (LOG.isWarnEnabled()) {
            LOG.warn("No queues available for thread {}, so it will not be run", threadNumber);
          }
        } else {
          SqsConsumerWorkerCallable workerCallable = new SqsConsumerWorkerCallable(
              buildAsyncClient(),
              getContext(),
              threadQueueUrlsToNames,
              conf.numberOfMessagesPerRequest,
              conf.maxBatchTimeMs,
              conf.maxBatchSize,
              parserFactory,
              conf.region.getLabel(),
              conf.sqsAttributesOption,
              new DefaultErrorRecordHandler(getContext(), (ToErrorContext) getContext()),
              conf.pollWaitTimeSeconds,
              conf.sqsMessageAttributeNames
          );

          completionService.submit(workerCallable);
        }
      });

      while (!getContext().isStopped()) {
        checkWorkerStatus(completionService);
      }
    } finally {
      shutdownExecutorIfNeeded();
    }

    try {
      CompletableFuture.supplyAsync(() -> {
        while (!getContext().isStopped()) {
          // To handle OnError STOP_PIPELINE we keep checking for an exception thrown
          // by any record processor in order to perform a graceful shutdown.
          try {
            Throwable t = error.poll(100, TimeUnit.MILLISECONDS);
            if (t != null) {
              return Optional.of(t);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        return Optional.<Throwable>empty();
      }).get().ifPresent(t -> {
        throw Throwables.propagate(t);
      });
    } catch (InterruptedException | ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  private static List<String> getQueueUrlsForThread(List<String> allUrls, int threadNumber, int maxThreads) {
    final List<String> urls = new LinkedList<>();
    IntStream.range(0, allUrls.size()).filter(i -> i % maxThreads == threadNumber).forEach(
        i -> urls.add(allUrls.get(i))
    );
    return urls;
  }

  private void checkWorkerStatus(ExecutorCompletionService<Exception> completionService) throws StageException {
    Future<Exception> future = completionService.poll();
    if (future != null) {
      try {
        Exception terminatingException = future.get();
        if (terminatingException != null) {
          if (terminatingException instanceof StageException) {
            throw (StageException) terminatingException;
          } else {
            throw new StageException(Errors.SQS_06, terminatingException.getMessage(), terminatingException);
          }
        }
      } catch (InterruptedException e) {
        LOG.error("Thread interrupted", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        Throwable cause = Throwables.getRootCause(e);
        if (cause != null && cause instanceof StageException) {
          throw (StageException) cause;
        } else {
          LOG.error("ExecutionException attempting to get completion service result: {}", e.getMessage(), e);
          throw new StageException(Errors.SQS_03, e.toString(), e);
        }
      }
    }
  }

  private void shutdownExecutorIfNeeded() {
    Optional.ofNullable(executorService).ifPresent(executor -> {
      if (!executor.isTerminated()) {
        LOG.info("Shutting down executor service");
        executor.shutdown();
      }
    });
  }
}
