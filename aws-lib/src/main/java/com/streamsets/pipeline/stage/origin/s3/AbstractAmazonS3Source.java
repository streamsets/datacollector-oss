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
package com.streamsets.pipeline.stage.origin.s3;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.InterfaceAudience;
import com.streamsets.pipeline.api.InterfaceStability;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public abstract class AbstractAmazonS3Source extends BasePushSource {

  private final static Logger LOG = LoggerFactory.getLogger(AbstractAmazonS3Source.class);

  protected final S3ConfigBean s3ConfigBean;
  private S3Spooler spooler;

  private int numberOfThreads;

  private ExecutorService executorService;

  private AmazonS3Source amazonS3Source;

  public AbstractAmazonS3Source(S3ConfigBean s3ConfigBean) {
    this.s3ConfigBean = s3ConfigBean;
  }

  @Override
  public int getNumberOfThreads() {
    return numberOfThreads;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    amazonS3Source = new AmazonS3SourceImpl(s3ConfigBean);

    numberOfThreads = s3ConfigBean.numberOfThreads;

    //init configuration
    s3ConfigBean.init(getContext(), issues);

    //preview settings
    if (getContext().isPreview()) {
      s3ConfigBean.basicConfig.maxWaitTime = 1000;
    }
    //init spooler
    if (issues.isEmpty()) {
      spooler = new S3Spooler(getContext(), s3ConfigBean);
      spooler.init();
    }

    return issues;
  }

  @Override
  public void destroy() {
    s3ConfigBean.destroy();
    if (spooler != null) {
      spooler.destroy();
    }
    super.destroy();
  }

  @Override
  public void produce(Map<String, String> lastSourceOffset, int maxBatchSize) throws StageException {
    int batchSize = Math.min(s3ConfigBean.basicConfig.maxBatchSize, maxBatchSize);
    if (!getContext().isPreview() && s3ConfigBean.basicConfig.maxBatchSize > maxBatchSize) {
      getContext().reportError(Errors.S3_SPOOLDIR_27, maxBatchSize);
    }

    amazonS3Source.handleOffset(lastSourceOffset, getContext());

    executorService = new SafeScheduledExecutorService(numberOfThreads, S3Constants.AMAZON_S3_THREAD_PREFIX);

    ExecutorCompletionService<Future> completionService = new ExecutorCompletionService<>(executorService);

    try {
      IntStream.range(0, numberOfThreads).forEach(threadNumber -> {
        AmazonS3Runnable runnable = getAmazonS3Runnable(batchSize, threadNumber);
        completionService.submit(runnable, null);
      });
      for (int i = 0; i < getNumberOfThreads(); i++) {
        try {
          completionService.take().get();
        } catch (ExecutionException e) {
          LOG.error("ExecutionException when attempting to wait for all runnables to complete, after context was" +
              " stopped: {}", e.getMessage(), e);
          final Throwable rootCause = Throwables.getRootCause(e);
          if (rootCause instanceof StageException) {
            throw (StageException) rootCause;
          }
          throw new StageException(Errors.S3_SPOOLDIR_26, rootCause);
        } catch (InterruptedException e) {
          LOG.error("InterruptedException when attempting to wait for all runnables to complete, after context " +
              "was stopped: {}", e.getMessage(), e);
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      shutdownExecutorIfNeeded();
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

  private AmazonS3Runnable getAmazonS3Runnable(int batchSize, int threadNumber) {
    return new AmazonS3RunnableBuilder().s3ConfigBean(s3ConfigBean)
                                        .context(getContext())
                                        .batchSize(batchSize)
                                        .spooler(spooler)
                                        .amazonS3Source(amazonS3Source)
                                        .threadNumber(threadNumber)
                                        .build();
  }
}
