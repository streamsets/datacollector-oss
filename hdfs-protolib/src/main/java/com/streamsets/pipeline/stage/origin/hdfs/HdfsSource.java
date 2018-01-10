/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.hdfs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import com.streamsets.pipeline.lib.hdfs.common.Errors;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class HdfsSource extends BasePushSource {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsSource.class);
  public static final String OFFSET_VERSION =
      "$com.streamsets.pipeline.stage.origin.hdfs.HdfsSource.offset.version$";
  private final HdfsSourceConfigBean hdfsSourceConfigBean;
  private final String OFFSET_VERSION_ONE = "1";
  private ExecutorService executorService;

  public HdfsSource(HdfsSourceConfigBean hdfsSourceConfigBean) {
    this.hdfsSourceConfigBean = hdfsSourceConfigBean;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();
    hdfsSourceConfigBean.init(getContext(), issues);
    return issues;
  }

  @Override
  public void destroy() {
    super.destroy();
  }

  @Override
  public int getNumberOfThreads() {
    return hdfsSourceConfigBean.numberOfThreads;
  }

  @VisibleForTesting
  FileSystem getFileSystem() {
    return hdfsSourceConfigBean.getFileSystem();
  }

  @Override
  public void produce(Map<String, String> lastOffsets, int maxBatchSize) throws StageException {
    String firstFile = handleLastOffsets(lastOffsets);

    if (!Strings.isNullOrEmpty(hdfsSourceConfigBean.firstFile)
        && hdfsSourceConfigBean.firstFile.compareTo(firstFile) > 0) {
      firstFile = hdfsSourceConfigBean.firstFile;
    }
    FileSystem fs = getFileSystem();

    HdfsDirectorySpooler.Builder builder =
          HdfsDirectorySpooler.builder()
              .setContext(getContext())
              .setFirstFile(firstFile)
              .setFileSystem(fs)
              .setDir(hdfsSourceConfigBean.dirPath)
              .setFilePattern(hdfsSourceConfigBean.pattern)
              .setMaxSpoolFiles(hdfsSourceConfigBean.maxSpoolSize);

    HdfsDirectorySpooler spooler = builder.build();
    spooler.init();

    try {
      executorService = new SafeScheduledExecutorService(getNumberOfThreads(), HdfsSourceRunnable.PREFIX);
      ExecutorCompletionService<Future> completionService = new ExecutorCompletionService<>(executorService);
      List<Future> allFutures = new LinkedList<>();
      IntStream.range(0, getNumberOfThreads()).forEach(threadNumber -> {
        HdfsSourceRunnable runnable = new HdfsSourceRunnableBuilder().threadNumber(threadNumber)
            .maxBatchSize(maxBatchSize)
            .context(getContext())
            .fileSystem(fs)
            .spooler(spooler)
            .lastOffsets(lastOffsets)
            .hdfsSourceConfigBean(hdfsSourceConfigBean)
            .build();
        allFutures.add(completionService.submit(runnable, null));
      });

      for (int i = 0; i < getNumberOfThreads(); i++) {
        try {
          // waiting for Future representing the next completed task
          Future future = completionService.take();
          if (future != null) {
            future.get();
          }
        } catch (ExecutionException e) {
          Throwable cause = Throwables.getRootCause(e);
          if (cause != null && cause instanceof StageException) {
            LOG.error(
                "ExecutionException when attempting to wait for all files HDFS runnables to complete, after context was" + " stopped: {}",
                e.getMessage(),
                e
            );
            throw (StageException) cause;
          } else {
            LOG.error("Internal Error. {}", e);
            throw new StageException(Errors.HADOOPFS_63, e.toString(), e);
          }
        } catch (InterruptedException e) {
          LOG.error(
              "InterruptedException when attempting to wait for all files HDFS runnables to complete, after context " + "was stopped: {}",
              e.getMessage(),
              e
          );
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      spooler.destroy();
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

  /**
   * returns the oldest file from the last offset
   */
  @VisibleForTesting
  String handleLastOffsets(Map<String, String> lastOffset) {
    String firstFile = "";
    for (String fileName : lastOffset.keySet()) {
      if (fileName.equals(OFFSET_VERSION)) {
        continue;
      }

      if (Strings.isNullOrEmpty(firstFile) || firstFile.compareTo(fileName) > 0) {
        firstFile = fileName;
      }
    }

    if (!lastOffset.containsKey(OFFSET_VERSION)) {
      getContext().commitOffset(OFFSET_VERSION, OFFSET_VERSION_ONE);
    }

    return firstFile;
  }
}
