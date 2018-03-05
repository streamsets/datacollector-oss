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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirRunnable;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirRunnableBuilder;
import com.streamsets.pipeline.lib.dirspooler.SpoolDirUtil;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.streamsets.pipeline.stage.origin.spooldir.Errors.SPOOLDIR_35;

public class SpoolDirSource extends BasePushSource {
  private final static Logger LOG = LoggerFactory.getLogger(SpoolDirSource.class);

  public static final String OFFSET_VERSION =
      "$com.streamsets.pipeline.stage.origin.spooldir.SpoolDirSource.offset.version$";
  public static final String OFFSET_VERSION_ONE = "1";
  public static final String SPOOLDIR_CONFIG_BEAN_PREFIX = "conf.";
  public static final String SPOOLDIR_DATAFORMAT_CONFIG_PREFIX = SPOOLDIR_CONFIG_BEAN_PREFIX + "dataFormatConfig.";

  private static final int MIN_OVERRUN_LIMIT = 64 * 1024;
  private final SpoolDirConfigBean conf;

  private boolean useLastModified;
  private DirectorySpooler spooler;
  private int numberOfThreads;
  private ExecutorService executorService;
  private String lastSourceFileName;

  public SpoolDirSource(SpoolDirConfigBean conf) {
    this.conf = conf;
  }

  @Override
  public int getNumberOfThreads() {
    return numberOfThreads;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    numberOfThreads = conf.numberOfThreads;
    lastSourceFileName = null;

    conf.dataFormatConfig.checkForInvalidAvroSchemaLookupMode(
        conf.dataFormat,
        "conf.dataFormat",
        getContext(),
        issues
    );

    boolean waitForPathToBePresent = !validateDir(
        conf.spoolDir, Groups.FILES.name(),
        SPOOLDIR_CONFIG_BEAN_PREFIX + "spoolDir",
        issues, !conf.allowLateDirectory
    );

    // Whether overrunLimit is less than max limit is validated by DataParserFormatConfig.
    if (conf.overrunLimit * 1024 < MIN_OVERRUN_LIMIT) {
      issues.add(
          getContext().createConfigIssue(
              Groups.FILES.name(),
              SPOOLDIR_CONFIG_BEAN_PREFIX + "overrunLimit",
              Errors.SPOOLDIR_06
          )
      );
    }

    if (conf.batchSize < 1) {
      issues.add(
          getContext().createConfigIssue(
              Groups.FILES.name(),
              SPOOLDIR_CONFIG_BEAN_PREFIX + "batchSize",
              Errors.SPOOLDIR_14
          )
      );
    }

    if (conf.poolingTimeoutSecs < 1) {
      issues.add(
          getContext().createConfigIssue(
              Groups.FILES.name(),
              SPOOLDIR_CONFIG_BEAN_PREFIX + "poolingTimeoutSecs",
              Errors.SPOOLDIR_15
          )
      );
    }

    validateFilePattern(issues);

    if (conf.maxSpoolFiles < 1) {
      issues.add(
          getContext().createConfigIssue(
              Groups.FILES.name(),
              SPOOLDIR_CONFIG_BEAN_PREFIX + "maxSpoolFiles",
              Errors.SPOOLDIR_17
          )
      );
    }

    validateInitialFileToProcess(issues);

    if (conf.errorArchiveDir != null && !conf.errorArchiveDir.isEmpty()) {
      validateDir(
          conf.errorArchiveDir,
          Groups.POST_PROCESSING.name(),
          SPOOLDIR_CONFIG_BEAN_PREFIX + "errorArchiveDir",
          issues,
          true);
    }

    if (conf.postProcessing == PostProcessingOptions.ARCHIVE) {
      if (conf.archiveDir != null && !conf.archiveDir.isEmpty()) {
        validateDir(
            conf.archiveDir,
            Groups.POST_PROCESSING.name(),
            SPOOLDIR_CONFIG_BEAN_PREFIX + "archiveDir",
            issues,
            true);
      } else {
        issues.add(
            getContext().createConfigIssue(
                Groups.POST_PROCESSING.name(),
                SPOOLDIR_CONFIG_BEAN_PREFIX + "archiveDir",
                Errors.SPOOLDIR_11
            )
        );
      }
      if (conf.retentionTimeMins < 0) {
        issues.add(
            getContext().createConfigIssue(
                Groups.POST_PROCESSING.name(),
                SPOOLDIR_CONFIG_BEAN_PREFIX + "retentionTimeMins",
                Errors.SPOOLDIR_19
            )
        );
      }
    }

    // Override the StringBuilder pool size maintained by Text and Log Data Parser Factories.
    conf.dataFormatConfig.stringBuilderPoolSize = conf.numberOfThreads;
    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        Groups.FILES.name(),
        SPOOLDIR_DATAFORMAT_CONFIG_PREFIX,
        conf.overrunLimit * 1024,
        issues
    );

    if (issues.isEmpty()) {
      if (getContext().isPreview()) {
        conf.poolingTimeoutSecs = 1;
      }

      DirectorySpooler.Builder builder =
          DirectorySpooler.builder().setDir(conf.spoolDir).setFilePattern(conf.filePattern)
              .setMaxSpoolFiles(conf.maxSpoolFiles)
              .setPostProcessing(DirectorySpooler.FilePostProcessing.valueOf(conf.postProcessing.name()))
              .waitForPathAppearance(waitForPathToBePresent)
              .processSubdirectories(conf.processSubdirectories)
              .setSpoolingPeriodSec(conf.spoolingPeriod);

      if (conf.postProcessing == PostProcessingOptions.ARCHIVE) {
        builder.setArchiveDir(conf.archiveDir);
        builder.setArchiveRetention(conf.retentionTimeMins);
      }
      if (conf.errorArchiveDir != null && !conf.errorArchiveDir.isEmpty()) {
        builder.setErrorArchiveDir(conf.errorArchiveDir);
      }
      builder.setPathMatcherMode(conf.pathMatcherMode);
      builder.setContext(getContext());
      this.useLastModified = conf.useLastModified == FileOrdering.TIMESTAMP;
      builder.setUseLastModifiedTimestamp(useLastModified);
      spooler = builder.build();
      spooler.init(conf.initialFileToProcess);
    }

    return issues;
  }

  private boolean validateDir(
      String dir,
      String group,
      String config,
      List<ConfigIssue> issues,
      boolean addDirPresenceIssues
  ) {
    if (dir.isEmpty()) {
      issues.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_11));
    }
    return validateDirPresence(dir, group, config, issues, addDirPresenceIssues);
  }

  private boolean validateDirPresence(
      String dir,
      String group,
      String config,
      List<ConfigIssue> issues,
      boolean addDirPresenceIssues
  ) {
    File fDir = new File(dir);
    List<ConfigIssue> issuesToBeAdded = new ArrayList<ConfigIssue>();
    boolean isValid = true;
    if (!fDir.exists()) {
      issuesToBeAdded.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_12, dir));
      isValid = false;
    } else if (!fDir.isDirectory()) {
      issuesToBeAdded.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_13, dir));
      isValid = false;
    }
    if (addDirPresenceIssues) {
      issues.addAll(issuesToBeAdded);
    }
    return isValid;
  }

  private void validateFilePattern(List<ConfigIssue> issues) {
    if (conf.filePattern == null || conf.filePattern.trim().isEmpty()) {
      issues.add(
          getContext().createConfigIssue(
              Groups.FILES.name(),
              SPOOLDIR_CONFIG_BEAN_PREFIX + "filePattern",
              Errors.SPOOLDIR_32,
              conf.filePattern
          )
      );
    } else {
      try {
        DirectorySpooler.createPathMatcher(conf.filePattern, conf.pathMatcherMode);
      } catch (Exception ex) {
        issues.add(
            getContext().createConfigIssue(
                Groups.FILES.name(),
                SPOOLDIR_CONFIG_BEAN_PREFIX + "filePattern",
                Errors.SPOOLDIR_16,
                conf.filePattern,
                ex.toString(),
                ex
            )
        );
      }
    }
  }

  private void validateInitialFileToProcess(List<ConfigIssue> issues) {
    if (conf.initialFileToProcess != null && !conf.initialFileToProcess.isEmpty()) {
      try {
        PathMatcher pathMatcher = DirectorySpooler.createPathMatcher(conf.filePattern, conf.pathMatcherMode);
        if (!pathMatcher.matches(new File(conf.initialFileToProcess).toPath().getFileName())) {
          issues.add(
              getContext().createConfigIssue(
                  Groups.FILES.name(),
                  SPOOLDIR_CONFIG_BEAN_PREFIX + "initialFileToProcess",
                  Errors.SPOOLDIR_18,
                  conf.initialFileToProcess,
                  conf.filePattern
              )
          );
        }
      } catch (Exception ex) {
      }
    }
  }

  @Override
  public void destroy() {
    shutdownExecutorIfNeeded();
    executorService = null;

    if (spooler != null) {
      spooler.destroy();
    }
    super.destroy();
  }

  protected DirectorySpooler getSpooler() {
    return spooler;
  }

  @VisibleForTesting
  Map<String, Offset> handleLastSourceOffset(Map<String, String> lastSourceOffset, PushSource.Context context) throws StageException {
    Map<String, Offset> offsetMap = new HashMap<>();

    if (lastSourceOffset != null && lastSourceOffset.size() > 0) {
      if (lastSourceOffset.containsKey(Source.POLL_SOURCE_OFFSET_KEY)) {
        // version one
        Offset offset = new Offset(Offset.VERSION_ONE, lastSourceOffset.get(Source.POLL_SOURCE_OFFSET_KEY));

        //Remove Poll Source Offset key from the offset.
        context.commitOffset(Source.POLL_SOURCE_OFFSET_KEY, null);

        // commit the offset version
        context.commitOffset(OFFSET_VERSION, OFFSET_VERSION_ONE);
        context.commitOffset(offset.getFile(), offset.getOffsetString());

        offsetMap.put(offset.getFile(), offset);
        lastSourceFileName = offset.getFile();
      } else {
        String version = lastSourceOffset.get(OFFSET_VERSION);
        Set<String> key = lastSourceOffset.keySet();
        Iterator iterator = key.iterator();

        while (iterator.hasNext()) {
          String keyString = (String) iterator.next();
          if (keyString.equals(OFFSET_VERSION)) {
            continue;
          }
          Offset offset = new Offset(version, keyString, lastSourceOffset.get(keyString));
          offsetMap.put(offset.getFile(), offset);

          if (lastSourceFileName != null) {
            if (useLastModified) {
              // return the newest file in the offset
              if (SpoolDirUtil.compareFiles(
                  new File(spooler.getSpoolDir(), lastSourceFileName),
                  new File(spooler.getSpoolDir(), offset.getFile())
              )) {
                lastSourceFileName = offset.getFile();
              }
            } else {
              if (offset.getFile().compareTo(lastSourceFileName) < 0) {
                lastSourceFileName = offset.getFile();
              }
            }
          } else {
            lastSourceFileName = offset.getFile();
          }
        }
      }
    }

    if (offsetMap.isEmpty()) {
      // commit the offset version
      context.commitOffset(OFFSET_VERSION, OFFSET_VERSION_ONE);

      Offset offset = new Offset(Offset.VERSION_ONE, null);
      offsetMap.put(offset.getFile(), offset);
      lastSourceFileName = offset.getFile();
    }

    return offsetMap;
  }

  @VisibleForTesting
  String getLastSourceFileName() {
    return lastSourceFileName;
  }

  @Override
  public void produce(Map<String, String> lastSourceOffset, int maxBatchSize) throws StageException {
    int batchSize = Math.min(conf.batchSize, maxBatchSize);

    Map<String, Offset> newSourceOffset = handleLastSourceOffset(lastSourceOffset, getContext());

    try {
      executorService = new SafeScheduledExecutorService(numberOfThreads, SpoolDirRunnable.SPOOL_DIR_THREAD_PREFIX);

      ExecutorCompletionService<Future> completionService = new ExecutorCompletionService<>(executorService);

      IntStream.range(0, numberOfThreads).forEach(threadNumber -> {
        SpoolDirRunnable runnable = getSpoolDirRunnable(threadNumber, batchSize, newSourceOffset);
        completionService.submit(runnable, null);
      });

      for (int i = 0; i < getNumberOfThreads(); i++) {
        try {
          completionService.take().get();
        } catch (ExecutionException e) {
          LOG.error(
              "ExecutionException when attempting to wait for all runnables to complete, after context was" +
                  " stopped: {}",
              e.getMessage(),
              e
          );
          throw new StageException(SPOOLDIR_35, e);
        } catch (InterruptedException e) {
          LOG.error(
              "InterruptedException when attempting to wait for all runnables to complete, after context " +
                  "was stopped: {}",
              e.getMessage(),
              e
          );
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

  @VisibleForTesting
  SpoolDirRunnable getSpoolDirRunnable(int threadNumber, int batchSize, Map<String, Offset> lastSourceOffset) {
    return new SpoolDirRunnableBuilder()
        .context(getContext())
        .threadNumber(threadNumber)
        .batchSize(batchSize)
        .offsets(lastSourceOffset)
        .lastSourcFileName(getLastSourceFileName())
        .spooler(getSpooler())
        .conf(conf)
        .build();
  }
}
