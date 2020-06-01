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
package com.streamsets.pipeline.lib.dirspooler;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public abstract class SpoolDirBaseSource extends BasePushSource {
  public static final String OFFSET_VERSION_ONE = "1";
  public static final String SPOOLDIR_CONFIG_BEAN_PREFIX = "conf.";
  public static final String SPOOLDIR_DATAFORMAT_CONFIG_PREFIX = SPOOLDIR_CONFIG_BEAN_PREFIX + "dataFormatConfig.";
  private static final Logger LOG = LoggerFactory.getLogger(SpoolDirBaseSource.class);
  private static final int MB = 1024;
  private static final int MIN_OVERRUN_LIMIT = 64 * 1024;

  protected static String GROUPS_POST_PROCESSING_CONFIG_NAME;
  protected static String GROUP_FILE_CONFIG_NAME;

  protected String lastSourceFileName;
  protected DirectorySpooler spooler;
  protected boolean useLastModified;

  protected SpoolDirConfigBean conf;
  private int numberOfThreads;
  private ExecutorService executorService;
  private WrappedFileSystem fs;
  protected SpoolDirBaseContext spoolDirBaseContext;

  abstract public WrappedFileSystem getFs();

  abstract public Map<String, Offset> handleLastSourceOffset(Map<String, String> lastSourceOffset, PushSource.Context context)
      throws StageException;

  @Override
  public int getNumberOfThreads() {
    return numberOfThreads;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    fs = getFs();

    numberOfThreads = conf.numberOfThreads;
    this.spoolDirBaseContext = new SpoolDirBaseContext(getContext(), numberOfThreads);
    lastSourceFileName = null;

    conf.dataFormatConfig.checkForInvalidAvroSchemaLookupMode(conf.dataFormat,
        "conf.dataFormat",
        getContext(),
        issues
    );

    boolean waitForPathToBePresent = !validateDir(conf.spoolDir, GROUP_FILE_CONFIG_NAME,
        SPOOLDIR_CONFIG_BEAN_PREFIX + "spoolDir",
        issues,
        !conf.allowLateDirectory
    );

    // Whether overrunLimit is less than max limit is validated by DataParserFormatConfig.
    if (conf.overrunLimit * 1024 < MIN_OVERRUN_LIMIT) {
      issues.add(getContext().createConfigIssue(
          GROUP_FILE_CONFIG_NAME,
          SPOOLDIR_CONFIG_BEAN_PREFIX + "overrunLimit",
          Errors.SPOOLDIR_06
      ));
    }

    if (conf.batchSize < 1) {
      issues.add(getContext().createConfigIssue(
          GROUP_FILE_CONFIG_NAME,
          SPOOLDIR_CONFIG_BEAN_PREFIX + "batchSize",
          Errors.SPOOLDIR_14
      ));
    }

    if (conf.poolingTimeoutSecs < 1) {
      issues.add(getContext().createConfigIssue(
          GROUP_FILE_CONFIG_NAME,
          SPOOLDIR_CONFIG_BEAN_PREFIX + "poolingTimeoutSecs",
          Errors.SPOOLDIR_15
      ));
    }

    validateFilePattern(issues);

    if (conf.maxSpoolFiles < 1) {
      issues.add(getContext().createConfigIssue(
          GROUP_FILE_CONFIG_NAME,
          SPOOLDIR_CONFIG_BEAN_PREFIX + "maxSpoolFiles",
          Errors.SPOOLDIR_17
      ));
    }

    validateInitialFileToProcess(issues);

    if (conf.errorArchiveDir != null && !conf.errorArchiveDir.isEmpty()) {
      validateDir(conf.errorArchiveDir,
          GROUPS_POST_PROCESSING_CONFIG_NAME,
          SPOOLDIR_CONFIG_BEAN_PREFIX + "errorArchiveDir",
          issues,
          true
      );
    }

    if (conf.postProcessing == PostProcessingOptions.ARCHIVE) {
      if (conf.archiveDir != null && !conf.archiveDir.isEmpty()) {
        validateDir(conf.archiveDir,
            GROUPS_POST_PROCESSING_CONFIG_NAME,
            SPOOLDIR_CONFIG_BEAN_PREFIX + "archiveDir",
            issues,
            true
        );
      } else {
        issues.add(getContext().createConfigIssue(
            GROUPS_POST_PROCESSING_CONFIG_NAME,
            SPOOLDIR_CONFIG_BEAN_PREFIX + "archiveDir",
            Errors.SPOOLDIR_11
        ));
      }
      if (conf.retentionTimeMins < 0) {
        issues.add(getContext().createConfigIssue(
            GROUPS_POST_PROCESSING_CONFIG_NAME,
            SPOOLDIR_CONFIG_BEAN_PREFIX + "retentionTimeMins",
            Errors.SPOOLDIR_19
        ));
      }
    }

    // Override the StringBuilder pool size maintained by Text and Log Data Parser Factories.
    conf.dataFormatConfig.stringBuilderPoolSize = conf.numberOfThreads;
    conf.dataFormatConfig.init(getContext(),
        conf.dataFormat, GROUP_FILE_CONFIG_NAME,
        SPOOLDIR_DATAFORMAT_CONFIG_PREFIX,
        conf.overrunLimit * MB,
        issues
    );

    if (issues.isEmpty()) {
      if (getContext().isPreview()) {
        conf.poolingTimeoutSecs = 5;
      }

      try {
        DirectorySpooler.Builder builder = getDirectorySpoolerBuilder()
            .setWrappedFileSystem(getFs())
            .setDir(conf.spoolDir)
            .setFilePattern(conf.filePattern)
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
      } catch (IOException e) {
        issues.add(
            getContext().createConfigIssue(
                GROUP_FILE_CONFIG_NAME,
                SPOOLDIR_CONFIG_BEAN_PREFIX + "spoolDir",
                Errors.SPOOLDIR_32,
                conf.filePattern
            )
        );
      }

    }

    return issues;
  }

  protected DirectorySpooler.Builder getDirectorySpoolerBuilder() {
    return new DirectorySpooler.Builder();
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
    return validateDirPresenceAndPermissions(dir, group, config, issues, addDirPresenceIssues);
  }

  private boolean validateDirPresenceAndPermissions(
      String dir,
      String group,
      String config,
      List<ConfigIssue> issues,
      boolean addDirPresenceIssues
  ) {
    if (SpoolDirUtil.isGlobPattern(dir)) {
      dir = SpoolDirUtil.truncateGlobPatternDirectory(dir);
    }

    List<ConfigIssue> issuesToBeAdded = new ArrayList<>();
    boolean isValid = true;

    try {
      WrappedFile fDir = fs.getFile(dir);
      if (!fs.exists(fDir)) {
        issuesToBeAdded.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_12, dir));
        isValid = false;
      } else if (!fs.isDirectory(fDir)) {
        issuesToBeAdded.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_13, dir));
        isValid = false;
      } else if (!fDir.canRead()) {
        issuesToBeAdded.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_38, dir));
        isValid = false;
      }
    } catch (IOException e) {
      issuesToBeAdded.add(getContext().createConfigIssue(group, config, Errors.SPOOLDIR_36, dir, e));
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
              GROUP_FILE_CONFIG_NAME,
              SPOOLDIR_CONFIG_BEAN_PREFIX + "filePattern",
              Errors.SPOOLDIR_32,
              conf.filePattern
          )
      );
    } else {
      try {
        getFs().patternMatches("dummy");
      } catch (Exception ex) {
        issues.add(
            getContext().createConfigIssue(
                GROUP_FILE_CONFIG_NAME,
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
      WrappedFile file = null;
      try {
        file = fs.getFile(conf.initialFileToProcess);
      } catch (IOException e) {
        issues.add(
            getContext().createConfigIssue(
                GROUP_FILE_CONFIG_NAME,
                SPOOLDIR_CONFIG_BEAN_PREFIX + "initialFileToProcess",
                Errors.SPOOLDIR_36,
                conf.initialFileToProcess,
                e
            )
        );
      }

      if (!fs.patternMatches(file.getFileName())) {
        issues.add(
            getContext().createConfigIssue(
                GROUP_FILE_CONFIG_NAME,
                SPOOLDIR_CONFIG_BEAN_PREFIX + "initialFileToProcess",
                Errors.SPOOLDIR_18,
                conf.initialFileToProcess,
                conf.filePattern
            )
        );
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

  public DirectorySpooler getSpooler() {
    return spooler;
  }

  public String getLastSourceFileName() {
    return lastSourceFileName;
  }

  @Override
  public void produce(Map<String, String> lastSourceOffset, int maxBatchSize) throws StageException {
    int batchSize = Math.min(conf.batchSize, maxBatchSize);
    if (!getContext().isPreview() && conf.batchSize > maxBatchSize) {
      getContext().reportError(Errors.SPOOLDIR_37, maxBatchSize);
    }

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
          Throwable rootCause = Throwables.getRootCause(e);
          if(spooler!=null && spooler.getDestroyCause() != null)
            rootCause = spooler.getDestroyCause();

          if (rootCause instanceof StageException) {
            throw (StageException) rootCause;
          }
          throw new StageException(Errors.SPOOLDIR_35, rootCause);
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

  public SpoolDirRunnable getSpoolDirRunnable(int threadNumber, int batchSize, Map<String, Offset> lastSourceOffset) {
    return new SpoolDirRunnableBuilder()
        .context(getContext())
        .threadNumber(threadNumber)
        .batchSize(batchSize)
        .offsets(lastSourceOffset)
        .lastSourcFileName(getLastSourceFileName())
        .spooler(getSpooler())
        .conf(conf)
        .wrappedFileSystem(getFs())
        .spoolDirBaseContext(spoolDirBaseContext)
        .build();
  }
}
