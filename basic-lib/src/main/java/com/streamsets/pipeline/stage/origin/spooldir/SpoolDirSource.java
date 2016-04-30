/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import com.streamsets.pipeline.lib.io.ObjectLengthException;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.PathMatcher;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SpoolDirSource extends BaseSource {
  private final static Logger LOG = LoggerFactory.getLogger(SpoolDirSource.class);
  private static final String OFFSET_SEPARATOR = "::";
  private static final String MINUS_ONE = "-1";
  private static final String ZERO = "0";
  private static final String NULL_FILE = "NULL_FILE_ID-48496481-5dc5-46ce-9c31-3ab3e034730c";
  private static final int MIN_OVERRUN_LIMIT = 64 * 1024;
  public static final String SPOOLDIR_CONFIG_BEAN_PREFIX = "conf.";
  public static final String SPOOLDIR_DATAFORMAT_CONFIG_PREFIX = SPOOLDIR_CONFIG_BEAN_PREFIX + "dataFormatConfig.";


  private final SpoolDirConfigBean conf;

  public SpoolDirSource(SpoolDirConfigBean conf) {
    this.conf = conf;
  }

  private DirectorySpooler spooler;
  private File currentFile;
  private DataParserFactory parserFactory;
  private DataParser parser;

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

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

    conf.dataFormatConfig.init(
        getContext(),
        conf.dataFormat,
        Groups.FILES.name(),
        SPOOLDIR_DATAFORMAT_CONFIG_PREFIX,
        conf.overrunLimit * 1024,
        issues
    );

    if (issues.isEmpty()) {
      parserFactory = conf.dataFormatConfig.getParserFactory();

      if (getContext().isPreview()) {
        conf.poolingTimeoutSecs = 1;
      }

      DirectorySpooler.Builder builder =
          DirectorySpooler.builder().setDir(conf.spoolDir).setFilePattern(conf.filePattern)
              .setMaxSpoolFiles(conf.maxSpoolFiles)
              .setPostProcessing(DirectorySpooler.FilePostProcessing.valueOf(conf.postProcessing.name()))
              .waitForPathAppearance(waitForPathToBePresent);
      if (conf.postProcessing == PostProcessingOptions.ARCHIVE) {
        builder.setArchiveDir(conf.archiveDir);
        builder.setArchiveRetention(conf.retentionTimeMins);
      }
      if (conf.errorArchiveDir != null && !conf.errorArchiveDir.isEmpty()) {
        builder.setErrorArchiveDir(conf.errorArchiveDir);
      }
      builder.setContext(getContext());
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
        DirectorySpooler.createPathMatcher(conf.filePattern);
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
        PathMatcher pathMatcher = DirectorySpooler.createPathMatcher(conf.filePattern);
        if (!pathMatcher.matches(new File(conf.initialFileToProcess).toPath())) {
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
    IOUtils.closeQuietly(parser);
    if (spooler != null) {
      spooler.destroy();
    }
    super.destroy();
  }

  protected DirectorySpooler getSpooler() {
    return spooler;
  }

  protected String getFileFromSourceOffset(String sourceOffset) throws StageException {
    String file = null;
    if (sourceOffset != null) {
      file = sourceOffset;
      int separator = sourceOffset.indexOf(OFFSET_SEPARATOR);
      if (separator > -1) {
        file = sourceOffset.substring(0, separator);
        if (NULL_FILE.equals(file)) {
          file = null;
        }
      }
    }
    return file;
  }

  protected String getOffsetFromSourceOffset(String sourceOffset) throws StageException {
    String offset = ZERO;
    if (sourceOffset != null) {
      int separator = sourceOffset.indexOf(OFFSET_SEPARATOR);
      if (separator > -1) {
        offset = sourceOffset.substring(separator + OFFSET_SEPARATOR.length());
      }
    }
    return offset;
  }

  protected String createSourceOffset(String file, String fileOffset) {
    return (file == null) ? NULL_FILE + OFFSET_SEPARATOR + fileOffset : file + OFFSET_SEPARATOR + fileOffset;
  }

  private boolean hasToFetchNextFileFromSpooler(String file, String offset) {
    return
        // we don't have a current file half way processed in the current agent execution
        currentFile == null ||
            // we don't have a file half way processed from a previous agent execution via offset tracking
            file == null ||
            // the current file is lexicographically lesser than the one reported via offset tracking
            // this can happen if somebody drop
            currentFile.getName().compareTo(file) < 0 ||
            // the current file has been fully processed
            MINUS_ONE.equals(offset);
  }

  private boolean isFileFromSpoolerEligible(File spoolerFile, String offsetFile, String offsetInFile) {
    if (spoolerFile == null) {
      // there is no new file from spooler, we return yes to break the loop
      return true;
    }
    if (offsetFile == null) {
      // file reported by offset tracking is NULL, means we are starting from zero
      return true;
    }
    if (spoolerFile.getName().compareTo(offsetFile) == 0 && !MINUS_ONE.equals(offsetInFile)) {
      // file reported by spooler is equal than current offset file
      // and we didn't fully process (not -1) the current file
      return true;
    }
    if (spoolerFile.getName().compareTo(offsetFile) > 0) {
      // file reported by spooler is newer than current offset file
      return true;
    }
    return false;
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    int batchSize = Math.min(conf.batchSize, maxBatchSize);
    // if lastSourceOffset is NULL (beginning of source) it returns NULL
    String file = getFileFromSourceOffset(lastSourceOffset);
    // if lastSourceOffset is NULL (beginning of source) it returns 0
    String offset = getOffsetFromSourceOffset(lastSourceOffset);
    if (hasToFetchNextFileFromSpooler(file, offset)) {
      currentFile = null;
      try {
        File nextAvailFile = null;
        do {
          if (nextAvailFile != null) {
            LOG.warn("Ignoring file '{}' in spool directory as is lesser than offset file '{}'",
                nextAvailFile.getName(), file);
          }
          nextAvailFile = getSpooler().poolForFile(conf.poolingTimeoutSecs, TimeUnit.SECONDS);
        } while (!isFileFromSpoolerEligible(nextAvailFile, file, offset));

        if (nextAvailFile == null) {
          // no file to process
          LOG.debug("No new file available in spool directory after '{}' secs, producing empty batch",
              conf.poolingTimeoutSecs);
        } else {
          // file to process
          currentFile = nextAvailFile;

          // if the current offset file is null or the file returned by the spooler is greater than the current offset
          // file we take the file returned by the spooler as the new file and set the offset to zero
          // if not, it means the spooler returned us the current file, we just keep processing it from the last
          // offset we processed (known via offset tracking)
          if (file == null || nextAvailFile.getName().compareTo(file) > 0) {
            file = currentFile.getName();
            offset = ZERO;
          }
        }
      } catch (InterruptedException ex) {
        // the spooler was interrupted while waiting for a file, we log and return, the pipeline agent will invoke us
        // again to wait for a file again
        LOG.warn("Pooling interrupted");
      }
    }

    if (currentFile != null) {
      // we have a file to process (from before or new from spooler)
      try {
        // we ask for a batch from the currentFile starting at offset
        offset = produce(currentFile, offset, batchSize, batchMaker);
      } catch (BadSpoolFileException ex) {
        LOG.error(Errors.SPOOLDIR_01.getMessage(), ex.getFile(), ex.getPos(), ex.toString(), ex);
        getContext().reportError(Errors.SPOOLDIR_01, ex.getFile(), ex.getPos(), ex.toString(), ex);
        try {
          // then we ask the spooler to error handle the failed file
          spooler.handleCurrentFileAsError();
        } catch (IOException ex1) {
          throw new StageException(Errors.SPOOLDIR_00, currentFile, ex1.toString(), ex1);
        }
        // we set the offset to -1 to indicate we are done with the file and we should fetch a new one from the spooler
        offset = MINUS_ONE;
      }
    }
    // create a new offset using the current file and offset
    return createSourceOffset(file, offset);
  }

  /**
   * Processes a batch from the specified file and offset up to a maximum batch size. If the file is fully process
   * it must return -1, otherwise it must return the offset to continue from next invocation.
   */
  public String produce(File file, String offset, int maxBatchSize, BatchMaker batchMaker) throws StageException,
      BadSpoolFileException {
    String sourceFile = file.getName();
    try {
      if (parser == null) {
        if (conf.dataFormat == DataFormat.AVRO) {
          parser = parserFactory.getParser(file, offset);
        } else {
          parser = parserFactory.getParser(file.getName(), new FileInputStream(file), offset);
        }
      }
      for (int i = 0; i < maxBatchSize; i++) {
        try {
          Record record = parser.parse();
          if (record != null) {
            record.getHeader().setAttribute(HeaderAttributeConstants.FILE, file.getPath());
            record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, offset == null ? "0" : offset);
            batchMaker.addRecord(record);
            offset = parser.getOffset();
          } else {
            parser.close();
            parser = null;
            offset = MINUS_ONE;
            break;
          }
        } catch (ObjectLengthException ex) {
          String exOffset = offset;
          offset = MINUS_ONE;
          switch (getContext().getOnErrorRecord()) {
            case DISCARD:
              break;
            case TO_ERROR:
              getContext().reportError(Errors.SPOOLDIR_02, sourceFile, exOffset, ex);
              break;
            case STOP_PIPELINE:
              throw new StageException(Errors.SPOOLDIR_02, sourceFile, exOffset);
            default:
              throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
                  getContext().getOnErrorRecord(), ex));
          }
        }
      }
    } catch (IOException|DataParserException ex) {
      if (ex instanceof ClosedByInterruptException || ex.getCause() instanceof ClosedByInterruptException) {
        //If the pipeline was stopped, we may get a ClosedByInterruptException while reading avro data.
        //This is because the thread is interrupted when the pipeline is stopped.
        //Instead of sending the file to error, publish batch and move one.
      } else {
        offset = MINUS_ONE;
        String exOffset;
        if (ex instanceof OverrunException) {
          exOffset = String.valueOf(((OverrunException) ex).getStreamOffset());
        } else {
          try {
            exOffset = (parser != null) ? parser.getOffset() : MINUS_ONE;
          } catch (IOException ex1) {
            LOG.warn("Could not get the file offset to report with error, reason: {}", ex1.toString(), ex);
            exOffset = MINUS_ONE;
          }
        }
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            // we failed to produce a record, which leaves the input file in an unknown state. all we can do here is
            // throw an exception.
            throw new BadSpoolFileException(file.getAbsolutePath(), exOffset, ex);
          case STOP_PIPELINE:
            getContext().reportError(Errors.SPOOLDIR_04, sourceFile, exOffset, ex.toString(), ex);
            throw new StageException(Errors.SPOOLDIR_04, sourceFile, exOffset, ex.toString());
          default:
            throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
                getContext().getOnErrorRecord(), ex));
        }
      }
    } finally {
      if (MINUS_ONE.equals(offset)) {
        if (parser != null) {
          try {
            parser.close();
            parser = null;
          } catch (IOException ex) {
            //NOP
          }
        }
      }
    }
    return offset;
  }

}
