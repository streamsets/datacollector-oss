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
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BasePushSource;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.io.ObjectLengthException;
import com.streamsets.pipeline.api.ext.io.OverrunException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.config.PostProcessingOptions;
import com.streamsets.pipeline.lib.dirspooler.DirectorySpooler;
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
import com.streamsets.pipeline.lib.io.fileref.LocalFileRef;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import com.streamsets.pipeline.lib.parser.RecoverableDataParserException;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.HeaderAttributeConstants;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SpoolDirSource extends BasePushSource {
  private final static Logger LOG = LoggerFactory.getLogger(SpoolDirSource.class);
  private static final String OFFSET_SEPARATOR = "::";
  private static final String MINUS_ONE = "-1";
  private static final String ZERO = "0";
  private static final String NULL_FILE = "NULL_FILE_ID-48496481-5dc5-46ce-9c31-3ab3e034730c";
  private static final int ONE = 1;
  static final String PERMISSIONS = "permissions";

  private static final String BASE_DIR = "baseDir";

  private static final int MIN_OVERRUN_LIMIT = 64 * 1024;
  public static final String SPOOLDIR_CONFIG_BEAN_PREFIX = "conf.";
  public static final String SPOOLDIR_DATAFORMAT_CONFIG_PREFIX = SPOOLDIR_CONFIG_BEAN_PREFIX + "dataFormatConfig.";
  private boolean useLastModified;

  private final SpoolDirConfigBean conf;

  private DirectorySpooler spooler;
  private File currentFile;
  private ErrorRecordHandler errorRecordHandler;
  private DataParserFactory parserFactory;
  private DataParser parser;

  private ELEval rateLimitElEval;
  private ELVars rateLimitElVars;

  private boolean shouldSendNoMoreDataEvent;
  private long noMoreDataRecordCount;
  private long noMoreDataErrorCount;
  private long noMoreDataFileCount;

  private long perFileRecordCount;
  private long perFileErrorCount;

  private long totalFiles;


  public SpoolDirSource(SpoolDirConfigBean conf) {
    this.conf = conf;
  }

  @Override
  public int getNumberOfThreads() {
    return ONE;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

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
              .waitForPathAppearance(waitForPathToBePresent)
              .processSubdirectories(conf.processSubdirectories);

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
      rateLimitElEval = FileRefUtil.createElEvalForRateLimit(getContext());;
      rateLimitElVars = getContext().createELVars();
    }

    shouldSendNoMoreDataEvent = false;
    noMoreDataRecordCount = 0;
    noMoreDataErrorCount = 0;
    noMoreDataFileCount = 0;

    perFileErrorCount = 0;
    perFileRecordCount = 0;

    totalFiles = 0;

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
    IOUtils.closeQuietly(parser);
    if (spooler != null) {
      spooler.destroy();
    }
    super.destroy();
  }

  protected DirectorySpooler getSpooler() {
    return spooler;
  }

  protected String getFileFromSourceOffset(Map<String, String> sourceOffset) throws StageException {
    String sourceOffsetFile = null;
    if (sourceOffset != null && sourceOffset.size() > 0) {
      sourceOffsetFile = sourceOffset.get(Source.POLL_SOURCE_OFFSET_KEY);
      int separator = sourceOffsetFile.indexOf(OFFSET_SEPARATOR);
      if (separator > -1) {
        sourceOffsetFile = sourceOffsetFile.substring(0, separator);
        if (NULL_FILE.equals(sourceOffsetFile)) {
          sourceOffsetFile = null;
        }
      }
    }
    return sourceOffsetFile;
  }

  protected String getOffsetFromSourceOffset(Map<String, String> sourceOffset) throws StageException {
    String offset = ZERO;
    if (sourceOffset != null && sourceOffset.size() > 0) {
      int separator = sourceOffset.get(Source.POLL_SOURCE_OFFSET_KEY).indexOf(OFFSET_SEPARATOR);
      if (separator > -1) {
        offset = sourceOffset.get(Source.POLL_SOURCE_OFFSET_KEY).substring(separator + OFFSET_SEPARATOR.length());
      }

      if (offset == null || offset.length() < 1 ) {
        offset = ZERO;
      }
    }
    return offset;
  }

  protected String createSourceOffset(String file, String fileOffset) {
    StringBuilder newOffset = new StringBuilder();

    if (file == null) {
      newOffset.append(NULL_FILE);
    } else {
      newOffset.append(file);
    }

    if (fileOffset != null && fileOffset.isEmpty()) {
      newOffset.append(OFFSET_SEPARATOR + MINUS_ONE);
    } else if (fileOffset != null) {
      newOffset.append(OFFSET_SEPARATOR + fileOffset);
    }

    return newOffset.toString();
  }

  private boolean hasToFetchNextFileFromSpooler(String file, String offset) {
    return
        // we don't have a current file half way processed in the current agent execution
        currentFile == null ||
            // we don't have a file half way processed from a previous agent execution via offset tracking
            file == null ||
            (useLastModified && compareFiles(new File(spooler.getSpoolDir(), file), currentFile)) ||
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
    if (spoolerFile.getAbsolutePath().compareTo(offsetFile) == 0 && !MINUS_ONE.equals(offsetInFile)) {
      // file reported by spooler is equal than current offset file
      // and we didn't fully process (not -1) the current file
      return true;
    }
    if (useLastModified) {
      if (compareFiles(spoolerFile, new File(offsetFile))) {
        return true;
      }
    } else {
      if (spoolerFile.getAbsolutePath().compareTo(offsetFile) > 0) {
        // file reported by spooler is newer than current offset file
        return true;
      }
    }
    return false;
  }

  /**
   * True if f1 is "newer" than f2.
   */
  private boolean compareFiles(File f1, File f2) {
    return f1.lastModified() > f2.lastModified() ||
            (f1.lastModified() == f2.lastModified() && f1.getName().compareTo(f2.getName()) > 0);
  }

  @VisibleForTesting
  void setErrorRecordHandler(DefaultErrorRecordHandler errorRecordHandler) {
    this.errorRecordHandler = errorRecordHandler;
  }

  @Override
  public void produce(Map<String, String> lastSourceOffset, int maxBatchSize) throws StageException {
    int batchSize = Math.min(conf.batchSize, maxBatchSize);
    while (!getContext().isStopped()) {
      BatchContext batchContext = getContext().startBatch();
      BatchMaker batchMaker = batchContext.getBatchMaker();
      errorRecordHandler = new DefaultErrorRecordHandler(getContext(), batchContext);

      String newSourceOffset = produceBatch(lastSourceOffset, batchSize, batchContext, batchMaker);
      lastSourceOffset = ImmutableMap.of(Source.POLL_SOURCE_OFFSET_KEY, newSourceOffset);
    }
  }

  private String produceBatch(Map<String, String> lastSourceOffset, int batchSize, BatchContext batchContext, BatchMaker batchMaker) throws StageException {
    // if lastSourceOffset is NULL (beginning of source) it returns NULL
    String file = getFileFromSourceOffset(lastSourceOffset);
    String fullPath = (file != null) ? getSpooler().getSpoolDir() + "/" + file : null;
    // if lastSourceOffset is NULL (beginning of source) it returns 0
    String offset = getOffsetFromSourceOffset(lastSourceOffset);

    if (hasToFetchNextFileFromSpooler(fullPath, offset)) {
      currentFile = null;
      try {
        File nextAvailFile = null;
        do {
          if (nextAvailFile != null) {
            LOG.warn("Ignoring file '{}' in spool directory as is lesser than offset file '{}'",
                nextAvailFile.toString(),
                fullPath
            );
          }
          nextAvailFile = getSpooler().poolForFile(conf.poolingTimeoutSecs, TimeUnit.SECONDS);
        } while (!isFileFromSpoolerEligible(nextAvailFile, fullPath, offset));

        if (nextAvailFile == null) {
          // no file to process
          LOG.debug("No new file available in spool directory after '{}' secs, producing empty batch",
              conf.poolingTimeoutSecs);

          // no-more-data event needs to be sent.
          shouldSendNoMoreDataEvent = true;

        } else {
          // since we have data to process, don't trigger the no-more-data event.
          shouldSendNoMoreDataEvent = false;

          // file to process
          currentFile = nextAvailFile;

          // if the current offset file is null or the file returned by the spooler is greater than the current offset
          // file we take the file returned by the spooler as the new file and set the offset to zero
          // if not, it means the spooler returned us the current file, we just keep processing it from the last
          // offset we processed (known via offset tracking)
          boolean pickFileFromSpooler = false;
          if (file == null) {
            pickFileFromSpooler = true;
          } else if (useLastModified) {
            File fileObject = new File(spooler.getSpoolDir(), file);
            if (compareFiles(nextAvailFile, fileObject)) {
              pickFileFromSpooler = true;
            }
          } else if (nextAvailFile.getName().compareTo(file) > 0) {
            pickFileFromSpooler = true;
          }
          if (pickFileFromSpooler) {
            file = currentFile.toString().replaceFirst(spooler.getSpoolDir() + "/", "");
            offset = ZERO;
          }
        }

        if (currentFile != null) {
          perFileRecordCount = 0;
          perFileErrorCount = 0;
          SpoolDirEvents.NEW_FILE.create(getContext(), batchContext)
              .with("filepath", currentFile.getAbsolutePath())
              .createAndSend();
          noMoreDataFileCount++;
          totalFiles++;
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

        if (MINUS_ONE.equals(offset)) {
          SpoolDirEvents.FINISHED_FILE.create(getContext(), batchContext)
              .with("filepath", currentFile.getAbsolutePath())
              .with("error-count", perFileErrorCount)
              .with("record-count", perFileRecordCount)
              .createAndSend();

          LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_READ);
          event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, currentFile.getAbsolutePath());
          event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.LOCAL_FS.name());
          event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, conf.filePattern);
          Map<String, String> props = new HashMap<>();
          props.put("Record Count", Long.toString(perFileRecordCount));
          event.setProperties(props);
          getContext().publishLineageEvent(event);
        }
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

    if (shouldSendNoMoreDataEvent) {
      LOG.info("sending no-more-data event.  records {} errors {} files {} ",
          noMoreDataRecordCount, noMoreDataErrorCount, noMoreDataFileCount
      );
      SpoolDirEvents.NO_MORE_DATA.create(getContext(), batchContext)
          .with("record-count", noMoreDataRecordCount)
          .with("error-count", noMoreDataErrorCount)
          .with("file-count", noMoreDataFileCount)
          .createAndSend();
      shouldSendNoMoreDataEvent = false;
      noMoreDataRecordCount = 0;
      noMoreDataErrorCount = 0;
      noMoreDataFileCount = 0;
    }


    //Process And Commit offsets
    getContext().processBatch(batchContext, Source.POLL_SOURCE_OFFSET_KEY, createSourceOffset(file, offset));

    return createSourceOffset(file, offset);
  }

  /**
   * Processes a batch from the specified file and offset up to a maximum batch size. If the file is fully processed
   * it must return -1, otherwise it must return the offset to continue from next invocation.
   */
  public String produce(File file, String offset, int maxBatchSize, BatchMaker batchMaker) throws StageException,
      BadSpoolFileException {
    String sourceFile = file.getName();
    try {
      if (parser == null) {
        switch (conf.dataFormat) {
          case AVRO:
            parser = parserFactory.getParser(file, offset);
            break;
          case WHOLE_FILE:
            FileRef localFileRef = new LocalFileRef.Builder()
                .filePath(file.getAbsolutePath())
                .bufferSize(conf.dataFormatConfig.wholeFileMaxObjectLen)
                .rateLimit(FileRefUtil.evaluateAndGetRateLimit(rateLimitElEval, rateLimitElVars, conf.dataFormatConfig.rateLimit))
                .createMetrics(true)
                .totalSizeInBytes(Files.size(file.toPath()))
                .build();
            parser = parserFactory.getParser(file.getName(), getFileMetadata(file), localFileRef);
            break;
          default:
            parser = parserFactory.getParser(file.getName(), new FileInputStream(file), offset);
        }
      }

      for (int i = 0; i < maxBatchSize; i++) {
        try {
          Record record;

          try {
            record = parser.parse();
          } catch(RecoverableDataParserException ex) {
            // Propagate partially parsed record to error stream
            record = ex.getUnparsedRecord();
            setHeaders(record, file, offset);

            errorRecordHandler.onError(new OnRecordErrorException(record, ex.getErrorCode(), ex.getParams()));
            perFileErrorCount++;
            noMoreDataErrorCount++;
            // We'll simply continue reading once this
            continue;
          }

          if (record != null) {
            setHeaders(record, file, offset);
            batchMaker.addRecord(record);
            offset = parser.getOffset();

            noMoreDataRecordCount++;
            perFileRecordCount++;

          } else {
            parser.close();
            parser = null;
            offset = MINUS_ONE;
            break;
          }
        } catch (ObjectLengthException ex) {
          String exOffset = offset;
          offset = MINUS_ONE;
          errorRecordHandler.onError(Errors.SPOOLDIR_02, sourceFile, exOffset, ex);
          perFileErrorCount++;
          noMoreDataErrorCount++;
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

  private void setHeaders(Record record, File file, String offset) {
    record.getHeader().setAttribute(HeaderAttributeConstants.FILE, file.getPath());
    record.getHeader().setAttribute(HeaderAttributeConstants.FILE_NAME, file.getName());
    record.getHeader().setAttribute(HeaderAttributeConstants.LAST_MODIFIED_TIME, String.valueOf(file.lastModified()));
    record.getHeader().setAttribute(HeaderAttributeConstants.OFFSET, offset == null ? "0" : offset);
    record.getHeader().setAttribute(BASE_DIR, conf.spoolDir);
  }

  @SuppressWarnings("unchecked")
  public static Map<String, Object> getFileMetadata(File file) throws IOException {
    boolean isPosix = file.toPath().getFileSystem().supportedFileAttributeViews().contains("posix");
    Map<String, Object>  metadata = new HashMap<>(Files.readAttributes(file.toPath(), isPosix? "posix:*" : "*"));
    metadata.put(HeaderAttributeConstants.FILE_NAME, file.getName());
    metadata.put(HeaderAttributeConstants.FILE, file.getPath());
    if (isPosix && metadata.containsKey(PERMISSIONS) && Set.class.isAssignableFrom(metadata.get(PERMISSIONS).getClass())) {
      Set<PosixFilePermission> posixFilePermissions = (Set<PosixFilePermission>)(metadata.get(PERMISSIONS));
      //converts permission to rwx- format and replace it in permissions field.
      // (totally containing 9 characters 3 for user 3 for group and 3 for others)
      metadata.put(PERMISSIONS, PosixFilePermissions.toString(posixFilePermissions));
    }
    return metadata;
  }

}
