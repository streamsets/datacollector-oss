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
package com.streamsets.pipeline.lib.dirspooler;

import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.BatchContext;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
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
import com.streamsets.pipeline.lib.io.fileref.FileRefUtil;
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

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class SpoolDirRunnable implements Runnable {
  public static final String SPOOL_DIR_METRICS = "Spool Directory Metrics for Thread - ";
  public static final String CURRENT_FILE = "Current File";
  public static final String SPOOL_DIR_THREAD_PREFIX = "Spool Directory Runner - ";
  public static final String PERMISSIONS = "permissions";

  private final static Logger LOG = LoggerFactory.getLogger(SpoolDirRunnable.class);
  private static final String THREAD_NAME = "Thread Name";
  private static final String STATUS = "Status";
  private static final String OFFSET = "Current Offset";
  private static final String MINUS_ONE = "-1";
  private static final String ZERO = "0";
  private static final String BASE_DIR = "baseDir";

  public static final String FILE_SEPARATOR = System.getProperty("file.separator");

  private final PushSource.Context context;
  private final int threadNumber;
  private final int batchSize;
  private final Map<String, Offset> offsets;
  private final String lastSourceFileName;
  private final DirectorySpooler spooler;
  private final Map<String, Object> gaugeMap;
  private final boolean useLastModified;
  private final WrappedFileSystem fs;

  private DataParser parser;
  private SpoolDirConfigBean conf;
  private DataParserFactory parserFactory;

  private ELEval rateLimitElEval;
  private ELVars rateLimitElVars;

  private boolean shouldSendNoMoreDataEvent;
  private long noMoreDataRecordCount;
  private long noMoreDataErrorCount;
  private long noMoreDataFileCount;

  private long perFileRecordCount;
  private long perFileErrorCount;
  private long totalFiles;

  private ErrorRecordHandler errorRecordHandler;

  private WrappedFile currentFile;

  public SpoolDirRunnable(
      PushSource.Context context,
      int threadNumber,
      int batchSize,
      Map<String, Offset> offsets,
      String lastSourcFileName,
      DirectorySpooler spooler,
      SpoolDirConfigBean conf,
      WrappedFileSystem fs
  ) {
    this.context = context;
    this.threadNumber = threadNumber;
    this.batchSize = batchSize;
    this.offsets = offsets;
    this.lastSourceFileName = lastSourcFileName;
    this.spooler = spooler;
    this.conf = conf;
    this.parserFactory = conf.dataFormatConfig.getParserFactory();
    this.shouldSendNoMoreDataEvent = false;
    this.rateLimitElEval = FileRefUtil.createElEvalForRateLimit(context);
    this.rateLimitElVars = context.createELVars();
    this.useLastModified = conf.useLastModified == FileOrdering.TIMESTAMP;
    this.fs = fs;

    // Metrics
    this.gaugeMap = context.createGauge(SPOOL_DIR_METRICS + threadNumber).getValue();
  }

  @Override
  public void run() {
    // get the file
    Thread.currentThread().setName(SPOOL_DIR_THREAD_PREFIX + threadNumber);
    initGaugeIfNeeded();
    Offset offset = offsets.get(lastSourceFileName);

    while (!context.isStopped()) {
      BatchContext batchContext = context.startBatch();
      this.errorRecordHandler = new DefaultErrorRecordHandler(context, batchContext);
      try {
        offset = produce(offset, batchContext);
      } catch (StageException ex) {
        handleStageError(ex.getErrorCode(), ex);
      }
    }

    IOUtils.closeQuietly(parser);
  }

  private Offset produce(Offset lastSourceOffset, BatchContext batchContext) throws StageException {

    // if lastSourceOffset is NULL (beginning of source) it returns NULL
    String file = lastSourceOffset.getRawFile();
    String lastSourceFile = file;
    String fullPath = (file != null) ? spooler.getSpoolDir() + FILE_SEPARATOR + file : null;
    // if lastSourceOffset is NULL (beginning of source) it returns 0
    String offset = lastSourceOffset.getOffset();

    try {
      if (hasToFetchNextFileFromSpooler(file, offset)) {
        updateGauge(Status.SPOOLING, null);
        currentFile = null;
        try {
          WrappedFile nextAvailFile = null;
          do {
            if (nextAvailFile != null) {
              LOG.warn(
                  "Ignoring file '{}' in spool directory as is lesser than offset file '{}'",
                  nextAvailFile.getAbsolutePath(),
                  fullPath
              );
            }
            nextAvailFile = spooler.poolForFile(conf.poolingTimeoutSecs, TimeUnit.SECONDS);
          } while (!isFileFromSpoolerEligible(nextAvailFile, fullPath, offset));

          if (nextAvailFile == null) {
            // no file to process
            LOG.debug(
                "No new file available in spool directory after '{}' secs, producing empty batch",
                conf.poolingTimeoutSecs
            );

            // no-more-data event needs to be sent.
            shouldSendNoMoreDataEvent = true;

          } else {
            // since we have data to process, don't trigger the no-more-data event.
            shouldSendNoMoreDataEvent = false;

            // file to process
            currentFile = nextAvailFile;

            // if the current offset file is null or the file returned by the dirspooler is greater than the current offset
            // file we take the file returned by the dirspooler as the new file and set the offset to zero
            // if not, it means the dirspooler returned us the current file, we just keep processing it from the last
            // offset we processed (known via offset tracking)
            boolean pickFileFromSpooler = false;
            if (file == null) {
              pickFileFromSpooler = true;
            } else if (useLastModified) {
              WrappedFile fileObject = fs.getFile(spooler.getSpoolDir(), file);
              //try {
                if (SpoolDirUtil.compareFiles(fs, nextAvailFile, fileObject)) {
                  pickFileFromSpooler = true;
                }
              /*} catch (IOException ex) {

              }*/
            } else if (nextAvailFile.getFileName().compareTo(file) > 0) {
              pickFileFromSpooler = true;
            }

            if (pickFileFromSpooler) {
              file = currentFile.toString().replaceFirst(spooler.getSpoolDir() + FILE_SEPARATOR, "");
              if (offsets.containsKey(file)) {
                offset = offsets.get(file).getOffset();
              } else {
                offset = ZERO;
              }
            }
          }

          if (currentFile != null && !offset.equals(MINUS_ONE)) {
            perFileRecordCount = 0;
            perFileErrorCount = 0;
            SpoolDirEvents.NEW_FILE.create(context, batchContext).with("filepath", currentFile.getAbsolutePath()).createAndSend();
            noMoreDataFileCount++;
            totalFiles++;
          }

        } catch (InterruptedException ex) {
          // the dirspooler was interrupted while waiting for a file, we log and return, the pipeline agent will invoke us
          // again to wait for a file again
          LOG.warn("Pooling interrupted");
          Thread.currentThread().interrupt();
        }
      }
    } catch (IOException ex) {
      LOG.error(ex.toString(), ex);
    }

    if (currentFile != null && !offset.equals(MINUS_ONE)) {
      // we have a file to process (from before or new from dirspooler)
      try {
        updateGauge(Status.READING, offset);

        // we ask for a batch from the currentFile starting at offset
        offset = generateBatch(currentFile, offset, batchSize, batchContext.getBatchMaker());

        if (MINUS_ONE.equals(offset)) {
          SpoolDirEvents.FINISHED_FILE.create(context, batchContext)
              .with("filepath", currentFile.getAbsolutePath())
              .with("error-count", perFileErrorCount)
              .with("record-count", perFileRecordCount)
              .createAndSend();

          LineageEvent event = context.createLineageEvent(LineageEventType.ENTITY_READ);
          event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, currentFile.getAbsolutePath());
          event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.LOCAL_FS.name());
          event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, conf.filePattern);
          Map<String, String> props = new HashMap<>();
          props.put("Record Count", Long.toString(perFileRecordCount));
          event.setProperties(props);
          context.publishLineageEvent(event);
        }
      } catch (BadSpoolFileException ex) {
        LOG.error(Errors.SPOOLDIR_01.getMessage(), ex.getFile(), ex.getPos(), ex.toString(), ex);
        context.reportError(Errors.SPOOLDIR_01, ex.getFile(), ex.getPos(), ex.toString(), ex);

        try {
          // then we ask the dirspooler to error handle the failed file
          spooler.handleCurrentFileAsError();

        } catch (IOException ex1) {
          throw new StageException(Errors.SPOOLDIR_00, currentFile, ex1.toString(), ex1);
        }
        // we set the offset to -1 to indicate we are done with the file and we should fetch a new one from the dirspooler
        offset = MINUS_ONE;
      }
    }

    if (shouldSendNoMoreDataEvent) {
      LOG.info("sending no-more-data event.  records {} errors {} files {} ",
          noMoreDataRecordCount, noMoreDataErrorCount, noMoreDataFileCount
      );
      SpoolDirEvents.NO_MORE_DATA.create(context, batchContext)
          .with("record-count", noMoreDataRecordCount)
          .with("error-count", noMoreDataErrorCount)
          .with("file-count", noMoreDataFileCount)
          .createAndSend();
      shouldSendNoMoreDataEvent = false;
      noMoreDataRecordCount = 0;
      noMoreDataErrorCount = 0;
      noMoreDataFileCount = 0;
    }

    Offset newOffset = new Offset(Offset.VERSION_ONE, file, offset);

    // Process And Commit offsets
    context.processBatch(batchContext, newOffset.getFile(), newOffset.getOffsetString());

    if (lastSourceFile != null && !lastSourceFile.equals(newOffset.getFile())) {
      context.commitOffset(lastSourceFile, null);
    }

    // if this is the end of the file, do post processing
    if (currentFile != null && newOffset.getOffset().equals(MINUS_ONE)) {
      spooler.doPostProcessing(fs.getFile(conf.spoolDir, newOffset.getFile()));
    }

    updateGauge(Status.BATCH_GENERATED, offset);

    return newOffset;
  }

  /**
   * Processes a batch from the specified file and offset up to a maximum batch size. If the file is fully processed
   * it must return -1, otherwise it must return the offset to continue from next invocation.
   */
  public String generateBatch(WrappedFile file, String offset, int maxBatchSize, BatchMaker batchMaker) throws
      StageException,
      BadSpoolFileException {
    if (offset == null) {
      offset = "0";
    }
    String sourceFile = file.getFileName();
    try {
      if (parser == null) {
        parser = SpoolDirUtil.getParser(
            fs,
            file,
            conf.dataFormat,
            parserFactory,
            offset,
            conf.dataFormatConfig.wholeFileMaxObjectLen,
            rateLimitElEval,
            rateLimitElVars,
            conf.dataFormatConfig.rateLimit
        );
      }

      Map<String, Object> recordHeaderAttr = generateHeaderAttrs(file);

      for (int i = 0; i < maxBatchSize; i++) {
        try {
          Record record;

          try {
            record = parser.parse();
          } catch(RecoverableDataParserException ex) {
            // Propagate partially parsed record to error stream
            record = ex.getUnparsedRecord();
            recordHeaderAttr.put(HeaderAttributeConstants.OFFSET, offset);
            setHeaders(record, recordHeaderAttr);

            errorRecordHandler.onError(new OnRecordErrorException(record, ex.getErrorCode(), ex.getParams()));
            perFileErrorCount++;
            noMoreDataErrorCount++;
            // We'll simply continue reading once this
            continue;
          }

          if (record != null) {
            recordHeaderAttr.put(HeaderAttributeConstants.OFFSET, offset);
            setHeaders(record, recordHeaderAttr);
            batchMaker.addRecord(record);
            offset = parser.getOffset();
            if (offset == null) {
              offset = "0";
            }
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
          offset = (parser != null) ? parser.getOffset() : MINUS_ONE;
          if (offset == null) {
            offset = "0";
          }
          errorRecordHandler.onError(Errors.SPOOLDIR_02, sourceFile, exOffset, ex);
          perFileErrorCount++;
          noMoreDataErrorCount++;
        }
      }
    } catch (IOException |DataParserException ex) {
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
        switch (context.getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            // we failed to produce a record, which leaves the input file in an unknown state. all we can do here is
            // throw an exception.
            throw new BadSpoolFileException(file.getAbsolutePath(), exOffset, ex);
          case STOP_PIPELINE:
            context.reportError(Errors.SPOOLDIR_04, sourceFile, exOffset, ex.toString(), ex);
            throw new StageException(Errors.SPOOLDIR_04, sourceFile, exOffset, ex.toString());
          default:
            throw new IllegalStateException(Utils.format("Unknown OnError value '{}'",
                context.getOnErrorRecord(), ex));
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

  private boolean hasToFetchNextFileFromSpooler(String file, String offset) throws IOException {
    return
        // we don't have a current file half way processed in the current agent execution
        currentFile == null ||
            // we don't have a file half way processed from a previous agent execution via offset tracking
            file == null ||
            (useLastModified && SpoolDirUtil.compareFiles(fs, fs.getFile(spooler.getSpoolDir(), file), currentFile)) ||
            // the current file is lexicographically lesser than the one reported via offset tracking
            // this can happen if somebody drop
            currentFile.getFileName().compareTo(file) < 0 ||
            // the current file has been fully processed
            MINUS_ONE.equals(offset);
  }

  private boolean isFileFromSpoolerEligible(WrappedFile spoolerFile, String offsetFile, String offsetInFile) {
    if (spoolerFile == null) {
      // there is no new file from dirspooler, we return yes to break the loop
      return true;
    }
    if (offsetFile == null) {
      // file reported by offset tracking is NULL, means we are starting from zero
      return true;
    }

    String fileName = spoolerFile.toString().replaceFirst(spooler.getSpoolDir() + FILE_SEPARATOR, "");
    if (offsets.containsKey(fileName)) {
      offsetInFile = offsets.get(fileName).getOffset();
      if (offsetInFile.equals(MINUS_ONE)) {
        return false;
      }
    } else {
      // check is newer then any other files in the offset
      for (String offsetFileName : offsets.keySet()) {
        if (useLastModified) {
          if (fs.exists(fs.getFile(spooler.getSpoolDir(), offsetFileName)) &&
              SpoolDirUtil.compareFiles(
                  fs,
                  fs.getFile(spooler.getSpoolDir(), offsetFileName),
                  spoolerFile
              )) {
            LOG.debug("File '{}' is less then offset file {}, ignoring", fileName, offsetFileName);
            return false;
          }
        } else {
          if (!offsetFileName.equals(Offset.NULL_FILE) && offsetFileName.compareTo(fileName) > 0) {
            LOG.debug("File '{}' is less then offset file {}, ignoring", fileName, offsetFileName);
            return false;
          }
        }
      }
    }

    if (spoolerFile.getAbsolutePath().compareTo(offsetFile) == 0 && !MINUS_ONE.equals(offsetInFile)) {
      // file reported by dirspooler is equal than current offset file
      // and we didn't fully process (not -1) the current file
      return true;
    }
    if (useLastModified) {
      if (SpoolDirUtil.compareFiles(fs, spoolerFile, fs.getFile(offsetFile))) {
        return true;
      }
    } else {
      if (spoolerFile.getAbsolutePath().compareTo(offsetFile) > 0) {
        // file reported by dirspooler is newer than current offset file
        return true;
      }
    }
    return false;
  }

  private Map<String, Object> generateHeaderAttrs(WrappedFile file) throws IOException {
    Map<String, Object> recordHeaderAttr = new HashMap<>();
    recordHeaderAttr.put(HeaderAttributeConstants.FILE, file.getAbsolutePath());
    recordHeaderAttr.put(HeaderAttributeConstants.FILE_NAME, file.getFileName());
    recordHeaderAttr.put(HeaderAttributeConstants.LAST_MODIFIED_TIME, String.valueOf(fs.getLastModifiedTime(file)));
    recordHeaderAttr.put(BASE_DIR, conf.spoolDir);
    return recordHeaderAttr;
  }

  private void setHeaders(Record record, Map<String, Object> recordHeaderAttr) throws IOException {
    recordHeaderAttr.forEach((k,v) -> record.getHeader().setAttribute(k, v.toString()));
  }

  private enum Status {
    SPOOLING,
    READING,
    GENERATING_BATCH,
    BATCH_GENERATED,
    ;
  }

  /**
   * Initialize the gauge with needed information
   */
  private void initGaugeIfNeeded() {
    gaugeMap.put(THREAD_NAME, Thread.currentThread().getName());
    gaugeMap.put(STATUS, "");
    gaugeMap.put(CURRENT_FILE, "");
  }

  private void updateGauge(Status status, String offset) {
    gaugeMap.put(STATUS, status.name());
    gaugeMap.put(
        CURRENT_FILE,
        currentFile == null ? "" : currentFile.getFileName()
    );
    gaugeMap.put(
        OFFSET,
        offset == null ? "" : offset
    );
  }

  /**
   * Handle Exception
   */
  private void handleStageError(ErrorCode errorCode, Exception e) {
    final String errorMessage = "Failure Happened";
    LOG.error(errorMessage, e);
    try {
      errorRecordHandler.onError(errorCode, e);
    } catch (StageException se) {
      LOG.error("Error when routing to stage error", se);
      //Way to throw stage exception from runnable to main source thread
      Throwables.propagate(se);
    }
  }
}
