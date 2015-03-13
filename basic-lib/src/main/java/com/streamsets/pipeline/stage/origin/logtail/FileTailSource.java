/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.logtail;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.OffsetCommitter;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.parser.CharDataParserFactory;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FileTailSource extends BaseSource implements OffsetCommitter {
  private static final int SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS = 100;

  private final DataFormat dataFormat;
  private final String fileName;
  private final int batchSize;
  private final int maxWaitTimeSecs;


  public FileTailSource(DataFormat dataFormat, String fileName, int batchSize, int maxWaitTimeSecs) {
    this.dataFormat = dataFormat;
    this.fileName = fileName;
    this.batchSize = batchSize;
    this.maxWaitTimeSecs = maxWaitTimeSecs;
  }

  private BlockingQueue<String> logLinesQueue;
  private long maxWaitTimeMillis;
  private LogTail logTail;
  private CharDataParserFactory parserFactory;

  private String fileOffset;
  private long recordCount;

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues = super.validateConfigs();
    File logFile = new File(fileName);
    if (!logFile.exists()) {
      try {
        // waiting for a second in case the log is in the middle of a file rotation and the file does not exist
        // at this very moment.
        Thread.sleep(1000);
      } catch (InterruptedException ex) {
        //NOP
      }
      if (!logFile.exists()) {
        issues.add(getContext().createConfigIssue(Groups.FILE.name(), "fileName", Errors.TAIL_00, logFile));
      }
    }
    if (logFile.exists() && !logFile.canRead()) {
      issues.add(getContext().createConfigIssue(Groups.FILE.name(), "fileName", Errors.TAIL_01, logFile));
    }
    if (logFile.exists() && !logFile.isFile()) {
      issues.add(getContext().createConfigIssue(Groups.FILE.name(), "fileName", Errors.TAIL_03, logFile));
    }
    switch (dataFormat) {
      case TEXT:
      case JSON:
      case LOG:
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.FILE.name(), "dataFormat", Errors.TAIL_02, dataFormat,
                                                  Arrays.asList(DataFormat.TEXT, DataFormat.JSON)));
    }
    return issues;
  }

  @Override
  protected void init() throws StageException {
    super.init();
    File logFile = new File(fileName);
    maxWaitTimeMillis = maxWaitTimeSecs * 1000;
    logLinesQueue = new ArrayBlockingQueue<>(2 * batchSize);
    logTail = new LogTail(logFile, true, getInfo(), logLinesQueue);
    logTail.start();
    switch (dataFormat) {
      case TEXT:
        parserFactory = new CharDataParserFactory.Builder(getContext(), CharDataParserFactory.Format.TEXT)
            .setMaxDataLen(-1).build();
        break;
      case JSON:
        parserFactory = new CharDataParserFactory.Builder(getContext(), CharDataParserFactory.Format.JSON)
            .setMode(JsonMode.MULTIPLE_OBJECTS).setMaxDataLen(-1).build();
        break;
      case LOG:
        //TODO: Add multiline support potentially
        parserFactory = new CharDataParserFactory.Builder(getContext(), CharDataParserFactory.Format.LOG)
            .setMaxDataLen(-1).build();
        break;
      default:
        throw new StageException(Errors.TAIL_02, "dataFormat", dataFormat);
    }
    fileOffset = logFile.getName() + "::" + System.currentTimeMillis();
    recordCount = 0;
  }

  @Override
  public void destroy() {
    logTail.stop();
    super.destroy();
  }

  String getFileOffset() {
    return fileOffset;
  }

  long getRecordCount() {
    return recordCount;
  }

  private String getOffset() {
    return getFileOffset() + "::" + getRecordCount();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    long start = System.currentTimeMillis();
    int fetch = Math.min(batchSize, maxBatchSize);
    List<String> lines = new ArrayList<>(fetch);
    while (((System.currentTimeMillis() - start) < maxWaitTimeMillis) && (logLinesQueue.size() < fetch)) {
      try {
        Thread.sleep(SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS);
      } catch (InterruptedException ex) {
        break;
      }
    }
    logLinesQueue.drainTo(lines, fetch);
    for (String line : lines) {
      String sourceId = getOffset();
      try (DataParser parser = parserFactory.getParser(sourceId, line)) {
        Record record = parser.parse();
        if (record != null) {
          batchMaker.addRecord(record);
          recordCount++;
        }
      } catch (IOException|DataParserException ex) {
        switch (getContext().getOnErrorRecord()) {
          case DISCARD:
            break;
          case TO_ERROR:
            getContext().reportError(Errors.TAIL_04, sourceId, ex.getMessage(), ex);
            break;
          case STOP_PIPELINE:
            if (ex instanceof StageException) {
              throw (StageException) ex;
            } else {
              throw new StageException(Errors.TAIL_04, sourceId, ex.getMessage(), ex);
            }
          default:
            throw new IllegalStateException(Utils.format("It should never happen. OnError '{}'",
                                                         getContext().getOnErrorRecord(), ex));
        }

      }
    }
    return getOffset();
  }

  @Override
  public void commit(String offset) throws StageException {
    //NOP
  }

}
