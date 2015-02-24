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
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.util.JsonLineToRecord;
import com.streamsets.pipeline.lib.util.LineToRecord;
import com.streamsets.pipeline.lib.util.ToRecord;

import java.io.File;
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
  private ToRecord lineToRecord;

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
        issues.add(getContext().createConfigIssue(Errors.TAIL_00, Groups.FILE.name(), fileName, logFile));
      }
    }
    if (logFile.exists() && !logFile.canRead()) {
      issues.add(getContext().createConfigIssue(Errors.TAIL_01, Groups.FILE.name(), fileName, logFile));
    }
    if (logFile.exists() && !logFile.isFile()) {
      issues.add(getContext().createConfigIssue(Errors.TAIL_03, Groups.FILE.name(), fileName, logFile));
    }
    switch (dataFormat) {
      case TEXT:
      case JSON:
        break;
      default:
        issues.add(getContext().createConfigIssue(Errors.TAIL_02, Groups.FILE.name(), "dataFormat", dataFormat,
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
        lineToRecord = new LineToRecord(false);
        break;
      case JSON:
        lineToRecord = new JsonLineToRecord();
        break;
      default:
        throw new StageException(Errors.TAIL_02, "dataFormat", dataFormat);
    }
    fileOffset = String.format("%s::%d", fileName, System.currentTimeMillis());
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
      Record record = lineToRecord.createRecord(getContext(), getFileOffset(), getRecordCount(), line, false);
      batchMaker.addRecord(record);
      recordCount++;
    }
    return getFileOffset() + "::" + getRecordCount();
  }

  @Override
  public void commit(String offset) throws StageException {
    //NOP
  }

}
