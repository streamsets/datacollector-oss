/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.logtail;

import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.lib.util.LineToRecord;
import com.streamsets.pipeline.lib.util.StageLibError;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@GenerateResourceBundle
@StageDef(version="1.0.0",
          label="File Tail",
          description = "Reads lines from the specified file as they are written to it. It must be text file, " +
                        "typically a log file.",
          icon="fileTail.png")
public class FileTailSource extends BaseSource {

  private static final int SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS = 100;

  private static final String OFFSET = "tailing";

  @ConfigDef(required = true,
             type = ConfigDef.Type.STRING,
             label = "File Path",
             description = "Full file path of the file to tail",
             displayPosition = 0)
  public String fileName;

  @ConfigDef(required = true,
             type = ConfigDef.Type.INTEGER,
             label = "Maximum Lines per Batch",
             description = "The maximum number of file lines that will be sent in a single batch",
             defaultValue = "10",
             displayPosition = 10)
  public int batchSize;

  @ConfigDef(required = true,
             type = ConfigDef.Type.INTEGER,
             label = "Batch Wait Time (secs)",
             description = " Maximum amount of time to wait to fill a batch before sending it",
             defaultValue = "5",
             displayPosition = 20)
  public int maxWaitTimeSecs;

  private BlockingQueue<String> logLinesQueue;
  private long maxWaitTimeMillis;
  private LogTail logTail;
  private LineToRecord lineToRecord;

  @Override
  protected void init() throws StageException {
    super.init();
    File logFile = new File(fileName);
    if (logFile.exists() && !logFile.canRead()) {
      throw new StageException(StageLibError.LIB_0001, logFile);
    }
    maxWaitTimeMillis = maxWaitTimeSecs * 1000;
    logLinesQueue = new ArrayBlockingQueue<>(2 * batchSize);
    logTail = new LogTail(logFile, true, getInfo(), logLinesQueue);
    logTail.start();
    lineToRecord = new LineToRecord();
  }

  @Override
  public void destroy() {
    logTail.stop();
    super.destroy();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    long start = System.currentTimeMillis();
    int fetch = Math.min(batchSize, maxBatchSize);
    String now = "." + Long.toString(System.currentTimeMillis()) + ".";
    List<String> lines = new ArrayList<>(fetch);
    while (((System.currentTimeMillis() - start) < maxWaitTimeMillis) && (logLinesQueue.size() < fetch)) {
      try {
        Thread.sleep(SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS);
      } catch (InterruptedException ex) {
        break;
      }
    }
    logLinesQueue.drainTo(lines, fetch);
    for (int i = 0; i < lines.size(); i++) {
      Record record = lineToRecord.createRecord(getContext(), fileName, -1, lines.get(i), false);
      batchMaker.addRecord(record);
    }
    return OFFSET;
  }

}
