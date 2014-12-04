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
import com.streamsets.pipeline.lib.util.StageLibError;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@GenerateResourceBundle
@StageDef(version="1.0.1",
          label="Tail log files")
public class LogTailSource extends BaseSource {

  private static final int SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS = 100;

  private static final String OFFSET = "tailing";

  @ConfigDef(required = true,
             type = ConfigDef.Type.STRING,
             label = "Log file")
  public String logFileName;

  @ConfigDef(required = false,
             type = ConfigDef.Type.BOOLEAN,
             label = "Tail from end of log file",
             defaultValue = "true")
  public boolean tailFromEnd;

  @ConfigDef(required = false,
             type = ConfigDef.Type.INTEGER,
             label = "Maximum lines prefetch",
             defaultValue = "100")
  public int maxLinesPrefetch;

  @ConfigDef(required = false,
             type = ConfigDef.Type.INTEGER,
             label = "Batch size",
             defaultValue = "10")
  public int batchSize;

  @ConfigDef(required = false,
             type = ConfigDef.Type.INTEGER,
             label = "Maximum wait time to fill up a batch",
             defaultValue = "5000")
  public int maxWaitTime;

  private BlockingQueue<String> logLinesQueue;
  private LogTail logTail;

  @Override
  protected void init() throws StageException {
    super.init();
    File logFile = new File(logFileName);
    if (logFile.exists() && !logFile.canRead()) {
      throw new StageException(StageLibError.LIB_0002, logFile);
    }
    logLinesQueue = new ArrayBlockingQueue<String>(maxLinesPrefetch);
    logTail = new LogTail(logFile, tailFromEnd, getInfo(), logLinesQueue);
    logTail.start();
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
    List<String> lines = new ArrayList<String>(fetch);
    while (((System.currentTimeMillis() - start) < maxWaitTime) && (logLinesQueue.size() < fetch)) {
      try {
        Thread.sleep(SLEEP_TIME_WAITING_FOR_BATCH_SIZE_MS);
      } catch (InterruptedException ex) {
        break;
      }
    }
    logLinesQueue.drainTo(lines, fetch);
    for (int i = 0; i < lines.size(); i++) {
      Record record = getContext().createRecord(logFileName + now + i);
      record.set(Field.create(lines.get(i)));
      batchMaker.addRecord(record);
    }
    return OFFSET;
  }

}
