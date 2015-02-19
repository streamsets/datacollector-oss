/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.spooldir;

import com.codahale.metrics.Counter;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.io.OverrunLineReader;
import com.streamsets.pipeline.lib.util.LineToRecord;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class LogDataProducer implements DataProducer {
  private final static Logger LOG = LoggerFactory.getLogger(LogDataProducer.class);

  private final Source.Context context;
  private final int maxLogLineLength;
  private final StringBuilder line;
  private final Counter linesOverMaxLengthCounter;
  private final LineToRecord lineToRecord;
  private OverrunLineReader lineReader;

  public LogDataProducer(Source.Context context, int maxLogLineLength, boolean setTruncated) {
    this.context = context;
    this.maxLogLineLength = maxLogLineLength;
    line = new StringBuilder(maxLogLineLength);
    linesOverMaxLengthCounter = context.createCounter("linesOverMaxLen");
    lineToRecord = new LineToRecord(setTruncated);
  }

  @Override
  public long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String sourceFile = file.getName();
    CountingReader reader = null;
    try {
      if (lineReader == null) {
        reader = new CountingReader(new FileReader(file));
        IOUtils.skipFully(reader, offset);
        lineReader = new OverrunLineReader(reader, maxLogLineLength);
        reader = null;
      }
      offset = produce(sourceFile, offset, lineReader, maxBatchSize, batchMaker);
    } catch (IOException ex) {
      long lastOffset = offset;
      offset = -1;
      throw new StageException(Errors.SPOOLDIR_03, file, lastOffset, ex.getMessage(), ex);
    } finally {
      if (offset == -1) {
        if (lineReader != null) {
          try {
            lineReader.close();
          } catch (IOException ex) {
            //NOP
          }
          lineReader = null;
        }
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ex) {
            //NOP
          }
        }
      }
    }
    return offset;
  }

  protected long produce(String sourceFile, long offset, OverrunLineReader lineReader, int maxBatchSize,
      BatchMaker batchMaker) throws IOException {
    for (int i = 0; i < maxBatchSize; i++) {
      line.setLength(0);
      int len = lineReader.readLine(line);
      if (len > maxLogLineLength) {
        linesOverMaxLengthCounter.inc();
        LOG.warn("Log line exceeds maximum length '{}', log file '{}', line starts at offset '{}'", maxLogLineLength,
                 sourceFile, offset);
      }
      if (len > -1) {
        Record record = lineToRecord.createRecord(context, sourceFile, offset, line.toString(), len > maxLogLineLength);
        batchMaker.addRecord(record);
        offset = lineReader.getCount();
      } else {
        offset = -1;
        break;
      }
    }
    return offset;
  }

}
