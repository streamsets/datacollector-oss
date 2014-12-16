/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.source.spooldir.log;

import com.codahale.metrics.Counter;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.io.OverrunLineReader;
import com.streamsets.pipeline.lib.stage.source.spooldir.AbstractSpoolDirSource;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

@GenerateResourceBundle
@StageDef(version = "1.0.0",
    label = "Log spool directory",
    description = "Consumes log files from a spool directory")
public class LogSpoolDirSource extends AbstractSpoolDirSource {
  private final static Logger LOG = LoggerFactory.getLogger(LogSpoolDirSource.class);

  public static final String LINE = "line";
  public static final String TRUNCATED = "truncated";

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Maximum Log Line Length",
      description = "The maximum length for log lines, if a line exceeds that length, it will be truncated",
      defaultValue = "1024")
  public int maxLogLineLength;

  private StringBuilder line;
  private Counter linesOverMaxLengthCounter;

  @Override
  protected void init() throws StageException {
    super.init();
    line = new StringBuilder(maxLogLineLength);
    linesOverMaxLengthCounter = getContext().createCounter("linesOverMaxLen");
  }

  @Override
  protected long produce(File file, long offset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String sourceFile = file.getName();
    try (CountingReader reader = new CountingReader(new FileReader(file))) {
      IOUtils.skipFully(reader, offset);
      OverrunLineReader lineReader = new OverrunLineReader(reader, maxLogLineLength);
      return produce(sourceFile, offset, lineReader, maxBatchSize, batchMaker);
    } catch (IOException ex) {
      throw new StageException(null, ex.getMessage(), ex);
    }
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
        Record record = getContext().createRecord(Utils.format("file={} offset={}", sourceFile, offset));
        Map<String, Field> map = new LinkedHashMap<>();
        map.put(LINE, Field.create(line.toString()));
        map.put(TRUNCATED, Field.create(len > maxLogLineLength));
        record.set(Field.create(map));
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
