/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.lib.stage.source.spooldir.log;

import com.codahale.metrics.Counter;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.container.Utils;
import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.io.OverrunLineReader;
import com.streamsets.pipeline.lib.stage.source.spooldir.AbstractSpoolDirSource;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

@GenerateResourceBundle
@StageDef(version = "1.0.0",
    label = "Log spool directory",
    description = "Consumes log files from a spool directory")
public class LogSpoolDirSource extends AbstractSpoolDirSource {
  private final static Logger LOG = LoggerFactory.getLogger(LogSpoolDirSource.class);

  @ConfigDef(required = true,
      type = ConfigDef.Type.INTEGER,
      label = "Maximum Log Line Length",
      description = "The maximum length for log lines, if a line exceeds that length, it will be trimmed",
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
        record.set(Field.create(line.toString()));
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
