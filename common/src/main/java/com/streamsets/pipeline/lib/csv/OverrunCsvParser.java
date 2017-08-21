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
package com.streamsets.pipeline.lib.csv;

import com.streamsets.pipeline.api.ext.io.OverrunException;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.lib.util.ExceptionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;

public class OverrunCsvParser extends CsvParser {
  private boolean overrun;

  public OverrunCsvParser(Reader reader, CSVFormat format, int maxObjectLen) throws IOException {
    this(reader, format, 0, maxObjectLen);
  }

  public OverrunCsvParser(Reader reader, CSVFormat format, long initialPosition, int maxObjectLen) throws IOException {
    this(
        new OverrunReader(reader, OverrunReader.getDefaultReadLimit(), false, false),
        format,
        initialPosition,
        0,
        maxObjectLen
    );
  }

  public OverrunCsvParser(
      OverrunReader reader,
      CSVFormat format,
      long initialPosition,
      int skipStartLines,
      int maxObjectLen
  ) throws IOException {
    super(reader, format, maxObjectLen, initialPosition, skipStartLines);
    OverrunReader countingReader = (OverrunReader) getReader();
    countingReader.setEnabled(true);
  }

  @Override
  protected CSVRecord nextRecord() throws IOException {
    if (overrun) {
      throw new IOException("The parser is unusable, the underlying reader had an overrun");
    }
    ((OverrunReader)getReader()).resetCount();
    return super.nextRecord();
  }

  @Override
  public String[] read() throws IOException {
    try {
      return super.read();
    } catch (RuntimeException ex) {
      OverrunException oex = ExceptionUtils.findSpecificCause(ex, OverrunException.class);
      if (oex != null) {
        overrun = true;
        throw oex;
      }
      throw ex;
    }
  }

}
