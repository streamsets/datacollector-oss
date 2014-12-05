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
package com.streamsets.pipeline.lib.csv;

import com.streamsets.pipeline.lib.io.CountingReader;
import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.util.ExceptionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;

public class OverrunCsvParser extends CsvParser {
  private final CountingReader countingReader;
  private boolean overrun;

  public OverrunCsvParser(Reader reader, CSVFormat format) throws IOException {
    this(reader, format, 0);
  }

  public OverrunCsvParser(Reader reader, CSVFormat format, long initialPosition) throws IOException {
    super(new OverrunReader(reader, OverrunReader.getDefaultReadLimit()), format, initialPosition);
    countingReader = (CountingReader) getReader();
  }

  @Override
  protected String[] readHeader() throws IOException {
    try {
      return super.readHeader();
    } catch (RuntimeException ex) {
      OverrunException oex = ExceptionUtils.findSpecificCause(ex, OverrunException.class);
      if (oex != null) {
        overrun = true;
        throw oex;
      }
      throw ex;
    }
  }

  @Override
  protected CSVRecord nextRecord() throws IOException {
    countingReader.resetCount();
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
