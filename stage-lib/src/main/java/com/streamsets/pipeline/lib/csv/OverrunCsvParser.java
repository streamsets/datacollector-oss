/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
