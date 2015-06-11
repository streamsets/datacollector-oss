/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.csv;

import com.streamsets.pipeline.lib.io.OverrunException;
import com.streamsets.pipeline.lib.io.OverrunReader;
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
    this(new OverrunReader(reader, OverrunReader.getDefaultReadLimit(), false, false), format, initialPosition, maxObjectLen);
  }

  public OverrunCsvParser(OverrunReader reader, CSVFormat format, long initialPosition, int maxObjectLen)
      throws IOException {
    super(reader, format, maxObjectLen, initialPosition);
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
