/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.csv;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.CountingReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

public class CsvParser implements Closeable, AutoCloseable {
  private long currentPos;
  private final CSVParser parser;
  private final CountingReader reader;
  private Iterator<CSVRecord> iterator;
  private CSVRecord nextRecord;
  private final String[] headers;
  private boolean closed;

  public CsvParser(Reader reader, CSVFormat format) throws IOException {
    this(new CountingReader(reader), format, 0);
  }

  @SuppressWarnings("unchecked")
  public CsvParser(CountingReader reader, CSVFormat format, long initialPosition) throws IOException {
    Utils.checkNotNull(reader, "reader");
    Utils.checkNotNull(reader.getPos() == 0,
                       "reader must be in position zero, the CsvParser will fastforward to the initialPosition");
    Utils.checkNotNull(format, "format");
    Utils.checkArgument(initialPosition >= 0, "initialPosition must be greater or equal than zero");
    this.reader = reader;
    currentPos = initialPosition;
    if (initialPosition == 0) {
      if (format.getSkipHeaderRecord()) {
        format = format.withSkipHeaderRecord(false);
        parser = new CSVParser(reader, format, 0, 0);
        headers = read();
      } else {
        parser = new CSVParser(reader, format, 0, 0);
        headers = null;
      }
    } else {
      if (format.getSkipHeaderRecord()) {
        format = format.withSkipHeaderRecord(false);
        parser = new CSVParser(reader, format, 0, 0);
        headers = read();
        while (getReaderPosition() < initialPosition && read() != null) {
        }
        if (getReaderPosition() != initialPosition) {
          throw new IOException(Utils.format("Could not position reader at position '{}', got '{}' instead",
                                             initialPosition, getReaderPosition()));
        }
      } else {
        IOUtils.skipFully(reader, initialPosition);
        parser = new CSVParser(reader, format, initialPosition, 0);
        headers = null;
      }
    }
  }

  protected Reader getReader() {
    return reader;
  }

  protected CSVRecord nextRecord() throws IOException {
    return (iterator.hasNext()) ? iterator.next() : null;
  }

  public String[] getHeaders() throws IOException {
    return headers;
  }

  public long getReaderPosition() {
    return currentPos;
  }

  public String[] read() throws IOException {
    if (closed) {
      throw new IOException("Parser has been closed");
    }
    if (iterator == null) {
      iterator = parser.iterator();
      nextRecord = nextRecord();
    }
    CSVRecord record = nextRecord;
    if (nextRecord != null) {
      nextRecord = nextRecord();
    }
    currentPos = (nextRecord != null) ? nextRecord.getCharacterPosition() : reader.getPos();
    return toArray(record);
  }

  private String[] toArray(CSVRecord record) {
    String[] array = (record == null) ? null : new String[record.size()];
    if (array != null) {
      for (int i = 0; i < record.size(); i++) {
        array[i] = record.get(i);
      }
    }
    return array;
  }

  @Override
  public void close() {
    try {
      closed = true;
      parser.close();
    } catch (IOException ex) {
      //NOP
    }
  }
}
