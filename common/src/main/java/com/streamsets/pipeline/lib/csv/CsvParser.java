/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.csv;

import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CsvParser implements Closeable, AutoCloseable {

  private final long initialPosition;
  private long currentPos;
  private final CSVParser parser;
  private final Reader reader;
  private Iterator<CSVRecord> iterator;
  private CSVRecord nextRecord;
  private String[] headers;

  public CsvParser(Reader reader, CSVFormat format) throws IOException {
    this(reader, format, 0);
  }

  @SuppressWarnings("unchecked")
  public CsvParser(Reader reader, CSVFormat format, long initialPosition) throws IOException {
    Utils.checkNotNull(reader, "reader");
    Utils.checkNotNull(format, "format");
    Utils.checkArgument(initialPosition >= 0, "initialPosition must be greater or equal than zero");
    this.initialPosition = initialPosition;
    currentPos = initialPosition;
    if (initialPosition > 0) {
      format = format.withHeader((String)null);
      IOUtils.skipFully(reader, initialPosition);
    }
    this.reader = reader;
    parser = new CSVParser(reader, format);
  }

  private void init() throws IOException {
    headers = (initialPosition == 0) ? readHeader() : null;
    iterator = parser.iterator();
    nextRecord = nextRecord();
  }

  protected Reader getReader() {
    return reader;
  }

  protected CSVRecord nextRecord() throws IOException {
    return (iterator.hasNext()) ? iterator.next() : null;
  }

  protected String[] readHeader() throws IOException {
    List<String> headers = new ArrayList<>();
    Map<String, Integer> headerMap = parser.getHeaderMap();
    if (headerMap != null && !headerMap.isEmpty()) {
      for (int i = 0; i < headerMap.size(); i++) {
        headers.add(null);
      }
      for (Map.Entry<String, Integer> entry : headerMap.entrySet()) {
        headers.set(entry.getValue(), entry.getKey());
      }
    }
    return headers.toArray(new String[headers.size()]);
  }

  // returns null if the parser initial position is not zero, to get headers when working at an offset, 2
  // reader creations need to be done, one at zero to get the headers, another one at the desired offset.
  public String[] getHeaders() throws IOException {
    if (iterator == null) {
      init();
    }
    return headers;
  }

  // returns -1 if at the end of the reader
  public long getReaderPosition() {
    return currentPos;
  }

  //we don't expose the CSVRecord because we cannot make consistent when working at an offset greater than zero
  public String[] read() throws IOException {
    if (iterator == null) {
      init();
    }
    CSVRecord record = nextRecord;
    if (nextRecord != null) {
      nextRecord = nextRecord();
    }
    currentPos = (nextRecord != null) ? initialPosition + nextRecord.getCharacterPosition() : -1;
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
      parser.close();
    } catch (IOException ex) {
      //NOP
    }
  }
}
