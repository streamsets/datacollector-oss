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

import com.streamsets.pipeline.container.Utils;
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
  private final Iterator<CSVRecord> iterator;
  private CSVRecord nextRecord;
  private final String[] headers;

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
    parser = new CSVParser(reader, format);
    iterator = parser.iterator();
    nextRecord = (iterator.hasNext()) ? iterator.next() : null;
    headers = (initialPosition == 0) ? getHeaders(parser) : null;
  }

  private String[] getHeaders(CSVParser parser) {
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
  public String[] getHeaders() {
    return headers;
  }

  // returns -1 if at the end of the reader
  public long getReaderPosition() {
    return currentPos;
  }

  //we don't expose the CSVRecord because we cannot make consistent when working at an offset greater than zero
  public String[] read() {
    CSVRecord record = nextRecord;
    if (nextRecord != null) {
      nextRecord = (iterator.hasNext()) ? iterator.next() : null;
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
  public void close() throws IOException {
    parser.close();
  }
}
