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

import com.streamsets.pipeline.api.ext.io.CountingReader;
import com.streamsets.pipeline.api.ext.io.ObjectLengthException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ExceptionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

public class CsvParser implements DelimitedDataParser {
  private long currentPos;
  private long skipLinesPosCorrection;
  private final CSVParser parser;
  private final CountingReader reader;
  private final int maxObjectLen;
  private Iterator<CSVRecord> iterator;
  private CSVRecord nextRecord;
  private final String[] headers;
  private boolean closed;

  public CsvParser(Reader reader, CSVFormat format, int maxObjectLen) throws IOException {
    this(new CountingReader(reader), format, maxObjectLen, 0, 0);
  }

  @SuppressWarnings("unchecked")
  public CsvParser(
      CountingReader reader,
      CSVFormat format,
      int maxObjectLen,
      long initialPosition,
      int skipStartLines
  ) throws IOException {
    Utils.checkNotNull(reader, "reader");
    Utils.checkNotNull(reader.getPos() == 0,
                       "reader must be in position zero, the CsvParser will fast-forward to the initialPosition");
    Utils.checkNotNull(format, "format");
    Utils.checkArgument(initialPosition >= 0, "initialPosition must be greater or equal than zero");
    Utils.checkArgument(skipStartLines >= 0, "skipStartLines must be greater or equal than zero");
    this.reader = reader;
    currentPos = initialPosition;
    this.maxObjectLen = maxObjectLen;
    if (initialPosition == 0) {
      if (skipStartLines > 0) {
        skipLinesPosCorrection = skipLines(reader, skipStartLines);
        currentPos = skipLinesPosCorrection;
      }
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
    fixNullHeaderNames();
  }

  private void fixNullHeaderNames() {
    // makes sure any blank column names in the header get replaced with an incremental string value
    if (headers == null) return;
    for (int x=0; x < headers.length; x++) {
      if (StringUtils.isEmpty(headers[x])) {
        headers[x] = "empty-" + x;
      }
    }
  }

  protected Reader getReader() {
    return reader;
  }

  protected CSVRecord nextRecord() throws IOException {
    // Since  the iterator interface doesn't allow  any checked exceptions the underlying CVS library will
    // re-throw any IOException  as IllegalStateException. We catch it here and unwrap it. If the cause is
    // in fact IOException we simply throw that exception instead.
    try {
      return (iterator.hasNext()) ? iterator.next() : null;
    } catch (IllegalStateException  e) {
      Throwable cause = e.getCause();
      if(cause instanceof IOException) {
        throw (IOException)cause;
      }

      throw e;
    }
  }

  @Override
  public String[] getHeaders() throws IOException {
    return headers;
  }

  @Override
  public long getReaderPosition() {
    return currentPos;
  }

  @Override
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
    long prevPos = currentPos;
    currentPos = (nextRecord != null) ? nextRecord.getCharacterPosition() + skipLinesPosCorrection : reader.getPos();
    if (maxObjectLen > -1) {
      if (currentPos - prevPos > maxObjectLen) {
        ExceptionUtils.throwUndeclared(new ObjectLengthException(Utils.format(
            "CSV Object at offset '{}' exceeds max length '{}'", prevPos, maxObjectLen), prevPos));
      }
    }
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
    try {
      closed = true;
      parser.close();
    } catch (IOException ex) {
      //NOP
    }
  }
}
