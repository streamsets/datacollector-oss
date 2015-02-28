/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.text;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TextDataParser implements DataParser {
  private final Stage.Context context;
  private final String readerId;
  private final OverrunReader reader;
  private final int maxObjectLen;
  private final String fieldTextName;
  private final String fieldTruncatedName;
  private final StringBuilder sb;
  private boolean eof;

  public TextDataParser(Stage.Context context, String readerId, OverrunReader reader, long readerOffset,
      int maxObjectLen, String fieldTextName, String fieldTruncatedName) throws IOException {
    this.context = context;
    this.readerId = readerId;
    this.reader = reader;
    this.maxObjectLen = maxObjectLen;
    this.fieldTextName = fieldTextName;
    this.fieldTruncatedName = fieldTruncatedName;
    reader.setEnabled(false);
    IOUtils.skipFully(reader, readerOffset);
    reader.setEnabled(true);
    sb = new StringBuilder(maxObjectLen);
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    long offset = reader.getPos();
    sb.setLength(0);
    int read = readLine(sb);
    Record record = null;
    if (read > -1) {
      record = context.createRecord(readerId + "::" + offset);
      Map<String, Field> map = new HashMap<>();
      map.put(fieldTextName, Field.create(sb.toString()));
      if (read > maxObjectLen) {
        map.put(fieldTruncatedName, Field.create(true));
      }
      record.set(Field.create(map));
    } else {
      eof = true;
    }
    return record;
  }

  @Override
  public long getOffset() {
    return (eof) ? -1 : reader.getPos();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  // returns the reader line length, the StringBuilder has up to maxObjectLen chars
  int readLine(StringBuilder sb) throws IOException {
    int c = reader.read();
    int count = (c == -1) ? -1 : 0;
    while (c > -1 && count < maxObjectLen && !checkEolAndAdjust(c)) {
      count++;
      sb.append((char) c);
      c = reader.read();
    }
    if (count >= maxObjectLen) {
      while (c > -1 && c != '\n' && c != '\r') {
        count++;
        c = reader.read();
      }
      checkEolAndAdjust(c);
    }
    return count;
  }

  boolean checkEolAndAdjust(int c) throws IOException {
    boolean eol = false;
    if (c == '\n') {
      eol = true;
    } else if (c == '\r') {
      eol = true;
      reader.mark(1);
      c = reader.read();
      if (c != '\n') {
        reader.reset();
      }
    }
    return eol;
  }

}
