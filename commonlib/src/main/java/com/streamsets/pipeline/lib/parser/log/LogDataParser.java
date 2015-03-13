/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.log;

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

public abstract class LogDataParser implements DataParser {

  static final String TEXT_FIELD_NAME = "text";
  static final String TRUNCATED_FIELD_NAME = "truncated";

  private final Stage.Context context;
  private final String readerId;
  private final OverrunReader reader;
  private final int maxObjectLen;
  private final StringBuilder sb;
  private boolean eof;
  private final boolean retainOriginalText;

  public LogDataParser(Stage.Context context, String readerId, OverrunReader reader, long readerOffset,
                        int maxObjectLen, boolean retainOriginalText) throws IOException {
    this.context = context;
    this.readerId = readerId;
    this.reader = reader;
    this.maxObjectLen = maxObjectLen;
    this.retainOriginalText = retainOriginalText;
    reader.setEnabled(false);
    IOUtils.skipFully(reader, readerOffset);
    reader.setEnabled(true);
    sb = new StringBuilder(maxObjectLen > 0 ? maxObjectLen : 1024);
  }

  private boolean isOverMaxObjectLen(int len) {
    return maxObjectLen > -1 && len > maxObjectLen;
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
      if(retainOriginalText) {
        map.put(TEXT_FIELD_NAME, Field.create(sb.toString()));
        if (isOverMaxObjectLen(read)) {
          map.put(TRUNCATED_FIELD_NAME, Field.create(true));
        }
      }
      Map<String, Field> fieldsFromLogLine = parseLogLine(sb);
      if(fieldsFromLogLine != null && !fieldsFromLogLine.isEmpty()) {
        for (Map.Entry<String, Field> e : fieldsFromLogLine.entrySet()) {
          map.put(e.getKey(), e.getValue());
        }
      } else {
        //<TODO>Throw exception?
      }
      record.set(Field.create(map));
    } else {
      eof = true;
    }
    return record;
  }

  protected abstract Map<String, Field> parseLogLine(StringBuilder sb);

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
    while (c > -1 && !isOverMaxObjectLen(count) && !checkEolAndAdjust(c)) {
      count++;
      sb.append((char) c);
      c = reader.read();
    }
    if (isOverMaxObjectLen(count)) {
      sb.setLength(sb.length() - 1);
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
