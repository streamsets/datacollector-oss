/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser.text;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.io.OverrunLineReader;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TextCharDataParser implements DataParser {
  private final Stage.Context context;
  private final String readerId;
  private final boolean collapseAllLines;
  private final OverrunLineReader reader;
  private final int maxObjectLen;
  private final String fieldTextName;
  private final String fieldTruncatedName;
  private final StringBuilder recordIdSb;
  private final int recordIdOffset;
  private final StringBuilder sb;
  private boolean eof;

  public TextCharDataParser(Stage.Context context, String readerId, boolean collapseAllLines, OverrunReader reader,
      long readerOffset, int maxObjectLen, String fieldTextName, String fieldTruncatedName) throws IOException {
    this.context = context;
    this.readerId = readerId;
    this.collapseAllLines = collapseAllLines;
    this.reader = new OverrunLineReader(reader, maxObjectLen);
    this.maxObjectLen = maxObjectLen;
    this.fieldTextName = fieldTextName;
    this.fieldTruncatedName = fieldTruncatedName;
    reader.setEnabled(false);
    IOUtils.skipFully(reader, readerOffset);
    reader.setEnabled(true);
    sb = new StringBuilder(maxObjectLen > 0 ? maxObjectLen : 1024);
    recordIdSb = new StringBuilder(readerId.length() + 15);
    recordIdSb.append(readerId).append("::");
    recordIdOffset = recordIdSb.length();
  }

  private boolean isOverMaxObjectLen(int len) {
    return maxObjectLen > -1 && len > maxObjectLen;
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    Record record;
    if (collapseAllLines) {
      record = parseAll();
    } else {
      record = parseLine();
    }
    return record;
  }

  public Record parseAll() throws IOException, DataParserException {
    Record record = null;
    reader.resetCount();
    long offset = reader.getPos();
    sb.setLength(0);
    while (reader.readLine(sb) > -1) {
      sb.append('\n');
    }
    if (sb.length() > 0) {
      record = context.createRecord(readerId + "::" + offset);
      Map<String, Field> map = new HashMap<>();
      map.put(fieldTextName, Field.create(sb.toString()));
      if (isOverMaxObjectLen(sb.length())) {
        map.put(fieldTruncatedName, Field.create(true));
      }
      record.set(Field.create(map));
    }
    eof = true;
    return record;
  }

  public Record parseLine() throws IOException, DataParserException {
    reader.resetCount();
    long offset = reader.getPos();
    sb.setLength(0);
    int read = reader.readLine(sb);
    Record record = null;
    if (read > -1) {
      recordIdSb.setLength(recordIdOffset);
      recordIdSb.append(offset);
      record = context.createRecord(recordIdSb.toString());
      Map<String, Field> map = new HashMap<>();
      map.put(fieldTextName, Field.create(sb.toString()));
      if (isOverMaxObjectLen(read)) {
        map.put(fieldTruncatedName, Field.create(true));
      }
      record.set(Field.create(map));
    } else {
      eof = true;
    }
    return record;
  }

  @Override
  public String getOffset() {
    return (eof) ? String.valueOf(-1) : String.valueOf(reader.getPos());
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

}
