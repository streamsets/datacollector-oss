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
package com.streamsets.pipeline.lib.parser.text;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.io.AbstractOverrunDelimitedReader;
import com.streamsets.pipeline.lib.io.OverrunCustomDelimiterReader;
import com.streamsets.pipeline.lib.io.OverrunLineReader;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TextCharDataParser extends AbstractDataParser {

  private static final Logger LOG = LoggerFactory.getLogger(TextCharDataParser.class);

  private final ProtoConfigurableEntity.Context context;
  private final String readerId;
  private final boolean collapseAllLines;
  private final AbstractOverrunDelimitedReader reader;
  private final int maxObjectLen;
  private final String fieldTextName;
  private final String fieldTruncatedName;
  private final StringBuilder recordIdSb;
  private final int recordIdOffset;
  private final GenericObjectPool<StringBuilder> stringBuilderPool;
  private final StringBuilder stringBuilder;

  private boolean eof;

  public TextCharDataParser(
      ProtoConfigurableEntity.Context context,
      String readerId,
      boolean collapseAllLines,
      boolean useCustomDelimiter,
      String customDelimiter,
      boolean includeCustomDelimiterInText,
      OverrunReader reader,
      long readerOffset,
      int maxObjectLen,
      String fieldTextName,
      String fieldTruncatedName,
      GenericObjectPool<StringBuilder> stringBuilderPool
  ) throws IOException {
    this.context = context;
    this.readerId = readerId;
    this.collapseAllLines = collapseAllLines;
    this.reader = (!collapseAllLines && useCustomDelimiter)?
        new OverrunCustomDelimiterReader(reader, maxObjectLen, customDelimiter, includeCustomDelimiterInText):
        new OverrunLineReader(reader, maxObjectLen);
    this.maxObjectLen = maxObjectLen;
    this.fieldTextName = fieldTextName;
    this.fieldTruncatedName = fieldTruncatedName;
    reader.setEnabled(false);
    IOUtils.skipFully(reader, readerOffset);
    reader.setEnabled(true);

    this.stringBuilderPool = stringBuilderPool;
    try {
      this.stringBuilder = stringBuilderPool.borrowObject();
      LOG.debug("Borrowed string builder from pool. Num Active {}, Num Idle {}", this.stringBuilderPool.getNumActive(), this.stringBuilderPool.getNumIdle());
    } catch (Exception e) {
      throw new IOException(Utils.format("Error borrowing string builder object from pool : {}", e.toString()), e);
    }

    recordIdSb = new StringBuilder(readerId.length() + 15);
    recordIdSb.append(readerId).append("::");
    recordIdOffset = recordIdSb.length();
  }

  private boolean isOverMaxObjectLen(int len) {
    return maxObjectLen > -1 && len > maxObjectLen;
  }

  private boolean isTruncated(int len) {
    return isOverMaxObjectLen(len) || truncated;
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
    stringBuilder.setLength(0);
    while (reader.readLine(stringBuilder) > -1) {
      stringBuilder.append('\n');
    }
    if (stringBuilder.length() > 0) {
      record = context.createRecord(readerId + "::" + offset);
      Map<String, Field> map = new HashMap<>();
      map.put(fieldTextName, Field.create(stringBuilder.toString()));
      if (isTruncated(stringBuilder.length())) {
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
    stringBuilder.setLength(0);
    int read = reader.readLine(stringBuilder);
    Record record = null;
    if (read > -1) {
      recordIdSb.setLength(recordIdOffset);
      recordIdSb.append(offset);
      record = context.createRecord(recordIdSb.toString());
      Map<String, Field> map = new HashMap<>();
      map.put(fieldTextName, Field.create(stringBuilder.toString()));
      if (isTruncated(read)) {
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
    stringBuilderPool.returnObject(this.stringBuilder);
    LOG.debug("Returned string builder to pool. Num Active {}, Num Idle {}", this.stringBuilderPool.getNumActive(), this.stringBuilderPool.getNumIdle());
    reader.close();
  }

}
