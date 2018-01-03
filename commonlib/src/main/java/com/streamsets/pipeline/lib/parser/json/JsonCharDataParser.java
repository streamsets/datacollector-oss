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
package com.streamsets.pipeline.lib.parser.json;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonObjectReader;
import com.streamsets.pipeline.api.ext.io.ObjectLengthException;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JsonCharDataParser extends AbstractDataParser {
  private final ProtoConfigurableEntity.Context context;
  private final String readerId;
  private final int maxObjectLen;
  private final JsonObjectReader parser;
  private boolean eof;

  public JsonCharDataParser(
      ProtoConfigurableEntity.Context context,
      String readerId,
      OverrunReader reader,
      long readerOffset,
      Mode mode,
      int maxObjectLen
  ) throws IOException {
    this.context = context;
    this.readerId = readerId;
    this.maxObjectLen = maxObjectLen;
    parser = ((ContextExtensions) context).createJsonObjectReader(
        reader,
        readerOffset,
        maxObjectLen,
        mode,
        Object.class
    );
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    Record record = null;
    long offset = parser.getReaderPosition();
    try {
      Object json = parser.read();
      if (json != null) {
        record = createRecord(offset, json);
      } else {
        eof = true;
      }
    } catch (ObjectLengthException ex) {
      throw new DataParserException(Errors.JSON_PARSER_02, readerId, offset, maxObjectLen);
    }
    return record;
  }

  public Field parseAsField() throws IOException, DataParserException {
    long offset = parser.getReaderPosition();
    try {
      Object json = parser.read();
      return (json != null) ? jsonToField(json,  offset) : null;
    } catch (ObjectLengthException ex) {
      throw new DataParserException(Errors.JSON_PARSER_02, readerId, offset, maxObjectLen);
    }
  }

  protected Record createRecord(long offset, Object json) throws DataParserException {
    Record record = context.createRecord(readerId + "::" + offset);
    record.set(jsonToField(json,  offset));
    return record;
  }

  @SuppressWarnings("unchecked")
  protected Field jsonToField(Object json, long offset) throws DataParserException {
    Field field;
    if (json == null) {
      field = Field.create(Field.Type.STRING, null);
    } else if (json instanceof List) {
      List jsonList = (List) json;
      List<Field> list = new ArrayList<>(jsonList.size());
      for (Object element : jsonList) {
        list.add(jsonToField(element, offset));
      }
      field = Field.create(list);
    } else if (json instanceof Map) {
      Map<String, Object> jsonMap = (Map<String, Object>) json;
      Map<String, Field> map = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
        map.put(entry.getKey(), jsonToField(entry.getValue(), offset));
      }
      field = Field.create(map);
    } else if (json instanceof String) {
      field = Field.create((String) json);
    } else if (json instanceof Boolean) {
      field = Field.create((Boolean) json);
    } else if (json instanceof Character) {
      field = Field.create((Character) json);
    } else if (json instanceof Byte) {
      field = Field.create((Byte) json);
    } else if (json instanceof Short) {
      field = Field.create((Short) json);
    } else if (json instanceof Integer) {
      field = Field.create((Integer) json);
    } else if (json instanceof Long) {
      field = Field.create((Long) json);
    } else if (json instanceof Float) {
      field = Field.create((Float) json);
    } else if (json instanceof Double) {
      field = Field.create((Double) json);
    } else if (json instanceof byte[]) {
      field = Field.create((byte[]) json);
    } else if (json instanceof Date) {
      field = Field.createDate((Date) json);
    } else if (json instanceof BigDecimal) {
      field = Field.create((BigDecimal) json);
    } else if (json instanceof BigInteger) {
      field = Field.create(new BigDecimal((BigInteger) json));
    } else {
      throw new DataParserException(Errors.JSON_PARSER_01, readerId, offset, json.getClass().getSimpleName());
    }
    return field;
  }

  @Override
  public String getOffset() {
    return eof ? String.valueOf(-1) : String.valueOf(parser.getReaderPosition());
  }

  @Override
  public void close() throws IOException {
    parser.close();
  }

}
