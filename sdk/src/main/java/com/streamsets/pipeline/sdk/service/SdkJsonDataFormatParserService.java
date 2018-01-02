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
package com.streamsets.pipeline.sdk.service;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.BaseService;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.api.ext.JsonObjectReader;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.api.service.ServiceDef;
import com.streamsets.pipeline.api.service.dataformats.DataFormatParserService;
import com.streamsets.pipeline.api.service.dataformats.DataParser;
import com.streamsets.pipeline.api.service.dataformats.DataParserException;
import com.streamsets.pipeline.sdk.Errors;
import com.streamsets.pipeline.sdk.RecordCreator;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ServiceDef(
  provides = DataFormatParserService.class,
  version = 1,
  label = "(Test) Runner implementation of very simple DataFormatParserService that will always parse input as JSON (field /)."
)
public class SdkJsonDataFormatParserService extends BaseService implements DataFormatParserService {

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    try {
      ContextExtensions ext = ((ContextExtensions) getContext());
      return new DataParserImpl(ext.createJsonObjectReader(new InputStreamReader(is), Long.parseLong(offset), Mode.MULTIPLE_OBJECTS, Object.class));
    } catch (IOException e) {
      throw new DataParserException(Errors.SDK_0001, e.toString(), e);
    }
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    try {
      ContextExtensions ext = ((ContextExtensions) getContext());
      return new DataParserImpl(ext.createJsonObjectReader(reader, offset, Mode.MULTIPLE_OBJECTS, Object.class));
    } catch (IOException e) {
      throw new DataParserException(Errors.SDK_0001, e.toString(), e);
    }
  }

  @Override
  public DataParser getParser(String id, Map<String, Object> metadata, FileRef fileRef) throws DataParserException {
    throw new UnsupportedOperationException("WholeFileFormat is not supported");
  }

  private static class DataParserImpl implements DataParser {

    private final JsonObjectReader recordReader;
    DataParserImpl(JsonObjectReader recordReader) {
      this.recordReader = recordReader;
    }

    @Override
    public Record parse() throws IOException, DataParserException {
      Object object = recordReader.read();
      if(object == null) {
        return null;
      }

      Record record = RecordCreator.create();
      record.set(jsonToField(object));
      return record;
    }

    @Override
    public String getOffset() throws DataParserException, IOException {
      return String.valueOf(recordReader.getReaderPosition());
    }

    @Override
    public void setTruncated() {
      // No-op
    }

    @Override
    public void close() throws IOException {
      recordReader.close();
    }
  }

  private static Field jsonToField(Object json) {
    Field field;
    if (json == null) {
      field = Field.create(Field.Type.STRING, null);
    } else if (json instanceof List) {
      List jsonList = (List) json;
      List<Field> list = new ArrayList<>(jsonList.size());
      for (Object element : jsonList) {
        list.add(jsonToField(element));
      }
      field = Field.create(list);
    } else if (json instanceof Map) {
      Map<String, Object> jsonMap = (Map<String, Object>) json;
      Map<String, Field> map = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
        map.put(entry.getKey(), jsonToField(entry.getValue()));
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
      throw new IllegalArgumentException("Unknown type: " + json.getClass().getSimpleName());
    }
    return field;
  }
}
