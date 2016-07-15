/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.parser.wholefile;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.parser.AbstractDataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class WholeFileDataParser extends AbstractDataParser {
  private static final String FILE_REF = "fileRef";
  private static final String FILE_INFO = "fileInfo";
  private static final String OFFSET_MINUS_ONE = "-1";
  private static final String OFFSET_ZERO = "0";
  private Stage.Context context;
  private String id;
  private FileRef fileRef;
  private Map<String, Object> metadata;
  private boolean alreadyParsed;
  private boolean isClosed;

  WholeFileDataParser(
      Stage.Context context,
      String id,
      Map<String, Object> metadata,
      FileRef fileRef
  )  {
    this.id = id;
    this.context = context;
    this.metadata = metadata;
    this.fileRef = fileRef;
  }

  @Override
  public Record parse() throws IOException, DataParserException {
    if (isClosed) {
      throw new IOException("The parser is closed");
    }
    if (!alreadyParsed) {
      Record record = context.createRecord(id);
      LinkedHashMap<String, Field> map = new LinkedHashMap<>();
      map.put(FILE_REF, Field.create(Field.Type.FILE_REF, fileRef));
      Map<String, Field> metadataField = new LinkedHashMap<>();
      for (String metadataKey : metadata.keySet()) {
        metadataField.put(metadataKey, createFieldForMetadata(metadata.get(metadataKey)));
      }
      map.put(FILE_INFO, Field.create(metadataField));
      record.set(Field.create(map));
      alreadyParsed = true;
      return record;
    }
    return null;
  }

  @Override
  public String getOffset() throws DataParserException, IOException {
    //Will return the offset -1 if already parsed else return 0.
    return (!alreadyParsed)? OFFSET_ZERO : OFFSET_MINUS_ONE;
  }

  @Override
  public void close() throws IOException {
    isClosed = true;
  }

  private static Field createFieldForMetadata(Object metadataObject) {
    if (metadataObject instanceof Boolean) {
      return Field.create((Boolean) metadataObject);
    } else if (metadataObject instanceof Character) {
      return Field.create((Character) metadataObject);
    } else if (metadataObject instanceof Byte) {
      return Field.create((Byte) metadataObject);
    } else if (metadataObject instanceof Short) {
      return Field.create((Short) metadataObject);
    } else if (metadataObject instanceof Integer) {
      return Field.create((Integer) metadataObject);
    } else if (metadataObject instanceof Long) {
      return Field.create((Long) metadataObject);
    } else if (metadataObject instanceof Float) {
      return Field.create((Float) metadataObject);
    } else if (metadataObject instanceof Double) {
      return Field.create((Double) metadataObject);
    } else if (metadataObject instanceof Date) {
      return Field.createDatetime((Date) metadataObject);
    } else if (metadataObject instanceof BigDecimal) {
      return Field.create((BigDecimal) metadataObject);
    } else if (metadataObject instanceof String) {
      return Field.create((String) metadataObject);
    } else if (metadataObject instanceof byte[]) {
      return Field.create((byte[]) metadataObject);
    } else if (metadataObject instanceof Collection) {
      Iterator iterator = ((Collection)metadataObject).iterator();
      List<Field> fields = new ArrayList<>();
      while (iterator.hasNext()) {
        fields.add(createFieldForMetadata(iterator.next()));
      }
      return Field.create(fields);
    } else if (metadataObject instanceof Map) {
      boolean isListMap = (metadataObject instanceof LinkedHashMap);
      Map<String, Field> fieldMap = isListMap? new LinkedHashMap<String, Field>() : new HashMap<String, Field>();
      Map map = (Map)metadataObject;
      for (Object key : map.keySet()) {
        fieldMap.put(key.toString(), createFieldForMetadata(map.get(key)));
      }
      return isListMap? Field.create(Field.Type.LIST_MAP, fieldMap) : Field.create(fieldMap);
    } else {
      return Field.create(metadataObject.toString());
    }
  }
}
