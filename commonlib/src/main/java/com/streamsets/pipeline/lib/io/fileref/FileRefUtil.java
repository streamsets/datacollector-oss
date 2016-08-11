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
package com.streamsets.pipeline.lib.io.fileref;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FileRefUtil {
  private FileRefUtil() {}

  //Metric Constants
  public static final String GAUGE_NAME = "File Transfer Statistics";
  public static final String FILE_NAME = "File Name";
  public static final String TRANSFER_THROUGHPUT = "Transfer Rate";
  public static final String COPIED_BYTES = "Copied Bytes";
  public static final String REMAINING_BYTES = "Remaining Bytes";
  public static final String TRANSFER_THROUGHPUT_METER = "transferRateKb";

  public static final String FILE_REF_FIELD_NAME = "fileRef";
  public static final String FILE_INFO_FIELD_NAME = "fileInfo";

  public static final String FILE_REF_FIELD_PATH = "/" + FILE_REF_FIELD_NAME;
  public static final String FILE_INFO_FIELD_PATH = "/" + FILE_INFO_FIELD_NAME;

  public static final ImmutableMap<String, ?> MANDATORY_METADATA_INFO =
      ImmutableMap
          .of("file", String.class)
          .of("filename", String.class)
          .of("owner", String.class)
          .of("group", String.class)
          .of("lastModifiedTime", Long.class)
          .of("lastAccessTime", Long.class)
          .of("creationTime", Long.class)
          .of("size", Long.class);

  public static final List<String> MANDATORY_FIELD_PATHS = ImmutableList.of(FILE_REF_FIELD_PATH, FILE_INFO_FIELD_PATH);

  public static Field getWholeFileRecordRootField(FileRef fileRef, Map<String, Object> metadata) {
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    map.put(FILE_REF_FIELD_NAME, Field.create(Field.Type.FILE_REF, fileRef));
    Map<String, Field> metadataField = new LinkedHashMap<>();
    for (String metadataKey : metadata.keySet()) {
      metadataField.put(metadataKey, createFieldForMetadata(metadata.get(metadataKey)));
    }
    map.put(FILE_INFO_FIELD_NAME, Field.create(metadataField));
    return Field.create(map);
  }

  public static Field createFieldForMetadata(Object metadataObject) {
    if (metadataObject == null) {
      return Field.create("");
    }
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
