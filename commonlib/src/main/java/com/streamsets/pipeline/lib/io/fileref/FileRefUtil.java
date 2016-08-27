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
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.EventRecord;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.Stage;

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
  public static final String FILE = "File";
  public static final String TRANSFER_THROUGHPUT = "Transfer Rate";
  public static final String SENT_BYTES = "Sent Bytes";
  public static final String REMAINING_BYTES = "Remaining Bytes";
  public static final String TRANSFER_THROUGHPUT_METER = "transferRateKb";
  public static final String COMPLETED_FILE_COUNT = "Completed File Count";

  public static final String BRACKETED_TEMPLATE = "%s (%s)";

  //Whole File Record constants
  public static final String FILE_REF_FIELD_NAME = "fileRef";
  public static final String FILE_INFO_FIELD_NAME = "fileInfo";

  public static final String FILE_REF_FIELD_PATH = "/" + FILE_REF_FIELD_NAME;
  public static final String FILE_INFO_FIELD_PATH = "/" + FILE_INFO_FIELD_NAME;


  //Whole File event Record constants
  public static final String WHOLE_FILE_WRITE_FINISH_EVENT = "wholeFileProcessed";

  public static final String WHOLE_FILE_SOURCE_FILE_INFO = "sourceFileInfo";
  public static final String WHOLE_FILE_TARGET_FILE_INFO = "targetFileInfo";

  public static final String WHOLE_FILE_SOURCE_FILE_INFO_PATH = "/" + WHOLE_FILE_SOURCE_FILE_INFO;
  public static final String WHOLE_FILE_TARGET_FILE_INFO_PATH = "/" + WHOLE_FILE_TARGET_FILE_INFO;

  public static final String WHOLE_FILE_CHECKSUM = "checksum";
  public static final String WHOLE_FILE_CHECKSUM_ALGO = "checksumAlgorithm";


  public static final ImmutableSet<String> MANDATORY_METADATA_INFO =
      new ImmutableSet.Builder<String>().add("owner").add("size").build();


  public static final List<String> MANDATORY_FIELD_PATHS =
      ImmutableList.of(FILE_REF_FIELD_PATH, FILE_INFO_FIELD_PATH, FILE_INFO_FIELD_PATH + "/size");

  public static Field getWholeFileRecordRootField(FileRef fileRef, Map<String, Object> metadata) {
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();
    map.put(FILE_REF_FIELD_NAME, Field.create(Field.Type.FILE_REF, fileRef));
    map.put(FILE_INFO_FIELD_NAME, createFieldForMetadata(metadata));
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

  public static EventRecord createAndInitWholeFileEventRecord(Stage.Context context) {
    EventRecord wholeFileEventRecord = context.createEventRecord(WHOLE_FILE_WRITE_FINISH_EVENT, 1);
    Map<String, Field> fieldMap = new HashMap<>();
    fieldMap.put(FileRefUtil.WHOLE_FILE_SOURCE_FILE_INFO, Field.create(Field.Type.MAP, new HashMap<String, Field>()));
    fieldMap.put(FileRefUtil.WHOLE_FILE_TARGET_FILE_INFO, Field.create(Field.Type.MAP, new HashMap<String, Field>()));
    fieldMap.put(FileRefUtil.WHOLE_FILE_CHECKSUM, Field.create(Field.Type.STRING, ""));
    fieldMap.put(FileRefUtil.WHOLE_FILE_CHECKSUM_ALGO, Field.create(Field.Type.STRING, ""));
    wholeFileEventRecord.set(Field.create(Field.Type.MAP, fieldMap));
    return wholeFileEventRecord;
  }


}
