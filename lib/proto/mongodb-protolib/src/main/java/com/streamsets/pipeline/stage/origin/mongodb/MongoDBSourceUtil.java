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
package com.streamsets.pipeline.stage.origin.mongodb;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.lib.util.JsonUtil;
import org.bson.types.Binary;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class MongoDBSourceUtil {
  public static final String BSON_TS_TIME_T_FIELD = "timestamp";
  public static final String BSON_TS_ORDINAL_FIELD = "ordinal";
  private static final Joiner JOINER = Joiner.on("::");

  private MongoDBSourceUtil() {
  }

  public static String getSourceRecordId(
      String connectionString,
      String database,
      String collection,
      String nextSourceOffset
  ) {
    return JOINER.join(Arrays.asList(connectionString, database, collection, nextSourceOffset));
  }

  public static Map<String, Field> createFieldFromDocument(Document doc) throws IOException {
    Set<Map.Entry<String, Object>> entrySet = doc.entrySet();
    Map<String, Field> fields = new HashMap<>(entrySet.size());
    for (Map.Entry<String, Object> entry : entrySet) {
      fields.put(entry.getKey(), jsonToField(entry.getValue()));
    }
    return fields;
  }

  @SuppressWarnings("unchecked")
  private static Field jsonToField(Object object) throws IOException {
    if (object instanceof ObjectId) {
      String objectId = object.toString();
      return JsonUtil.jsonToField(objectId);
    } else if (object instanceof Binary) {
      byte[] data = ((Binary) object).getData();
      return JsonUtil.jsonToField(data);
    } else if (object instanceof BsonTimestamp) {
      int time = ((BsonTimestamp) object).getTime();
      Date date = new Date(time * 1000L);
      Map<String, Object> jsonMap = new LinkedHashMap<>();
      jsonMap.put(BSON_TS_TIME_T_FIELD, date);
      jsonMap.put(BSON_TS_ORDINAL_FIELD, ((BsonTimestamp) object).getInc());
      return JsonUtil.jsonToField(jsonMap);
    } else if (object instanceof List) {
      List jsonList = (List) object;
      List<Field> list = new ArrayList<>(jsonList.size());
      for (Object element : jsonList) {
        list.add(jsonToField(element));
      }
      return Field.create(list);
    } else if (object instanceof Map) {
      Map<String, Object> jsonMap = (Map<String, Object>) object;
      Map<String, Field> map = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
        map.put(entry.getKey(), jsonToField(entry.getValue()));
      }
      return Field.create(map);
    }
    return JsonUtil.jsonToField(object);
  }
}
