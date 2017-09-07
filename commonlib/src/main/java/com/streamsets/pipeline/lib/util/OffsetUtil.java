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
package com.streamsets.pipeline.lib.util;

import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.ext.DataCollectorServices;
import com.streamsets.pipeline.api.ext.json.JsonMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class OffsetUtil {
  private static final JsonMapper JSON_MAPPER = DataCollectorServices.instance().get(JsonMapper.SERVICE_KEY);
  private static final Logger LOG = LoggerFactory.getLogger(OffsetUtil.class);

  private OffsetUtil() {}

  /**
   * Serialize the Map of table to offset to a String
   * @param offsetMap Map of table to Offset.
   * @return Serialized offset
   * @throws StageException When Serialization exception happens
   */
  public static String serializeOffsetMap(Map<String, String> offsetMap) throws IOException {
    return JSON_MAPPER.writeValueAsString(offsetMap);
  }

  /**
   * Deserialize String offset to Map of table to offset
   * @param lastSourceOffset Serialized offset String
   * @return Map of table to lastOffset
   * @throws StageException When Deserialization exception happens
   */
  @SuppressWarnings("unchecked")
  public static Map<String, String> deserializeOffsetMap(String lastSourceOffset) throws IOException {
    Map<String, String> offsetMap;
    if (StringUtils.isEmpty(lastSourceOffset)) {
      offsetMap = new HashMap<>();
    } else {
      offsetMap = JSON_MAPPER.readValue(lastSourceOffset, Map.class);
    }
    return offsetMap;
  }

}
