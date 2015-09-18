/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.config.PipelineConfiguration;
import com.streamsets.datacollector.config.StageConfiguration;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.BeanHelper;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import com.streamsets.pipeline.api.Config;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PipelineConfigurationUtil {

  private static final String KEY = "key";
  private static final String VALUE = "value";
  public static Map<String, String> getFlattenedStringMap(String mapName, PipelineConfiguration pipelineConf) {
    Map<String, String> result = new HashMap<>();
    for (Map.Entry<String, Object> entry : getFlattenedMap(mapName, pipelineConf).entrySet()) {
      result.put(entry.getKey(), String.valueOf(entry.getValue()));
    }
    return result;
  }

  public static Map<String, Object> getFlattenedMap(String mapName, PipelineConfiguration pipelineConf) {
    Map<String, Object> constants = new HashMap<>();
    if(pipelineConf != null && pipelineConf.getConfiguration() != null) {
      for (Config config : pipelineConf.getConfiguration()) {
        if (mapName.equals(config.getName()) && config.getValue() != null) {
          for (Map<String, String> map : (List<Map<String, String>>) config.getValue()) {
            constants.put(map.get(KEY), map.get(VALUE));
          }
          return constants;
        }
      }
    }
    return constants;
  }

  public static String getSourceLibName(String pipelineJson) throws JsonParseException, JsonMappingException,
    IOException {
    ObjectMapper json = ObjectMapperFactory.getOneLine();
    PipelineConfigurationJson pipelineConfigBean = json.readValue(pipelineJson, PipelineConfigurationJson.class);
    PipelineConfiguration pipelineConf = BeanHelper.unwrapPipelineConfiguration(pipelineConfigBean);
    for (int i = 0; i < pipelineConf.getStages().size(); i++) {
      StageConfiguration stageConf = pipelineConf.getStages().get(i);
      if (stageConf.getInputLanes().isEmpty()) {
        return stageConf.getLibrary();
      }
    }
    return null;
  }

}
