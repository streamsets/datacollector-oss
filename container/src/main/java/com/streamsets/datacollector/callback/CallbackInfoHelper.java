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
package com.streamsets.datacollector.callback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.event.json.MetricRegistryJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class CallbackInfoHelper {

  private final static Logger LOG = LoggerFactory.getLogger(CallbackInfoHelper.class);

  private final CallbackInfo callbackInfo;

  private MetricRegistryJson metricRegistryJson;

  CallbackInfoHelper(CallbackInfo callbackInfo) {
    this.callbackInfo = callbackInfo;
  }


  public MetricRegistryJson getMetricRegistryJson() {
    if (callbackInfo.getCallbackObjectType() != CallbackObjectType.METRICS) {
      return null;
    }

    if(metricRegistryJson == null) {
      ObjectMapper objectMapper = ObjectMapperFactory.get();
      try {
        metricRegistryJson = objectMapper.readValue(callbackInfo.getCallbackObject(), MetricRegistryJson.class);
      } catch (IOException ex) {
        LOG.warn("Error while serializing slave callbackObject: , {}", ex.toString(), ex);
      }
    }
    return metricRegistryJson;
  }

}
