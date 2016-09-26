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
package com.streamsets.pipeline.sdk;

import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.email.EmailSender;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContextInfoCreator {

  private ContextInfoCreator() {}

  public static Stage.Info createInfo(final String name, final int version, final String instanceName) {
    return new Stage.Info() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public int getVersion() {
        return version;
      }

      @Override
      public String getInstanceName() {
        return instanceName;
      }
    };
  }

  private static StageContext createContext(
      Class<?> stageClass,
      String instanceName,
      boolean isPreview,
      OnRecordError onRecordError,
      List<String> outputLanes,
      String resourcesDir
  ) {
    Map<String, Class<?>[]> configToElDefMap;
    if(stageClass == null) {
      configToElDefMap = Collections.emptyMap();
    } else {
      try {
        configToElDefMap = ElUtil.getConfigToElDefMap(stageClass);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return new StageContext(
        instanceName,
        StageType.SOURCE,
        isPreview,
        onRecordError,
        outputLanes,
        configToElDefMap,
        new HashMap<String, Object>(),
        ExecutionMode.STANDALONE,
        resourcesDir,
        new EmailSender(new Configuration())
    );
  }

  public static Source.Context createSourceContext(Class<?> stageClass, String instanceName, boolean isPreview,
      OnRecordError onRecordError, List<String> outputLanes, String resourcesDir) {
    return createContext(stageClass, instanceName, isPreview, onRecordError, outputLanes, resourcesDir);
  }

  public static Source.Context createSourceContext(String instanceName, boolean isPreview, OnRecordError onRecordError,
                                                   List<String> outputLanes) {
    return createContext(null, instanceName, isPreview, onRecordError, outputLanes, null);
  }
  public static Source.Context createSourceContext(String instanceName, boolean isPreview, OnRecordError onRecordError,
                                                   List<String> outputLanes, String resourcesDir) {
    return createContext(null, instanceName, isPreview, onRecordError, outputLanes, resourcesDir);
  }

  @SuppressWarnings("unchecked")
  public static Target.Context createTargetContext(Class<?> stageClass, String instanceName, boolean isPreview,
      OnRecordError onRecordError, String resourcesDir) {
    return createContext(stageClass, instanceName, isPreview, onRecordError, Collections.EMPTY_LIST, resourcesDir);
  }

  public static Target.Context createTargetContext(String instanceName, boolean isPreview, OnRecordError onRecordError) {
    return createContext(null, instanceName, isPreview, onRecordError, Collections.EMPTY_LIST, null);
  }

  public static Processor.Context createProcessorContext(String instanceName, boolean isPreview, OnRecordError onRecordError) {
    return createContext(null, instanceName, isPreview, onRecordError, Collections.EMPTY_LIST, null);
  }

  public static void setLastBatch(Stage.Context context, long lastBatch) {
    ((StageContext)context).setLastBatchTime(lastBatch);
  }
}
