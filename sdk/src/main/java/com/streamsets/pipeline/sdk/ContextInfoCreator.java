/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.datacollector.config.StageType;
import com.streamsets.datacollector.runner.StageContext;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContextInfoCreator {

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

  private static StageContext createContext(Class<?> stageClass, String instanceName, boolean isPreview,
                                            OnRecordError onRecordError, List<String> outputLanes, String resourcesDir) {
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
    return new StageContext(instanceName, StageType.SOURCE, isPreview, onRecordError, outputLanes, configToElDefMap,
      new HashMap<String, Object>(), false, resourcesDir);
  }

  public static Source.Context createSourceContext(Class<?> stageClass, String instanceName, boolean isPreview,
      OnRecordError onRecordError, List<String> outputLanes, String resourcesDir) {
    return createContext(stageClass, instanceName, isPreview, onRecordError, outputLanes, resourcesDir);
  }

  public static Source.Context createSourceContext(String instanceName, boolean isPreview, OnRecordError onRecordError,
                                                   List<String> outputLanes) {
    return createContext(null, instanceName, isPreview, onRecordError, outputLanes, null);
  }

  @SuppressWarnings("unchecked")
  public static Target.Context createTargetContext(Class<?> stageClass, String instanceName, boolean isPreview,
      OnRecordError onRecordError, String resourcesDir) {
    return createContext(stageClass, instanceName, isPreview, onRecordError, Collections.EMPTY_LIST, resourcesDir);
  }

  public static Target.Context createTargetContext(String instanceName, boolean isPreview, OnRecordError onRecordError) {
    return createContext(null, instanceName, isPreview, onRecordError, Collections.EMPTY_LIST, null);
  }

  public static void setLastBatch(Stage.Context context, long lastBatch) {
    ((StageContext)context).setLastBatchTime(lastBatch);
  }
}
