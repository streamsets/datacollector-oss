/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.sdk;

import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Source;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.StageType;
import com.streamsets.pipeline.runner.StageContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class ContextInfoCreator {

  public static Stage.Info createInfo(final String name, final String version, final String instanceName) {
    return new Stage.Info() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getVersion() {
        return version;
      }

      @Override
      public String getInstanceName() {
        return instanceName;
      }
    };
  }

  private static StageContext createContext(String instanceName, boolean isPreview, OnRecordError onRecordError,
      List<String> outputLanes) {
    return new StageContext(instanceName, StageType.SOURCE, isPreview, onRecordError, outputLanes,
      Collections.<String, Class<?>[]> emptyMap(), new HashMap<String, Object>(), false);
  }

  public static Source.Context createSourceContext(String instanceName, boolean isPreview, OnRecordError onRecordError,
      List<String> outputLanes) {
    return createContext(instanceName, isPreview, onRecordError, outputLanes);
  }

  public static Processor.Context createProcessorContext(String instanceName, boolean isPreview,
      OnRecordError onRecordError, List<String> outputLanes) {
    return createContext(instanceName, isPreview, onRecordError, outputLanes);
  }

  @SuppressWarnings("unchecked")
  public static Target.Context createTargetContext(String instanceName, boolean isPreview, OnRecordError onRecordError) {
    return createContext(instanceName, isPreview, onRecordError, Collections.EMPTY_LIST);
  }

}
