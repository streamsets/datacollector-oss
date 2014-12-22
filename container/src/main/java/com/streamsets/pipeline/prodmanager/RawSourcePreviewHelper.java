/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.prodmanager;

import com.streamsets.pipeline.api.RawSourcePreviewer;
import com.streamsets.pipeline.config.ConfigDefinition;
import com.streamsets.pipeline.config.PipelineConfiguration;
import com.streamsets.pipeline.config.RawSourceDefinition;
import com.streamsets.pipeline.config.StageConfiguration;
import com.streamsets.pipeline.config.StageDefinition;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.stagelibrary.StageLibraryTask;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.store.PipelineStoreTask;
import com.streamsets.pipeline.util.*;
import com.streamsets.pipeline.util.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;

import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RawSourcePreviewHelper {

  private static final String MAX_SOURCE_PREVIEW_SIZE_KEY = "preview.maxSourcePreviewSize";
  private static final int MAX_SOURCE_PREVIEW_SIZE_DEFAULT = 4*1024;
  private static final String MIME_TYPE = "mimeType";
  private static final String PREVIEW_STRING = "previewString";

  public static Map<String, String> preview(String name, String rev, MultivaluedMap<String, String> previewParams,
                                            PipelineStoreTask store, StageLibraryTask stageLibrary,
                                            Configuration configuration)
      throws PipelineStoreException, PipelineRuntimeException, IOException {

    PipelineConfiguration pipelineConf = store.load(name, rev);
    if(pipelineConf.getStages().isEmpty()) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0159, name);
    }

    //Source stage is always the first one in the entire pipeline
    StageConfiguration stageConf = pipelineConf.getStages().get(0);
    StageDefinition stageDefinition = stageLibrary.getStage(stageConf.getLibrary(), stageConf.getStageName(),
      stageConf.getStageVersion());
    RawSourceDefinition rawSourceDefinition = stageDefinition.getRawSourceDefinition();
    List<ConfigDefinition> configDefinitions = rawSourceDefinition.getConfigDefinitions();

    validateParameters(previewParams, configDefinitions);

    //Attempt to load the previewer class from stage class loader
    Class previewerClass;
    try {
      previewerClass = stageDefinition.getStageClassLoader().loadClass(stageDefinition.getRawSourceDefinition()
        .getRawSourcePreviewerClass());
    } catch (ClassNotFoundException e) {
      //Try loading from this class loader
      try {
        previewerClass = RawSourcePreviewHelper.class.getClassLoader().loadClass(
          stageDefinition.getRawSourceDefinition().getRawSourcePreviewerClass());
      } catch (ClassNotFoundException e1) {
        throw new RuntimeException(e1);
      }
    }

    int bytesToRead = configuration.get(MAX_SOURCE_PREVIEW_SIZE_KEY, MAX_SOURCE_PREVIEW_SIZE_DEFAULT);
    bytesToRead = Math.min(bytesToRead, MAX_SOURCE_PREVIEW_SIZE_DEFAULT);

    RawSourcePreviewer rawSourcePreviewer;
    try {
      rawSourcePreviewer = (RawSourcePreviewer) previewerClass.newInstance();
      //inject values from url to fields in the rawSourcePreviewer
      for(ConfigDefinition confDef : configDefinitions) {
        Field f = previewerClass.getField(confDef.getFieldName());
        f.set(rawSourcePreviewer, getValueFromParam(f, previewParams.get(confDef.getName()).get(0)));
      }
      rawSourcePreviewer.setMimeType(rawSourceDefinition.getMimeType());
    } catch (IllegalAccessException | InstantiationException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }

    BoundedInputStream bIn = new BoundedInputStream(rawSourcePreviewer.preview(bytesToRead), bytesToRead);
    String previewString = IOUtils.toString(bIn);
    Map<String, String> previewMap = new HashMap<>();
    previewMap.put(MIME_TYPE, rawSourcePreviewer.getMimeType());
    previewMap.put(PREVIEW_STRING, previewString);

    return previewMap;
  }

  private static Object getValueFromParam(Field field, String stringValue) {
    Class<?> type = field.getType();
    if(String.class.isAssignableFrom(type)) {
      return stringValue;
    } else if (Integer.class.isAssignableFrom(type) || Integer.TYPE == type) {
      return Integer.parseInt(stringValue);
    } else if (Long.class.isAssignableFrom(type) || Long.TYPE == type) {
      return Long.parseLong(stringValue);
    } else if (Boolean.class.isAssignableFrom(type) || Boolean.TYPE == type) {
      return Boolean.parseBoolean(stringValue);
    }
    return null;
  }

  private static void validateParameters(MultivaluedMap<String, String> previewParams,
                                  List<ConfigDefinition> configDefinitions) throws PipelineRuntimeException {
    //validate that all configuration required by config definitions are supplied through the URL
    List<String> requiredPropertiesNotSet = new ArrayList<>();
    for(ConfigDefinition confDef: configDefinitions) {
      if(confDef.isRequired() && !previewParams.containsKey(confDef.getName())) {
        requiredPropertiesNotSet.add(confDef.getName());
      }
    }

    if(!requiredPropertiesNotSet.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append(requiredPropertiesNotSet.get(0));
      for(int i = 1; i < requiredPropertiesNotSet.size(); i++) {
        sb.append(", ").append(requiredPropertiesNotSet.get(i));
      }
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0160, sb.toString());
    }
  }
}
