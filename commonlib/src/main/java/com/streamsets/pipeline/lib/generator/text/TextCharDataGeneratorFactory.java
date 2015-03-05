/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.text;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;

public class TextCharDataGeneratorFactory extends CharDataGeneratorFactory {
  static final String KEY_PREFIX = "text.";
  public static final String FIELD_PATH_KEY = KEY_PREFIX + "fieldPath";
  static final String FIELD_PATH_DEFAULT = "";
  public static final String EMPTY_LINE_IF_NULL_KEY = KEY_PREFIX + "emptyLineIfNull";
  static final boolean EMPTY_LINE_IF_NULL_DEFAULT = false;

  public static Map<String, Object> registerConfigs(Map<String, Object> configs) {
    configs.put(FIELD_PATH_KEY, FIELD_PATH_DEFAULT);
    configs.put(EMPTY_LINE_IF_NULL_KEY, EMPTY_LINE_IF_NULL_DEFAULT);
    return configs;
  }

  private final String fieldPath;
  private final boolean emptyLineIfNullDefault;

  public TextCharDataGeneratorFactory(Stage.Context context, Map<String, Object> configs) {
    fieldPath = (String) configs.get(FIELD_PATH_KEY);
    emptyLineIfNullDefault = (Boolean) configs.get(EMPTY_LINE_IF_NULL_KEY);
  }

  @Override
  public DataGenerator getGenerator(Writer writer) throws IOException, DataGeneratorException {
    return new TextDataGenerator(writer, fieldPath, emptyLineIfNullDefault);
  }

}
