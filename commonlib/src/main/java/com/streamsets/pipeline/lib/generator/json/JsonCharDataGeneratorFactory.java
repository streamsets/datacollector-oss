/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.json;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;

public class JsonCharDataGeneratorFactory extends CharDataGeneratorFactory {

  public static Map<String, Object> registerConfigs(Map<String, Object> configs) {
    return configs;
  }

  private final JsonMode jsonMode;

  public JsonCharDataGeneratorFactory(Stage.Context context, JsonMode jsonMode, Map<String, Object> configs) {
    this.jsonMode = jsonMode;
  }

  @Override
  public DataGenerator getGenerator(Writer writer) throws IOException, DataGeneratorException {
    return new JsonDataGenerator(writer, jsonMode);
  }

}
