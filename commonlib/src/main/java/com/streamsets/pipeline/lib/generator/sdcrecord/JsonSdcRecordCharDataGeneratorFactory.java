/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.sdcrecord;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.ContextExtensions;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;

public class JsonSdcRecordCharDataGeneratorFactory extends CharDataGeneratorFactory {

  public static Map<String, Object> registerConfigs(Map<String, Object> configs) {
    return configs;
  }

  private final ContextExtensions context;

  public JsonSdcRecordCharDataGeneratorFactory(Stage.Context context, Map<String, Object> configs) {
    this.context = (ContextExtensions) context;
  }

  @Override
  public DataGenerator getGenerator(Writer writer) throws IOException, DataGeneratorException {
    return new JsonSdcRecordDataGenerator(context.createJsonRecordWriter(writer));
  }

}
