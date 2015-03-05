/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.delimited;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;

public class DelimitedCharDataGeneratorFactory extends CharDataGeneratorFactory {
  static final String KEY_PREFIX = "delimited.";
  public static final String HEADER_KEY = KEY_PREFIX + "header";
  static final String HEADER_DEFAULT = "header";
  public static final String VALUE_KEY = KEY_PREFIX + "value";
  static final String VALUE_DEFAULT = "value";

  public static Map<String, Object> registerConfigs(Map<String, Object> configs) {
    configs.put(HEADER_KEY, HEADER_DEFAULT);
    configs.put(VALUE_KEY, VALUE_DEFAULT);
    return configs;
  }

  private final CSVFormat format;
  private final CsvHeader header;
  private final String headerKey;
  private final String valueKey;

  public DelimitedCharDataGeneratorFactory(Stage.Context context, CSVFormat format, CsvHeader header,
      Map<String, Object> configs) {
    this.format = format;
    this.header = header;
    this.headerKey = (String) configs.get(HEADER_KEY);
    this.valueKey = (String) configs.get(VALUE_KEY);
  }

  @Override
  public DataGenerator getGenerator(Writer writer) throws IOException, DataGeneratorException {
    return new DelimitedDataGenerator(writer, format, header, headerKey, valueKey);
  }

}
