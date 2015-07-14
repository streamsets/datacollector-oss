/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator.delimited;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DelimitedDataGeneratorFactory extends DataGeneratorFactory {
  static final String KEY_PREFIX = "delimited.";
  public static final String HEADER_KEY = KEY_PREFIX + "header";
  static final String HEADER_DEFAULT = "header";
  public static final String VALUE_KEY = KEY_PREFIX + "value";
  static final String VALUE_DEFAULT = "value";
  public static final String REPLACE_NEWLINES_KEY = KEY_PREFIX + "replaceNewLines";
  static final boolean REPLACE_NEWLINES_DEFAULT = true;

  public static final Map<String, Object> CONFIGS;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(HEADER_KEY, HEADER_DEFAULT);
    configs.put(VALUE_KEY, VALUE_DEFAULT);
    configs.put(REPLACE_NEWLINES_KEY, REPLACE_NEWLINES_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of(CsvMode.class, CsvHeader.class);

  private final CSVFormat format;
  private final CsvHeader header;
  private final String headerKey;
  private final String valueKey;
  private final boolean replaceNewLines;

  public DelimitedDataGeneratorFactory(Settings settings) {
    super(settings);
    this.format = settings.getMode(CsvMode.class).getFormat();
    this.header = settings.getMode(CsvHeader.class);
    headerKey = settings.getConfig(HEADER_KEY);
    valueKey = settings.getConfig(VALUE_KEY);
    replaceNewLines = settings.getConfig(REPLACE_NEWLINES_KEY);
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    return new DelimitedCharDataGenerator(createWriter(os), format, header, headerKey, valueKey, replaceNewLines);
  }

}
