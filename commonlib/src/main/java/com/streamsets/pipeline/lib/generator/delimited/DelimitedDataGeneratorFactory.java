/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
