/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.generator.xml;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class XmlDataGeneratorFactory extends DataGeneratorFactory {

  public static final Map<String, Object> CONFIGS;

  static final String KEY_PREFIX = "xml.";
  public static final String SCHEMA_VALIDATION = KEY_PREFIX + "schemaValidation";
  static final boolean SCHEMA_VALIDATION_DEFAULT = false;
  public static final String PRETTY_FORMAT = KEY_PREFIX + "prettyFormat";
  static final boolean PRETTY_FORMAT_DEFAULT = false;
  public static final String SCHEMAS = KEY_PREFIX + "validationSchemas";
  static final List<String> SCHEMAS_DEFAULT = Collections.emptyList();

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(SCHEMA_VALIDATION, SCHEMA_VALIDATION_DEFAULT);
    configs.put(PRETTY_FORMAT, PRETTY_FORMAT_DEFAULT);
    configs.put(SCHEMAS, SCHEMAS_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of();

  private final boolean schemaValidation;
  private final List<String> schemas;
  private final boolean prettyFormat;

  public XmlDataGeneratorFactory(Settings settings) {
    super(settings);
    Utils.checkNotNull(settings, "settings");
    this.schemaValidation = settings.getConfig(SCHEMA_VALIDATION);
    this.schemas = settings.getConfig(SCHEMAS);
    this.prettyFormat = settings.getConfig(PRETTY_FORMAT);
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    return new XmlCharDataGenerator(createWriter(os), isSchemaValidation(), getSchemas(), isPrettyFormat());
  }

  boolean isSchemaValidation() {
    return schemaValidation;
  }

  List<String> getSchemas() {
    return schemas;
  }

  boolean isPrettyFormat() {
    return prettyFormat;
  }
}
