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
package com.streamsets.pipeline.lib.generator.text;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.config.TextFieldMissingAction;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TextDataGeneratorFactory extends DataGeneratorFactory {
  static final String KEY_PREFIX = "text.";
  public static final String FIELD_PATH_KEY = KEY_PREFIX + "fieldPath";
  static final String FIELD_PATH_DEFAULT = "";
  public static final String MISSING_FIELD_ACTION_KEY = "missingField";
  public static final TextFieldMissingAction MISSING_FIELD_ACTION_DEFAULT = TextFieldMissingAction.ERROR;
  public static final String RECORD_SEPARATOR_IF_NULL_KEY = KEY_PREFIX + "recordSeparatorIfNull";
  static final boolean RECORD_SEPARATOR_IF_NULL_DEFAULT = false;
  public static final String RECORD_SEPARATOR_KEY = KEY_PREFIX + "recordSeparator";
  public static final String RECORD_SEPARATOR_DEFAULT = "\\n";

  public static final Map<String, Object> CONFIGS;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(FIELD_PATH_KEY, FIELD_PATH_DEFAULT);
    configs.put(RECORD_SEPARATOR_IF_NULL_KEY, RECORD_SEPARATOR_IF_NULL_DEFAULT);
    configs.put(RECORD_SEPARATOR_KEY, RECORD_SEPARATOR_DEFAULT);
    configs.put(MISSING_FIELD_ACTION_KEY, MISSING_FIELD_ACTION_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }


  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of();

  private final String fieldPath;
  private final boolean recordSeparatorIfNullDefault;
  private final String recordSeparator;
  private final TextFieldMissingAction missingAction;

  public TextDataGeneratorFactory(Settings settings) {
    super(settings);
    fieldPath = settings.getConfig(FIELD_PATH_KEY);
    recordSeparatorIfNullDefault = settings.getConfig(RECORD_SEPARATOR_IF_NULL_KEY);
    recordSeparator = StringEscapeUtils.unescapeJava(settings.<String>getConfig(RECORD_SEPARATOR_KEY));
    missingAction = settings.getConfig(MISSING_FIELD_ACTION_KEY);
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    return new TextCharDataGenerator(
      createWriter(os),
      fieldPath,
      recordSeparatorIfNullDefault,
      recordSeparator,
      missingAction
    );
  }

}
