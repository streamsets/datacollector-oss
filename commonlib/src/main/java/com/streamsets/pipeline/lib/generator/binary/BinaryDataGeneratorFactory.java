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
package com.streamsets.pipeline.lib.generator.binary;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BinaryDataGeneratorFactory extends DataGeneratorFactory {

  static final String KEY_PREFIX = "binary.";
  public static final String FIELD_PATH_KEY = KEY_PREFIX + "fieldPath";
  static final String FIELD_PATH_DEFAULT = "/";

  public static final Map<String, Object> CONFIGS;

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(FIELD_PATH_KEY, FIELD_PATH_DEFAULT);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  @SuppressWarnings("unchecked")
  public static final Set<Class<? extends Enum>> MODES = (Set) ImmutableSet.of();

  private final String fieldPath;

  public BinaryDataGeneratorFactory(Settings settings) {
    super(settings);
    fieldPath = settings.getConfig(FIELD_PATH_KEY);
    Utils.checkNotNull(fieldPath, "Binary Field Path");
  }

  @Override
  public DataGenerator getGenerator(OutputStream os) throws IOException {
    return new BinaryDataGenerator(os, fieldPath);
  }

  @Override
  public Writer createWriter(OutputStream os) {
    throw new UnsupportedOperationException();
  }

}
