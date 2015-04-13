/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.data.DataFormat;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedCharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.json.JsonCharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.sdcrecord.JsonSdcRecordCharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.text.TextCharDataGeneratorFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

public enum DataGeneratorFormat implements DataFormat<CharDataGeneratorFactory> {
  TEXT(TextCharDataGeneratorFactory.class, TextCharDataGeneratorFactory.MODES, TextCharDataGeneratorFactory.CONFIGS),
  JSON(JsonCharDataGeneratorFactory.class, JsonCharDataGeneratorFactory.MODES, JsonCharDataGeneratorFactory.CONFIGS),
  DELIMITED(DelimitedCharDataGeneratorFactory.class, DelimitedCharDataGeneratorFactory.MODES,
    DelimitedCharDataGeneratorFactory.CONFIGS),
  SDC_RECORD(JsonSdcRecordCharDataGeneratorFactory.class, JsonSdcRecordCharDataGeneratorFactory.MODES,
    JsonSdcRecordCharDataGeneratorFactory.CONFIGS),
  ;

  private final Class<? extends CharDataGeneratorFactory> klass;
  private final Constructor<? extends CharDataGeneratorFactory> constructor;
  private final Set<Class<? extends Enum>> modes;
  private Map<String, Object> configs;

  DataGeneratorFormat(Class<? extends CharDataGeneratorFactory> klass, Set<Class<? extends Enum>> modes,
                   Map<String, Object> configs) {
    this.klass = klass;
    try {
      constructor = klass.getConstructor(DataFactory.Settings.class);
      Utils.checkState((constructor.getModifiers() & Modifier.PUBLIC) != 0,
        Utils.formatL("Constructor for DataFactory '{}' must be public",
          klass.getName()));
    } catch (Exception ex) {
      throw new RuntimeException(Utils.format("Could not obtain constructor '<init>({})' for DataFactory '{}': {}",
        DataFactory.Settings.class, klass.getName(), ex.getMessage()), ex);
    }
    this.modes = modes;
    this.configs = configs;

  }

  @Override
  public Set<Class<? extends Enum>> getModes() {
    return modes;
  }

  @Override
  public Map<String, Object> getConfigs() {
    return configs;
  }

  @Override
  public CharDataGeneratorFactory create(DataFactory.Settings settings) {
    try {
      return constructor.newInstance(settings);
    } catch (Exception ex) {
      throw new RuntimeException(Utils.format("Could not create DataFactory instance for '{}': {}",
        klass.getName(), ex.getMessage(), ex));
    }
  }
}
