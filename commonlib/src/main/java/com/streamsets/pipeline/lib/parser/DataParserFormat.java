/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.data.DataFormat;
import com.streamsets.pipeline.lib.parser.delimited.DelimitedCharDataParserFactory;
import com.streamsets.pipeline.lib.parser.json.JsonCharDataParserFactory;
import com.streamsets.pipeline.lib.parser.log.LogCharDataParserFactory;
import com.streamsets.pipeline.lib.parser.sdcrecord.JsonSdcRecordCharDataParserFactory;
import com.streamsets.pipeline.lib.parser.text.TextCharDataParserFactory;
import com.streamsets.pipeline.lib.parser.xml.XmlCharDataParserFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

public enum DataParserFormat implements DataFormat<CharDataParserFactory> {
  TEXT(TextCharDataParserFactory.class, TextCharDataParserFactory.MODES, TextCharDataParserFactory.CONFIGS),
  JSON(JsonCharDataParserFactory.class, JsonCharDataParserFactory.MODES, JsonCharDataParserFactory.CONFIGS),
  XML(XmlCharDataParserFactory.class, XmlCharDataParserFactory.MODES, XmlCharDataParserFactory.CONFIGS),
  DELIMITED(DelimitedCharDataParserFactory.class, DelimitedCharDataParserFactory.MODES, DelimitedCharDataParserFactory.CONFIGS),
  SDC_RECORD(JsonSdcRecordCharDataParserFactory.class, JsonSdcRecordCharDataParserFactory.MODES, JsonSdcRecordCharDataParserFactory.CONFIGS),
  LOG(LogCharDataParserFactory.class, LogCharDataParserFactory.MODES, LogCharDataParserFactory.CONFIGS),

  ;

  private final Class<? extends CharDataParserFactory> klass;
  private final Constructor<? extends CharDataParserFactory> constructor;
  private final Set<Class<? extends Enum>> modes;
  private Map<String, Object> configs;

  DataParserFormat(Class<? extends CharDataParserFactory> klass, Set<Class<? extends Enum>> modes,
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
  public CharDataParserFactory create(DataFactory.Settings settings) {
    try {
      return constructor.newInstance(settings);
    } catch (Exception ex) {
      throw new RuntimeException(Utils.format("Could not create DataFactory instance for '{}': {}",
                                              klass.getName(), ex.getMessage(), ex));
    }
  }


}
