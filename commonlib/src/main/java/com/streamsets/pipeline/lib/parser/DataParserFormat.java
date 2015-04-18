/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.data.DataFactory;
import com.streamsets.pipeline.lib.data.DataFormat;
import com.streamsets.pipeline.lib.parser.delimited.DelimitedDataParserFactory;
import com.streamsets.pipeline.lib.parser.json.JsonDataParserFactory;
import com.streamsets.pipeline.lib.parser.log.LogDataParserFactory;
import com.streamsets.pipeline.lib.parser.sdcrecord.SdcRecordDataParserFactory;
import com.streamsets.pipeline.lib.parser.text.TextDataParserFactory;
import com.streamsets.pipeline.lib.parser.xml.XmlDataParserFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

public enum DataParserFormat implements DataFormat<DataParserFactory> {
  TEXT(TextDataParserFactory.class, TextDataParserFactory.MODES, TextDataParserFactory.CONFIGS),
  JSON(JsonDataParserFactory.class, JsonDataParserFactory.MODES, JsonDataParserFactory.CONFIGS),
  XML(XmlDataParserFactory.class, XmlDataParserFactory.MODES, XmlDataParserFactory.CONFIGS),
  DELIMITED(DelimitedDataParserFactory.class, DelimitedDataParserFactory.MODES, DelimitedDataParserFactory.CONFIGS),
  SDC_RECORD(SdcRecordDataParserFactory.class, SdcRecordDataParserFactory.MODES, SdcRecordDataParserFactory.CONFIGS),
  LOG(LogDataParserFactory.class, LogDataParserFactory.MODES, LogDataParserFactory.CONFIGS),

  ;

  private final Class<? extends DataParserFactory> klass;
  private final Constructor<? extends DataParserFactory> constructor;
  private final Set<Class<? extends Enum>> modes;
  private Map<String, Object> configs;

  DataParserFormat(Class<? extends DataParserFactory> klass, Set<Class<? extends Enum>> modes,
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
  public DataParserFactory create(DataFactory.Settings settings) {
    try {
      return constructor.newInstance(settings);
    } catch (Exception ex) {
      throw new RuntimeException(Utils.format("Could not create DataFactory instance for '{}': {}",
                                              klass.getName(), ex.getMessage(), ex));
    }
  }


}
