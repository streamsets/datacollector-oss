/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.generator;


import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.config.LogMode;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedCharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.json.JsonCharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.sdcrecord.JsonSdcRecordCharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.text.TextCharDataGeneratorFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class CharDataGeneratorFactory {

  public DataGenerator getGenerator(File file) throws IOException, DataGeneratorException {
    FileWriter fileWriter;
    try {
      fileWriter = new FileWriter(file);
    } catch (IOException ex) {
      throw new DataGeneratorException(Errors.DATA_GENERATOR_00, file.getAbsolutePath(), ex.getMessage(), ex);
    }
    try {
      return getGenerator(fileWriter);
    } catch (DataGeneratorException ex) {
      try {
        fileWriter.close();
      } catch (IOException ioEx) {
        //NOP
      }
      throw ex;
    }
  }

  public abstract DataGenerator getGenerator(Writer writer) throws IOException, DataGeneratorException;


  public enum Format {
    TEXT(),
    JSON(JsonMode.class),
    XML(),
    DELIMITED(CsvMode.class, CsvHeader.class),
    SDC_RECORD(),
    LOG(LogMode.class),

    ;

    private final Class<? extends Enum>[] modes;

    Format(Class<? extends Enum> ... modes) {
      this.modes = modes;
    }

    public Class<? extends Enum>[] getModes() {
      return modes;
    }

  }

  public static class Builder {
    private static final Map<String, Object> DEFAULT_CONFIGS;

    static {
      Map<String, Object> defaultConfigs = new HashMap<>();
      DelimitedCharDataGeneratorFactory.registerConfigs(defaultConfigs);
      JsonCharDataGeneratorFactory.registerConfigs(defaultConfigs);
      TextCharDataGeneratorFactory.registerConfigs(defaultConfigs);
      JsonSdcRecordCharDataGeneratorFactory.registerConfigs(defaultConfigs);
      DEFAULT_CONFIGS = Collections.unmodifiableMap(defaultConfigs);
    }

    private Format format;
    private Map<Class<? extends Enum>, Enum> modes;
    private Stage.Context context;
    private Map<String, Object> configs;

    public Builder(Stage.Context context, Format format) {
      Utils.checkNotNull(context, "context");
      this.format = Utils.checkNotNull(format, "format");
      this.context = context;
      modes = new HashMap<>();
      configs = new HashMap<>(DEFAULT_CONFIGS);
    }

    public Builder setMode(Enum mode) {
      Utils.checkNotNull(mode, "mode");
      Utils.checkState(format.getModes() != null, Utils.formatL("Format '{}' does not support any mode", format));
      boolean supported = false;
      for (Class<? extends Enum> eMode : format.getModes()) {
        if (eMode.isInstance(mode)) {
          supported = true;
          break;
        }
      }
      Utils.checkState(supported, Utils.formatL("Format '{}' does not support '{}' mode", format,
                                                mode.getClass().getSimpleName()));
      modes.put(mode.getClass(), mode);
      return this;
    }

    public Builder setConfig(String key, Object value) {
      Utils.checkNotNull(key, "key");
      Utils.checkNotNull(value, "value");
      Utils.checkArgument(DEFAULT_CONFIGS.containsKey(key), Utils.formatL("Unsupported configuration '{}'", key));
      Utils.checkArgument(DEFAULT_CONFIGS.get(key).getClass() == value.getClass(),
                          Utils.formatL("Configuration '{}' must be of type '{}'", key,
                                        DEFAULT_CONFIGS.get(key).getClass().getSimpleName()));
      configs.put(key, value);
      return this;
    }

    public CharDataGeneratorFactory build() {
      Utils.checkState(modes.size() == format.getModes().length, "All required modes have not been set");
      return build(context, format, modes, configs);
    }

    CharDataGeneratorFactory build(Stage.Context context, Format format, Map<Class<? extends Enum>, Enum> modes,
        Map<String, Object> configs) {
      CharDataGeneratorFactory factory;
      switch (format) {
        case TEXT:
          factory = new TextCharDataGeneratorFactory(context, configs);
          break;
        case JSON:
          JsonMode jsonMode = (JsonMode) modes.get(JsonMode.class);
          factory = new JsonCharDataGeneratorFactory(context, jsonMode, configs);
          break;
        case XML:
          throw new IllegalArgumentException("Not yet implemented XML");
        case DELIMITED:
          CsvMode csvMode = (CsvMode) modes.get(CsvMode.class);
          CsvHeader csvHeader = (CsvHeader) modes.get(CsvHeader.class);
          factory = new DelimitedCharDataGeneratorFactory(context, csvMode.getFormat(), csvHeader, configs);
          break;
        case SDC_RECORD:
          factory = new JsonSdcRecordCharDataGeneratorFactory(context, configs);
          break;
        case LOG:
          throw new IllegalArgumentException("Not yet implemented LOG");
        default:
          throw new IllegalStateException(Utils.format("Unsupported format '{}'", format));
      }
      return factory;
    }
  }

}
