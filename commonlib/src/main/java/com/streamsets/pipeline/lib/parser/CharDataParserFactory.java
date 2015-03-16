/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.parser;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.io.OverrunReader;
import com.streamsets.pipeline.lib.parser.delimited.DelimitedCharDataParserFactory;
import com.streamsets.pipeline.lib.parser.json.JsonCharDataParserFactory;
import com.streamsets.pipeline.lib.parser.sdcrecord.JsonSdcRecordCharDataParserFactory;
import com.streamsets.pipeline.lib.parser.text.TextCharDataParserFactory;
import com.streamsets.pipeline.lib.parser.xml.XmlCharDataParserFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class CharDataParserFactory {

  public DataParser getParser(String id, String data) throws DataParserException {
    return getParser(id, new OverrunReader(new StringReader(data), 0, false), 0);
  }

  public DataParser getParser(File file, Charset charset, int overrunLimit, long fileOffset) throws DataParserException {
    Reader fileReader;
    try {
      InputStream fis = new FileInputStream(file);
      fileReader = new InputStreamReader(fis, charset);
    } catch (IOException ex) {
      throw new DataParserException(Errors.DATA_PARSER_00, file.getAbsolutePath(), ex.getMessage(), ex);
    }
    try {
      OverrunReader reader = new OverrunReader(fileReader, overrunLimit, false);
      return getParser(file.getName(), reader, fileOffset);
    } catch (DataParserException ex) {
      try {
        fileReader.close();
      } catch (IOException ioEx) {
        //NOP
      }
      throw ex;
    }
  }

  // the reader must be in position zero, the getParser() call will fast forward while initializing the parser
  public abstract DataParser getParser(String id, OverrunReader reader, long readerOffset) throws DataParserException;


  public enum Format {
    TEXT(),
    JSON(JsonMode.class),
    XML(),
    DELIMITED(CsvMode.class, CsvHeader.class),
    SDC_RECORD(),
    LOG(),

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
      TextCharDataParserFactory.registerConfigs(defaultConfigs);
      JsonCharDataParserFactory.registerConfigs(defaultConfigs);
      XmlCharDataParserFactory.registerConfigs(defaultConfigs);
      DelimitedCharDataParserFactory.registerConfigs(defaultConfigs);
      JsonSdcRecordCharDataParserFactory.registerConfigs(defaultConfigs);
      DEFAULT_CONFIGS = Collections.unmodifiableMap(defaultConfigs);
    }

    private Format format;
    private Map<Class<? extends Enum>, Enum> modes;
    private Stage.Context context;
    private int maxDataLen;
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
      Utils.checkArgument(DEFAULT_CONFIGS.containsKey(key), Utils.formatL("Unsupported configuration '{}'", key));
      Utils.checkArgument(value == null || DEFAULT_CONFIGS.get(key).getClass() == value.getClass(),
                          Utils.formatL("Configuration '{}' must be of type '{}'", key,
                                        DEFAULT_CONFIGS.get(key).getClass().getSimpleName()));
      if (value == null) {
        value = DEFAULT_CONFIGS.get(key);
      }
      configs.put(key, value);
      return this;
    }

    public Builder setMaxDataLen(int maxDataLen) {
      Utils.checkArgument(maxDataLen > 0 || maxDataLen == -1, Utils.formatL(
          "maxDataLen '{}' cannot be zero, use -1 to disable it", maxDataLen));
      this.maxDataLen = maxDataLen;
      return this;
    }

    public CharDataParserFactory build() {
      Utils.checkState(modes.size() == format.getModes().length, "All required modes have not been set");
      Utils.checkState(maxDataLen != 0, "maxDataLen has not been set");
      return build(context, format, modes, maxDataLen, configs);
    }

    CharDataParserFactory build(Stage.Context context, Format format, Map<Class<? extends Enum>, Enum> modes,
        int maxDataLen, Map<String, Object> configs) {
      CharDataParserFactory factory;
      switch (format) {
        case TEXT:
          factory = new TextCharDataParserFactory(context, maxDataLen, configs);
          break;
        case JSON:
          JsonMode jsonMode = (JsonMode) modes.get(JsonMode.class);
          factory = new JsonCharDataParserFactory(context, maxDataLen, jsonMode.getFormat(), configs);
          break;
        case XML:
          factory = new XmlCharDataParserFactory(context, maxDataLen, configs);
          break;
        case DELIMITED:
          CsvMode csvMode = (CsvMode) modes.get(CsvMode.class);
          CsvHeader csvHeader = (CsvHeader) modes.get(CsvHeader.class);
          factory = new DelimitedCharDataParserFactory(context, csvMode.getFormat(), csvHeader, maxDataLen, configs);
          break;
        case SDC_RECORD:
          factory = new JsonSdcRecordCharDataParserFactory(context, maxDataLen, configs);
          break;
        case LOG:
          throw new IllegalArgumentException("LOG not yet implemented");
        default:
          throw new IllegalStateException(Utils.format("Unsupported format '{}'", format));
      }
      return factory;
    }
  }

}
