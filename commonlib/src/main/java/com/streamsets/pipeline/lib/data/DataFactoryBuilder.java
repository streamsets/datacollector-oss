/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.data;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.impl.Utils;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataFactoryBuilder<B extends DataFactoryBuilder, DF extends DataFactory, F extends DataFormat<DF>> {
  private final static Charset UTF8 = Charset.forName("UTF-8");
  private static final int MAX_OVERRUN_LIMIT = Integer.parseInt(
    System.getProperty("DataFactoryBuilder.OverRunLimit", "1000000"));

  private final Stage.Context context;
  private final F format;
  private final Set<Class<? extends Enum>> expectedModes;
  private final Map<Class<? extends Enum>, Enum> modes;
  private final Map<String, Object> configs;
  private Compression compression = Compression.NONE;
  private Charset charset = UTF8;
  private int maxDataLen;
  private int overRunLimit = MAX_OVERRUN_LIMIT;

  public DataFactoryBuilder(Stage.Context context, F format) {
    this.context = Utils.checkNotNull(context, "context");
    this.format = Utils.checkNotNull(format, "format");
    modes = new HashMap<>();
    try {
      expectedModes = format.getModes();
      configs = new HashMap<>(format.getConfigs());
    } catch (Exception ex) {
      throw new RuntimeException("It should not happen", ex);
    }
  }

  public B setMode(Enum mode) {
    Utils.checkNotNull(mode, "mode");
    Utils.checkArgument(expectedModes.contains(mode.getClass()), Utils.formatL("Format '{}' does not support mode '{}'",
                                                                               format, mode.getClass()));
    modes.put(mode.getClass(), mode);
    return (B) this;
  }

  public B setConfig(String key, Object value) {
    Utils.checkNotNull(key, "key");
    Utils.checkArgument(configs.containsKey(key),
                        Utils.formatL("Format '{}', unsupported configuration '{}'", format, key));
    Utils.checkArgument(value == null || configs.get(key).getClass() == value.getClass(),
                        Utils.formatL("Format '{}', configuration '{}' must be of type '{}'", format, key,
                                      configs.get(key).getClass().getSimpleName()));
    if (value == null) {
      value = configs.get(key);
    }
    configs.put(key, value);
    return (B) this;
  }

  public B setCompression(Compression compression) {
    Utils.checkNotNull(compression, "compression");
    this.compression = compression;
    return (B) this;
  }

  public B setCharset(Charset charset) {
    Utils.checkNotNull(charset, "charset");
    this.charset = charset;
    return (B) this;
  }

  public B setMaxDataLen(int maxDataLen) {
    Utils.checkArgument(maxDataLen > 0 || maxDataLen == -1, Utils.formatL(
        "maxDataLen '{}' cannot be zero, use -1 to disable it", maxDataLen));
    this.maxDataLen = maxDataLen;
    return (B) this;
  }

  public B setOverRunLimit(int overRunLimit) {
    Utils.checkArgument(overRunLimit > 0 && overRunLimit <= MAX_OVERRUN_LIMIT, Utils.formatL(
      "overRunLimit '{}' must be greater than 0 and less than or equal to " + MAX_OVERRUN_LIMIT, overRunLimit));
    this.overRunLimit = overRunLimit;
    return (B) this;
  }

  public DF build() {
    Utils.checkState(modes.size() == expectedModes.size(),
                     Utils.formatL("Format '{}', all required modes have not been set", format));
    Utils.checkState(maxDataLen != 0, "maxDataLen has not been set");
    DataFactory.Settings settings = new DataFactory.Settings(context, format, compression, charset, maxDataLen, modes,
                                                             configs, overRunLimit);
    return format.create(settings);
  }


}
