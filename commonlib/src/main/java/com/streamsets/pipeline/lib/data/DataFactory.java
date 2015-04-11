/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.data;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Stage;

import java.nio.charset.Charset;
import java.util.Map;

public abstract class DataFactory {

  public static class Settings {
    private final Stage.Context context;
    private final DataFormat format;
    private final Compression compression;
    private final Charset charset;
    private final int maxRecordLen;
    private final Map<Class<? extends Enum>, Enum> modes;
    private final Map<String, Object> configs;
    private final int overRunLimit;

    Settings(Stage.Context context, DataFormat format, Compression compression, Charset charset, int maxRecordLen,
        Map<Class<? extends Enum>, Enum> modes, Map<String, Object> configs, int overRunLimit) {
      this.context = context;
      this.format = format;
      this.compression = compression;
      this.charset = charset;
      this.maxRecordLen = maxRecordLen;
      this.modes = ImmutableMap.copyOf(modes);
      this.configs = ImmutableMap.copyOf(configs);
      this.overRunLimit = overRunLimit;
    }

    public Stage.Context getContext() {
      return context;
    }

    public DataFormat getFormat() {
      return format;
    }

    public Compression getCompression() {
      return compression;
    }

    public Charset getCharset() {
      return charset;
    }

    public int getMaxRecordLen() {
      return maxRecordLen;
    }

    @SuppressWarnings("unchecked")
    public <T extends Enum> T getMode(Class<T> klass) {
      return (T) modes.get(klass);
    }

    @SuppressWarnings("unchecked")
    public <T> T getConfig(String name) {
      return (T) configs.get(name);
    }

    public int getOverRunLimit() {
      return overRunLimit;
    }
  }

  private final Settings settings;

  protected DataFactory(Settings settings) {
    this.settings = settings;
  }

  protected Settings getSettings() {
    return settings;
  }

}
