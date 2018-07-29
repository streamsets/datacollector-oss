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
package com.streamsets.pipeline.lib.data;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.config.Compression;

import java.nio.charset.Charset;
import java.util.Map;

public abstract class DataFactory {

  public static class Settings {
    private final ProtoConfigurableEntity.Context context;
    private final DataFormat format;
    private final Compression compression;
    private final String filePatternInArchive;
    private final Charset charset;
    private final boolean removeCtrlChars;
    private final int maxRecordLen;
    private final Map<Class<? extends Enum>, Enum> modes;
    private final Map<String, Object> configs;
    private final int overRunLimit;
    private final int stringBuilderPoolSize;

    Settings(
        ProtoConfigurableEntity.Context context,
        DataFormat format,
        Compression compression,
        String filePatternInArchive,
        Charset charset,
        int maxRecordLen,
        Map<Class<? extends Enum>, Enum> modes,
        Map<String, Object> configs,
        int overRunLimit,
        boolean removeCtrlChars,
        int stringBuilderPoolSize
    ) {
      this.context = context;
      this.format = format;
      this.compression = compression;
      this.filePatternInArchive = filePatternInArchive;
      this.charset = charset;
      this.removeCtrlChars = removeCtrlChars;
      this.maxRecordLen = maxRecordLen;
      this.modes = ImmutableMap.copyOf(modes);
      this.configs = ImmutableMap.copyOf(configs);
      this.overRunLimit = overRunLimit;
      this.stringBuilderPoolSize = stringBuilderPoolSize;
    }

    public ProtoConfigurableEntity.Context getContext() {
      return context;
    }

    public DataFormat getFormat() {
      return format;
    }

    public Compression getCompression() {
      return compression;
    }

    public String getFilePatternInArchive() {
      return filePatternInArchive;
    }

    public Charset getCharset() {
      return charset;
    }

    public int getMaxRecordLen() {
      return maxRecordLen;
    }

    public boolean getRemoveCtrlChars() {
      return removeCtrlChars;
    }

    public int getStringBuilderPoolSize() {
      return stringBuilderPoolSize;
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

  public Settings getSettings() {
    return settings;
  }

}
