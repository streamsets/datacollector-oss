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

import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.common.DataFormatConstants;
import com.streamsets.pipeline.config.Compression;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataFactoryBuilder<B extends DataFactoryBuilder, DF extends DataFactory, F extends DataFormat<DF>> {

  private final ProtoConfigurableEntity.Context context;
  private final F format;
  private final Set<Class<? extends Enum>> expectedModes;
  private final Map<Class<? extends Enum>, Enum> modes;
  private final Map<String, Object> configs;
  private Compression compression = Compression.NONE;
  private Charset charset = DataFormatConstants.UTF8;
  private boolean removeCtrlChars;
  private int maxDataLen;
  private int overRunLimit = DataFormatConstants.MAX_OVERRUN_LIMIT;
  private String filePatternInArchive = DataFormatConstants.FILE_PATTERN_IN_ARCHIVE;
  private int stringBuilderPoolSize = DataFormatConstants.STRING_BUILDER_POOL_SIZE;

  public DataFactoryBuilder(ProtoConfigurableEntity.Context context, F format) {
    this.context = Utils.checkNotNull(context, "context");
    this.format = Utils.checkNotNull(format, "format");
    modes = new HashMap<>();
    try {
      expectedModes = format.getModes();
      configs = new HashMap<>(format.getConfigs());
    } catch (Exception ex) {
      throw new RuntimeException(Utils.format("Unexpected exception: {}", ex.toString()), ex);
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
    if(value != null) {
      if (List.class.isAssignableFrom(value.getClass())) {
        Utils.checkArgument(
          List.class.isAssignableFrom(configs.get(key).getClass()),
          Utils.format("Supplied list for config {} when {} expected", key, configs.get(key).getClass().getSimpleName())
        );
      } else {
        Utils.checkArgument(configs.get(key).getClass().isAssignableFrom(value.getClass()),
          Utils.formatL("Format '{}', configuration '{}' must be of type '{}'", format, key,
            configs.get(key).getClass().getSimpleName()));
      }
    }

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

  public B setFilePatternInArchive(String compressionFilePattern) {
    Utils.checkNotNull(compressionFilePattern, "filePatternInArchive");
    this.filePatternInArchive = compressionFilePattern;
    return (B) this;
  }

  public B setCharset(Charset charset) {
    Utils.checkNotNull(charset, "charset");
    this.charset = charset;
    return (B) this;
  }

  public B setRemoveCtrlChars(boolean removeCtrlChars) {
    Utils.checkNotNull(removeCtrlChars, "removeCtrlChars");
    this.removeCtrlChars = removeCtrlChars;
    return (B) this;
  }

  public B setMaxDataLen(int maxDataLen) {
    Utils.checkArgument(maxDataLen > 0 || maxDataLen == -1, Utils.formatL(
        "maxDataLen '{}' cannot be zero, use -1 to disable it", maxDataLen));
    this.maxDataLen = maxDataLen;
    return (B) this;
  }

  public B setStringBuilderPoolSize(int stringBuilderPoolSize) {
    Utils.checkArgument(stringBuilderPoolSize > 0, Utils.formatL(
      "stringBuilderPoolSize '{}' cannot be less than 1", stringBuilderPoolSize));
    this.stringBuilderPoolSize = stringBuilderPoolSize;
    return (B) this;
  }

  public B setOverRunLimit(int overRunLimit) {
    Utils.checkArgument(overRunLimit > 0 && overRunLimit <= DataFormatConstants.MAX_OVERRUN_LIMIT, Utils.formatL(
      "overRunLimit '{}' must be greater than 0 and less than or equal to " + DataFormatConstants.MAX_OVERRUN_LIMIT, overRunLimit));
    this.overRunLimit = overRunLimit;
    return (B) this;
  }

  public DF build() {
    Utils.checkState(modes.size() == expectedModes.size(),
                     Utils.formatL("Format '{}', all required modes have not been set", format));
    DataFactory.Settings settings = new DataFactory.Settings(context, format, compression, filePatternInArchive,
        charset, maxDataLen, modes, configs, overRunLimit, removeCtrlChars, stringBuilderPoolSize);
    return format.create(settings);
  }


}
