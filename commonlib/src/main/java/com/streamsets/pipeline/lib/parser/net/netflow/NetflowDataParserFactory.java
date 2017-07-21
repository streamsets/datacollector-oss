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
package com.streamsets.pipeline.lib.parser.net.netflow;

import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ext.io.OverrunReader;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.parser.DataParser;
import com.streamsets.pipeline.lib.parser.DataParserException;
import com.streamsets.pipeline.lib.parser.DataParserFactory;
import org.apache.commons.lang.StringUtils;

import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NetflowDataParserFactory extends DataParserFactory {
  private static final String KEY_PREFIX = "netflow.";
  public static final String MAX_TEMPLATE_CACHE_SIZE_KEY = KEY_PREFIX + "maxTemplateCacheSize";
  public static final String TEMPLATE_CACHE_TIMEOUT_MS_KEY = KEY_PREFIX + "templateCacheTimeoutMs";
  public static final String OUTPUT_VALUES_MODE_KEY = KEY_PREFIX + "outputValuesMode";

  public static final String DEFAULT_MAX_TEMPLATE_CACHE_SIZE_STR = "-1";
  public static final String DEFAULT_TEMPLATE_CACHE_TIMEOUT_MS_STR = "-1";
  public static final String DEFAULT_OUTPUT_VALUES_MODE_STR = "RAW_AND_INTERPRETED";

  public static final int DEFAULT_MAX_TEMPLATE_CACHE_SIZE = Integer.parseInt(DEFAULT_MAX_TEMPLATE_CACHE_SIZE_STR);
  public static final int DEFAULT_TEMPLATE_CACHE_TIMEOUT_MS = Integer.parseInt(DEFAULT_TEMPLATE_CACHE_TIMEOUT_MS_STR);
  public static final OutputValuesMode DEFAULT_OUTPUT_VALUES_MODE = OutputValuesMode.valueOf(
      DEFAULT_OUTPUT_VALUES_MODE_STR
  );

  public static final String OUTPUT_VALUES_MODE_LABEL = "Output Values Mode";
  public static final String OUTPUT_VALUES_MODE_TOOLTIP = "Controls whether to store raw and/or interpreted values in" +
      " the record generated from a flow. Only applies to Netflow v9.";

  public static final String MAX_TEMPLATE_CACHE_SIZE_LABEL = "Max Template Cache Size";
  public static final String MAX_TEMPLATE_CACHE_SIZE_TOOLTIP = "Controls the maximum size of the template cache (i.e." +
      " the number of flow templates to keep cached across all export packets). Leave as -1 for unlimited. Only" +
      " applies to Netflow v9.";

  public static final String TEMPLATE_CACHE_TIMEOUT_MS_LABEL = "Template Cache Timeout (ms)";
  public static final String TEMPLATE_CACHE_TIMEOUT_MS_TOOLTIP = "Controls the maximum length of time flow" +
      " templates are cached, after last being used to parse a data flow. Leave as -1 for unlimited (never expires)." +
      " Only applies to Netflow v9.";

  public static final Map<String, Object> CONFIGS;

  public static final Set<Class<? extends Enum>> MODES = ImmutableSet.of();

  static {
    Map<String, Object> configs = new HashMap<>();
    configs.put(MAX_TEMPLATE_CACHE_SIZE_KEY, DEFAULT_MAX_TEMPLATE_CACHE_SIZE);
    configs.put(TEMPLATE_CACHE_TIMEOUT_MS_KEY, DEFAULT_TEMPLATE_CACHE_TIMEOUT_MS);
    configs.put(OUTPUT_VALUES_MODE_KEY, DEFAULT_OUTPUT_VALUES_MODE);
    CONFIGS = Collections.unmodifiableMap(configs);
  }

  public NetflowDataParserFactory(Settings settings) {
    super(settings);
  }

  @Override
  public DataParser getParser(String id, InputStream is, String offset) throws DataParserException {
    return new NetflowDataParser(
        getSettings().getContext(),
        id,
        is,
        StringUtils.isNumeric(offset) ? Long.parseLong(offset) : null,
        getSettings().getMaxRecordLen(),
        getSettings().getCharset(),
        (OutputValuesMode) CONFIGS.get(OUTPUT_VALUES_MODE_KEY),
        (int) CONFIGS.get(MAX_TEMPLATE_CACHE_SIZE_KEY),
        (int) CONFIGS.get(TEMPLATE_CACHE_TIMEOUT_MS_KEY)
    );
  }

  @Override
  public DataParser getParser(String id, Reader reader, long offset) throws DataParserException {
    throw new UnsupportedOperationException("NetflowDataParserFactory does not support character-based input");
  }

  public static void validateConfigs(
      Stage.Context context,
      List<Stage.ConfigIssue> issues,
      String group,
      String configPrefix,
      int maxTemplateCacheSize,
      int templateCacheTimeoutMs
  ) {
    validateConfigs(
        context,
        issues,
        group,
        configPrefix,
        maxTemplateCacheSize,
        templateCacheTimeoutMs,
        "maxTemplateCacheSize",
        "templateCacheTimeoutMs"
    );
  }

  public static void validateConfigs(
      Stage.Context context,
      List<Stage.ConfigIssue> issues,
      String group,
      String configPrefix,
      int maxTemplateCacheSize,
      int templateCacheTimeoutMs,
      String maxTemplateCacheSizeField,
      String templateCacheTimeoutMsField
  ) {
    if (maxTemplateCacheSize < -1 || maxTemplateCacheSize == 0) {
      issues.add(context.createConfigIssue(
          group,
          configPrefix + maxTemplateCacheSizeField,
          Errors.NETFLOW_15
      ));
    }
    if (templateCacheTimeoutMs < -1 || templateCacheTimeoutMs == 0) {
      issues.add(context.createConfigIssue(
          group,
          configPrefix + templateCacheTimeoutMsField,
          Errors.NETFLOW_16
      ));
    }
  }
}
