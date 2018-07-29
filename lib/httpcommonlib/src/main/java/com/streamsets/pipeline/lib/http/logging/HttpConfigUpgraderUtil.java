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

package com.streamsets.pipeline.lib.http.logging;

import com.streamsets.pipeline.api.Config;

import java.util.List;

public abstract class HttpConfigUpgraderUtil {

  public static void addDefaultRequestLoggingConfigs(List<Config> configs, String pathToJerseyClientConfigBean) {
    final String newConfigBeanPath = String.format("%s.requestLoggingConfig", pathToJerseyClientConfigBean);

    configs.add(new Config(String.format(
        "%s.%s",
        newConfigBeanPath,
        RequestLoggingConfigBean.LOGGING_ENABLED_FIELD_NAME
    ), false));

    configs.add(new Config(String.format(
        "%s.logLevel",
        newConfigBeanPath
    ), JulLogLevelChooserValues.DEFAULT_LEVEL));

    configs.add(new Config(String.format(
        "%s.verbosity",
        newConfigBeanPath
    ), VerbosityChooserValues.DEFAULT_VERBOSITY));

    configs.add(new Config(String.format(
        "%s.maxEntitySize",
        newConfigBeanPath
    ), RequestLoggingConfigBean.DEFAULT_MAX_ENTITY_SIZE));
  }

}
