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

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;
import org.glassfish.jersey.logging.LoggingFeature;

public class RequestLoggingConfigBean {

  private static final int BASE_DISPLAY_POSITION = 200000;
  public static final String LOGGING_ENABLED_FIELD_NAME = "enableRequestLogging";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Enable Request Logging",
      description = "Enable logging of HTTP request and response data.",
      defaultValue = "false",
      displayPosition = BASE_DISPLAY_POSITION + 0,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0"
  )
  public boolean enableRequestLogging;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Log Level",
      description = "The log level to use when logging messages (from java.util.logging package).",
      defaultValue = JulLogLevelChooserValues.DEFAULT_LEVEL,
      displayPosition = BASE_DISPLAY_POSITION + 10,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      dependsOn = LOGGING_ENABLED_FIELD_NAME,
      triggeredByValue = "true"
  )
  @ValueChooserModel(JulLogLevelChooserValues.class)
  public String logLevel = JulLogLevelChooserValues.DEFAULT_LEVEL;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Verbosity",
      description = "The verbosity to use when logging messages (i.e. which type of request/response data).",
      defaultValue = VerbosityChooserValues.DEFAULT_VERBOSITY_STR,
      displayPosition = BASE_DISPLAY_POSITION + 20,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      dependsOn = LOGGING_ENABLED_FIELD_NAME,
      triggeredByValue = "true"
  )
  @ValueChooserModel(VerbosityChooserValues.class)
  public LoggingFeature.Verbosity verbosity = VerbosityChooserValues.DEFAULT_VERBOSITY;

  public static final String DEFAULT_MAX_ENTITY_SIZE_STR = "0";
  public static final int DEFAULT_MAX_ENTITY_SIZE = 0;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.NUMBER,
      label = "Max Entity Size",
      description = "The maximum entity size to log (in bytes). Entities larger than this value will not be logged.",
      defaultValue = DEFAULT_MAX_ENTITY_SIZE_STR,
      min = DEFAULT_MAX_ENTITY_SIZE,
      displayPosition = BASE_DISPLAY_POSITION + 30,
      displayMode = ConfigDef.DisplayMode.ADVANCED,
      group = "#0",
      dependsOn = LOGGING_ENABLED_FIELD_NAME,
      triggeredByValue = "true"
  )
  @ValueChooserModel(JulLogLevelChooserValues.class)
  public int maxEntitySize = DEFAULT_MAX_ENTITY_SIZE;

}
