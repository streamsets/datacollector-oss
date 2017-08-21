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
package com.streamsets.pipeline.stage.processor.geolocation;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooserModel;

public class GeolocationFieldConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="",
      label = "Input Field Name",
      description = "Use an integer or string field with IP address data in the following format: n.n.n.n",
      displayPosition = 10
  )
  public String inputFieldName;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue="",
    label = "Output Field Name",
    description = "Examples: /<field name>",
    displayPosition = 10
  )
  public String outputFieldName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="COUNTRY_NAME",
      label = "GeoIP2 Field",
      description = "",
      displayPosition = 10
  )
  @ValueChooserModel(GeolocationFieldEnumChooserValues.class)
  public GeolocationField targetType;

  @Override
  public String toString() {
    return "GeolocationFieldConfig{" +
      "inputFieldName='" + inputFieldName + '\'' +
      ", outputFieldName='" + outputFieldName + '\'' +
      ", targetType=" + targetType +
      '}';
  }
}
