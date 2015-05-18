/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.geolocation;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ValueChooser;

public class GeolocationFieldConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue="",
      label = "Input Field Name",
      description = "Must either be an integer or a string in a.b.c.d format",
      displayPosition = 10
  )
  public String inputFieldName;

  @ConfigDef(
    required = true,
    type = ConfigDef.Type.STRING,
    defaultValue="",
    label = "Output Field Name",
    description = "Examples: /srcaddr_country_name, /srcaddr_city_name, /srcaddr_city_name, /srcaddr_latitude",
    displayPosition = 10
  )
  public String outputFieldName;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="COUNTRY",
      label = "Geolocation Field",
      description = "",
      displayPosition = 10
  )
  @ValueChooser(GeolocationFieldEnumChooserValues.class)
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
