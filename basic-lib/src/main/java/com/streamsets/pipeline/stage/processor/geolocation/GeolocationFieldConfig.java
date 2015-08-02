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
