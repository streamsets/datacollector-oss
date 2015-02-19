/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.ChooserMode;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldSelector;
import com.streamsets.pipeline.api.ValueChooser;

import java.util.List;

public class FieldTypeConverterConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Fields to Convert",
      description = "You can convert multiple fields to the same type",
      displayPosition = 10
  )
  @FieldSelector
  public List<String> fields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="INTEGER",
      label = "Convert to Type",
      description = "Select a compatible data type",
      displayPosition = 10
  )
  @ValueChooser(chooserValues = FieldTypeChooserValues.class, type = ChooserMode.PROVIDED)
  public Field.Type targetType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ENGLISH",
      label = "Data Locale",
      description = "Affects the interpretation of locale sensitive data, such as using the comma as a decimal " +
                    "separator",
      displayPosition = 20,
      dependsOn = "targetType",
      triggeredByValue = {"BYTE", "INTEGER", "LONG", "DOUBLE", "DECIMAL", "FLOAT", "SHORT"}
  )
  @ValueChooser(chooserValues = DataLocaleChooserValues.class, type = ChooserMode.PROVIDED)
  public DataLocale dataLocale;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="YYYY_MM_DD",
      label = "Date Format",
      description="Select or enter any valid date or datetime format",
      displayPosition = 30,
      dependsOn = "targetType",
      triggeredByValue = {"DATE", "DATETIME"}
  )
  @ValueChooser(chooserValues = DateFormatChooserValues.class, type = ChooserMode.SUGGESTED)
  public String dateFormat;

}
