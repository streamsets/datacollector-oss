/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DateFormat;
import com.streamsets.pipeline.config.LocaleChooserValues;
import com.streamsets.pipeline.config.DateFormatChooserValues;
import com.streamsets.pipeline.config.PrimitiveFieldTypeChooserValues;

import java.util.List;
import java.util.Locale;

public class FieldTypeConverterConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="",
      label = "Fields to Convert",
      description = "You can convert multiple fields to the same type",
      displayPosition = 10
  )
  @FieldSelectorModel
  public List<String> fields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="INTEGER",
      label = "Convert to Type",
      description = "Select a compatible data type",
      displayPosition = 10
  )
  @ValueChooserModel(PrimitiveFieldTypeChooserValues.class)
  public Field.Type targetType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "en,US",
      label = "Data Locale",
      description = "Affects the interpretation of locale sensitive data, such as using the comma as a decimal " +
                    "separator",
      displayPosition = 20,
      dependsOn = "targetType",
      triggeredByValue = {"BYTE", "INTEGER", "LONG", "DOUBLE", "DECIMAL", "FLOAT", "SHORT"}
  )
  @ValueChooserModel(LocaleChooserValues.class)
  public String dataLocale;

  private Locale locale;

  public Locale getLocale() {
    if (locale == null) {
      locale = LocaleChooserValues.getLocale(dataLocale);
    }
    return locale;
  }

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
  @ValueChooserModel(DateFormatChooserValues.class)
  public DateFormat dateFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Other Date Format",
      displayPosition = 40,
      dependsOn = "dateFormat",
      triggeredByValue = "OTHER"
  )
  public String otherDateFormat;

}
