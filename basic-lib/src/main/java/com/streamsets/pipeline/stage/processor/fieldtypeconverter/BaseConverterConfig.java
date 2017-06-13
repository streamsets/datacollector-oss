/**
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
package com.streamsets.pipeline.stage.processor.fieldtypeconverter;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.CharsetChooserValues;
import com.streamsets.pipeline.config.DateFormat;
import com.streamsets.pipeline.config.DecimalScaleRoundingStrategy;
import com.streamsets.pipeline.config.DecimalScaleRoundingStrategyChooserValues;
import com.streamsets.pipeline.config.LocaleChooserValues;
import com.streamsets.pipeline.config.DateFormatChooserValues;
import com.streamsets.pipeline.config.PrimitiveFieldTypeChooserValues;

import java.util.Locale;

/**
 * Base configuration for target type
 */
public class BaseConverterConfig {

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue="INTEGER",
      label = "Convert to Type",
      description = "Select a compatible data type",
      displayPosition = 20
  )
  @ValueChooserModel(PrimitiveFieldTypeChooserValues.class)
  public Field.Type targetType;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      defaultValue = "false",
      label = "Treat Input Field as Date",
      description = "Select to convert input Long to DateTime before converting to a String",
      displayPosition = 20,
      dependsOn = "targetType",
      triggeredByValue = "STRING"
  )
  public boolean treatInputFieldAsDate;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "en,US",
      label = "Data Locale",
      description = "Affects the interpretation of locale sensitive data, such as using the comma as a decimal " +
                    "separator",
      displayPosition = 30,
      dependsOn = "targetType",
      triggeredByValue = {"BYTE", "INTEGER", "LONG", "DOUBLE", "DECIMAL", "FLOAT", "SHORT"}
  )
  @ValueChooserModel(LocaleChooserValues.class)
  public String dataLocale;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.NUMBER,
      defaultValue = "-1",
      label = "Scale",
      description = "Decimal Value Scale",
      displayPosition = 40,
      dependsOn = "targetType",
      triggeredByValue = {"DECIMAL"}
  )
  public int scale;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.MODEL,
      defaultValue = "ROUND_UNNECESSARY",
      label = "Rounding Strategy",
      description = "Rounding strategy during scale conversion",
      displayPosition = 50,
      dependsOn = "targetType",
      triggeredByValue = {"DECIMAL"}
  )
  @ValueChooserModel(DecimalScaleRoundingStrategyChooserValues.class)
  public DecimalScaleRoundingStrategy decimalScaleRoundingStrategy;

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
      displayPosition = 40,
      dependsOn = "targetType",
      triggeredByValue = {"DATE", "DATETIME", "TIME", "STRING"}
  )
  @ValueChooserModel(DateFormatChooserValues.class)
  public DateFormat dateFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Other Date Format",
      displayPosition = 50,
      dependsOn = "dateFormat",
      triggeredByValue = "OTHER"
  )
  public String otherDateFormat;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "UTF-8",
      label = "CharSet",
      displayPosition = 80,
      dependsOn = "targetType",
      triggeredByValue = "STRING"
  )
  @ValueChooserModel(CharsetChooserValues.class)
  public String encoding = "UTF-8";

  /**
   * Return configured date mask.
   */
  public String getDateMask() {
    return dateFormat != DateFormat.OTHER ? dateFormat.getFormat() : otherDateFormat;
  }

}
