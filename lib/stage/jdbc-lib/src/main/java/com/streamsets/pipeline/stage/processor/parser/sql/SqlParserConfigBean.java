/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.processor.parser.sql;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.lib.jdbc.parser.sql.UnsupportedFieldTypeChooserValues;
import com.streamsets.pipeline.lib.jdbc.parser.sql.UnsupportedFieldTypeValues;

public class SqlParserConfigBean {

  @ConfigDefBean
  public PrivateHikariConfigBean hikariConfigBean = new PrivateHikariConfigBean();

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "SQL Field",
      description = "Field containing the plSql query",
      displayPosition = 10,
      group = "PARSE"
  )
  @FieldSelectorModel(singleValued = true)
  public String sqlField;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Target Field",
      description = "Target field to which fields from the SQL query are to be added as sub-fields",
      displayPosition = 20,
      group = "PARSE"
  )
  public String resultFieldPath;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Resolve Schema from DB",
      description = "Connect to the DB to resolve the schema. Automatically resolves fields to the correct supported " +
          "types",
      displayPosition = 20,
      group = "PARSE"
  )
  public boolean resolveSchema;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "Unsupported Field Type",
      description = "Action to take if an unsupported field type is encountered. When buffering locally," +
          " the action is triggered immediately when the record is read without waiting for the commit",
      displayPosition = 30,
      group = "PARSE",
      defaultValue = "TO_ERROR"
  )
  @ValueChooserModel(UnsupportedFieldTypeChooserValues.class)
  public UnsupportedFieldTypeValues unsupportedFieldOp;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Add unsupported fields to records",
      description = "Add values of unsupported fields as unparsed strings to records",
      displayPosition = 40,
      group = "PARSE",
      defaultValue = "false"
  )
  public boolean sendUnsupportedFields;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.BOOLEAN,
      label = "Case Sensitive Names",
      description = "Use for lower or mixed-case database, table and field names. " +
          "By default, names are changed to all caps. " +
          "Select only when the database or tables were created with quotation marks around the names.",
      displayPosition = 50,
      group = "PARSE",
      defaultValue = "false"
  )
  public boolean caseSensitive;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Date format",
      description = "Format to use to parse date, time, timestamp fields. ",
      displayPosition = 60,
      group = "PARSE",
      defaultValue = "dd-MM-yyyy HH:mm:ss"
  )
  public String dateFormat = "dd-MM-yyyy HH:mm:ss";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Timestamp with local timezone format",
      description = "Format to use to parse date, time, timestamp fields. ",
      displayPosition = 60,
      group = "PARSE",
      defaultValue = "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]"
  )
  public String localDatetimeFormat = "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Zoned DateTime format",
      description = "Format to use to parse timestamp with timezone fields. ",
      displayPosition = 60,
      group = "PARSE",
      defaultValue = "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS] VV"
  )
  public String zonedDatetimeFormat = "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS] VV";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      label = "DB Time Zone",
      description = "Time Zone that the DB is operating in",
      displayPosition = 999,
      group = "PARSE"
  )
  @ValueChooserModel(TimeZoneChooserValues.class)
  public String dbTimeZone;
}
