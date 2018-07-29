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
package com.streamsets.pipeline.stage.destination.bigtable;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.FieldSelectorModel;
import com.streamsets.pipeline.api.ListBeanModel;
import com.streamsets.pipeline.api.ValueChooserModel;

import java.util.List;

public class BigtableConfigBean {

 @ConfigDef(required = true,
     type = ConfigDef.Type.STRING,
     defaultValue = "",
     label = "Instance ID",
     description = "Google Bigtable Instance ID",
     displayPosition = 10,
     group = "BIGTABLE"
 )
 public String bigtableInstanceID;

 @ConfigDef(required = true,
     type = ConfigDef.Type.STRING,
     defaultValue = "",
     label = "Project ID",
     description = "Google Bigtable Project ID",
     displayPosition = 20,
     group = "BIGTABLE"
 )
 public String bigtableProjectID;

 @ConfigDef(
     required = true,
     type = ConfigDef.Type.STRING,
     defaultValue = "",
     label = "Table Name",
     description = "Google Bigtable Destination Table Name",
     displayPosition = 30,
     group = "BIGTABLE"
 )
 public String tableName;

 @ConfigDef(
     required = false,
     type = ConfigDef.Type.STRING,
     defaultValue = "",
     label = "Default Column Family Name",
     description = "Default Column Family Name is used for all qualifiers when Explicit Field " +
         "Mapping is unchecked.  Also used when Explicit Field Mapping is checked, " +
         "but a column only specifies a qualifier and does not explicit specify a column family",
     displayPosition = 40,
     group = "BIGTABLE"
 )
 public String columnFamily;


 @ConfigDef(required = false,
     type = ConfigDef.Type.BOOLEAN,
     defaultValue = "false",
     label = "Row Key is Composite",
     description = "If checked, specified fields are concatenated to create a row key.  Otherwise,  "
     + "a single field is used as the row key.",
     displayPosition = 45,
     group = "BIGTABLE"
 )
 public boolean createCompositeRowKey;

 @ConfigDef(
     required = true,
     type = ConfigDef.Type.MODEL,
     defaultValue = "",
     label = "Row Key",
     description = "Specify field which is a single column row key",
     displayPosition = 50,
     dependsOn = "createCompositeRowKey",
     triggeredByValue = "false",
     group = "BIGTABLE"
 )
 @FieldSelectorModel(singleValued = true)
 public String singleColumnRowKey;

 @ConfigDef(
     required = true,
     type = ConfigDef.Type.MODEL,
     defaultValue = "",
     label = "Row Key Fields",
     description = "Fields and field widths which comprise the row key.",
     displayPosition = 50,
     dependsOn = "createCompositeRowKey",
     triggeredByValue = "true",
     group = "BIGTABLE"
 )
 @ListBeanModel
 public List<BigtableRowKeyMapping> rowKeyColumnMapping;

 @ConfigDef(required = false,
     type = ConfigDef.Type.BOOLEAN,
     defaultValue = "false",
     label = "Create Table and Column Families",
     description = "If checked, the table and column families will be created if they do not exist ",
     displayPosition = 60,
     group = "BIGTABLE"
 )
 public boolean createTableAndColumnFamilies;

 @ConfigDef(required = false,
     type = ConfigDef.Type.BOOLEAN,
     defaultValue = "false",
     label = "Ignore Missing Data Values",
     description = "If checked, records which have missing fields will be processed, if "
     + "unchecked the record will be sent to the On Record Error destination.  Regardless of this setting "
     + "any record which is missing a row key field will not be processed and will "
     + "be sent to the error destination.",
     displayPosition = 70,
     group = "BIGTABLE"
 )
 public boolean ignoreMissingFields;

 @ConfigDef(required = false,
     type = ConfigDef.Type.BOOLEAN,
     defaultValue = "true",
     label = "Explicit Column Family Mapping",
     description = "If checked, qualifiers prefaced by a column family (Eg. 'cf:qual') " +
         "will be inserted with the specified column family.  If the column family does " +
         "not exist, and 'Create Table and Column Families' is checked, the column family " +
         "will be created. If Create Table and Column Families is not checked, and the " +
         "column family does not exist, the stage's On Record Error processing will apply. " +
         " If this option is not checked, any specified column family will be ignored " +
         "and Default Column Family will be used for all qualifiers.",
     displayPosition = 70,
     group = "BIGTABLE"
 )
 public boolean explicitFieldMapping;

 @ConfigDef(required = false,
     type = ConfigDef.Type.MODEL,
     defaultValue = "",
     label = "Fields",
     description = "Field Paths in the incoming record, Bigtable column names and Bigtable storage types",
     displayPosition = 80,
     group = "BIGTABLE"
 )
 @ListBeanModel
 public List<BigtableFieldMapping> fieldColumnMapping;

 @ConfigDef(
     required = false,
     type = ConfigDef.Type.MODEL,
     defaultValue = "SYSTEM_TIME",
     label = "Time Basis",
     description = "Timestamp value to use as time basis.",
     displayPosition = 90,
     group = "BIGTABLE"
 )
 @ValueChooserModel(TimeBasisChooserValues.class)
 public TimeBasis timeBasis = TimeBasis.SYSTEM_TIME;

 @ConfigDef(required = false,
     type = ConfigDef.Type.MODEL,
     label = "Time Stamp Field Name",
     description = "Field name that contains the record's time stamp value",
     displayPosition = 95,
     dependsOn = "timeBasis",
     triggeredByValue = "FROM_RECORD",
     group = "BIGTABLE"
 )
 @FieldSelectorModel(singleValued = true)
 public String timeStampField;

 @ConfigDef(required = false,
     type = ConfigDef.Type.NUMBER,
     defaultValue = "1000",
     label = "Number of Records to Buffer",
     description = "Number of records to buffer per commit to Bigtable.  1 <= number <= 1000",
     min = 1,
     displayPosition = 100,
     group = "BIGTABLE"
 )
 public int numToBuffer;

}
