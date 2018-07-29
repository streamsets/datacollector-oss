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
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  BIGTABLE_01("Error connecting to Cloud Bigtable for project: '{}'  '{}'"),
  BIGTABLE_02("Error creating Cloud Bigtable admin API object: {}"),
  BIGTABLE_03("Time stamp field cannot be blank"),
  BIGTABLE_04("Column family '{}' does not exist and Create Table and Column Families is not enabled"),
  BIGTABLE_05("Cannot access Column Family information: '{}'"),
  BIGTABLE_06("Default ColumnFamily must be specified for qualifier '{}'"),
  BIGTABLE_07("Cannot access credential file '{}' referenced by environment variable '{}'"),
  BIGTABLE_08("Incorrect datatype '{}' for time stamp field '{}'.  Time stamps must be long."),
  BIGTABLE_09("No field names or invalid field name specified for row key '{}'"),
  BIGTABLE_10("Row key field '{}' was not found in record"),
  BIGTABLE_11("Single column row key selected - field path '{}' not found in record."),
  BIGTABLE_12("Cannot convert type: '{}' to '{}'"),
  BIGTABLE_13("Conversion not defined for '{}'"),
  BIGTABLE_14("Time stamp field does not exist: '{}'"),
  BIGTABLE_15("Time stamp conversion failure {}: "),
  BIGTABLE_16("Invalid qualifier (or 'column_family:qualifier') name '{}'"),
  BIGTABLE_17("Failure communicating with Bigtable: '{}'"),
  BIGTABLE_18("Create table '{}' failed: '{}'"),
  BIGTABLE_19("Getting connection to table '{}' failed: '{}'"),
  BIGTABLE_20("Failure inserting into table: '{}'"),
  BIGTABLE_21("Invalid conversion for field '{}' from '{}' to '{}'"),
  BIGTABLE_22("Table '{}' does not exist."),
  BIGTABLE_23("There are no fields in this record to insert into Bigtable "),
  BIGTABLE_24("No field paths are specified to insert into Bigtable"),
  ;

  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}
