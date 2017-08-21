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
package com.streamsets.pipeline.stage.destination.hive;

import com.streamsets.pipeline.lib.event.EventCreator;

/**
 * All events that the Hive Metastore target generates.
 */
public final class HiveMetastoreEvents {

  /**
   * Fired once a new table is being created.
   */
  public static EventCreator NEW_TABLE = new EventCreator.Builder("new-table", 2)
    .withRequiredField("table") // Fully qualified Table table name in format `db`.`table`
    .withRequiredField("columns") // LinkedHashMap<String, String> where key is a column name and value Hive type (INT, DECIMAL(10,2))
    .withRequiredField("partitions") // LinkedHashMap<String, String> where key is a column name and value Hive type (INT, DECIMAL(10,2))
    .build();

  /**
   * Fired on schema change for new columns.
   */
  public static EventCreator NEW_COLUMNS = new EventCreator.Builder("new-columns", 1)
    .withRequiredField("table") // Fully qualified Table table name in format `db`.`table`
    .withRequiredField("columns") // LinkedHashMap<String, String> where key is a column name and value Hive type (INT, DECIMAL(10,2))
    .build();

  /**
   * Fired when a new partition is detected
   */
  public static EventCreator NEW_PARTITION = new EventCreator.Builder("new-partition", 1)
    .withRequiredField("table") // Fully qualified Table table name in format `db`.`table`
    .withRequiredField("partition") // LinkedHashMap<String, String> where key is the partition column name and value is partition value
    .build();

  /**
   * Fired when a new Avro schema file is being created
   */
  public static EventCreator AVRO_SCHEMA_STORED = new EventCreator.Builder("avro-schema-stored", 1)
    .withRequiredField("table") // Fully qualified Table table name in format `db`.`table`
    .withRequiredField("avro_schema") // Contains the Avro schema, which was written to HDFS
    .withRequiredField("schema_location") // Location of the Avro schema file which has been written to HDFS
    .build();

  private HiveMetastoreEvents() {
    // instantiation is prohibited
  }
}
