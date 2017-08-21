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
package com.streamsets.pipeline.lib.generator.avro;

import com.google.common.base.Joiner;
import com.streamsets.pipeline.api.StageException;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class AvroSchemaGenerator<E> {

  public static final Joiner joiner = Joiner.on(".");
  public static String schemaName;

  public AvroSchemaGenerator(final String name){
    schemaName = name;
  }

  /**
   * Main function to be called to auto-create an Avro schema
   * There are two classes that extend this abstract class,
   * AvroHiveSchemaGenerator and AvroSchemaGeneratorFromRecord.
   * @param record: incoming record that will form avro schema.
   *  For AvroHiveSchemaGenerator, record is Map<String, HiveTypeInfo>,
   *  and for AvroSchemaGeneratorFromRecord, record is InputStream that is directly converted from Record.
   * @return Schema: auto generated schema
   */
  public abstract String inferSchema(E record) throws StageException;

  // Build a schema with type "record". This will be the top level schema and contains fields
  public static Schema buildSchema(Map<String, Schema> fields, Object... levels) {
    List<Schema.Field> recordFields = new ArrayList<>(fields.size());

    for (Map.Entry<String, Schema> entry : fields.entrySet()) {
      recordFields.add(new Schema.Field(
          entry.getKey(), entry.getValue(),
          null,   // Avro's Schema.Field constructor requires doc.
          entry.getValue().getJsonProp("default"))
      );
    }

    Schema recordSchema;
    if (levels.length == 0) {
      recordSchema = Schema.createRecord(schemaName, null, null, false);
    } else {
      LinkedList<String> lvl = (LinkedList<String>)levels[0];
      recordSchema = Schema.createRecord(joiner.join(lvl), null, null, false);
    }
    recordSchema.setFields(recordFields);
    return recordSchema;
  }

}
