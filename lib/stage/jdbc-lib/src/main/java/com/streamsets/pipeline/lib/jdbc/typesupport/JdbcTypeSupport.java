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
package com.streamsets.pipeline.lib.jdbc.typesupport;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.schemawriter.JdbcSchemaWriter;
import com.streamsets.pipeline.lib.jdbc.JdbcStageCheckedException;

import java.util.HashMap;
import java.util.Map;

/**
 * Interface for supporting JdbcType.
 */
public abstract class JdbcTypeSupport {
  public static final String JDBC_OBJECT_ESCAPE =  "`";
  public static final String COLUMN_TYPE = JDBC_OBJECT_ESCAPE + "%s" + JDBC_OBJECT_ESCAPE + " %s";
  public static final String COMMA = ",";
  public static final String OPEN_BRACKET = "(";
  public static final String CLOSE_BRACKET = ")";

  public static final String TYPE_INFO = "typeInfo";
  public static final String TYPE = "type";
  public static final String EXTRA_INFO = "extraInfo";


  /**
   * Generate Hive Type Info Representation inside the Metadata Record.
   * @param jdbcTypeInfo {@link JdbcTypeInfo}
   * @return {@link Field} which is the field in the metadata record which represents {@link JdbcTypeInfo}
   */
  public Field generateJdbcTypeInfoFieldForMetadataRecord(JdbcTypeInfo jdbcTypeInfo) {
    Map<String, Field> fields = new HashMap<>();
    fields.put(TYPE, Field.create(jdbcTypeInfo.getJdbcType().name()));
    fields.put(EXTRA_INFO, generateExtraInfoFieldForMetadataRecord(jdbcTypeInfo));
    return Field.create(fields);
  }

  /**
   * Generate {@link JdbcTypeInfo} from the Metadata Record <br>.
   * (Reverse of {@link #generateJdbcTypeInfoFieldForMetadataRecord(JdbcTypeInfo)})
   *
   * @throws StageException if the metadata field is not valid.
   * @param jdbcTypeInfoField
   * @return {@link JdbcTypeInfo}
   * @throws StageException If the record has invalid
   */
  @SuppressWarnings("unchecked")
  public JdbcTypeInfo generateJdbcTypeInfoFromMetadataField(Field jdbcTypeInfoField, JdbcSchemaWriter schemaWriter) throws StageException {
    if (jdbcTypeInfoField.getType() == Field.Type.MAP) {
      Map<String, Field> fields = (Map<String, Field>) jdbcTypeInfoField.getValue();
      if (!fields.containsKey(TYPE)
          || !fields.containsKey(EXTRA_INFO)) {
        throw new StageException(JdbcErrors.JDBC_308, TYPE_INFO);
      }
      JdbcType jdbcType = JdbcType.getJdbcTypeFromString(fields.get(TYPE).getValueAsString());
      return generateJdbcTypeInfoFromMetadataField(jdbcType, fields.get(EXTRA_INFO), schemaWriter);
    } else {
      throw new StageException(JdbcErrors.JDBC_308, TYPE_INFO);
    }
  }

  /**
   * Generate Column Definition for create table / add columns
   * @param jdbcTypeInfo {@link JdbcTypeInfo}
   * @param columnName Column Name
   * @return Column definition
   */
  public String generateColumnTypeDefinition(JdbcTypeInfo jdbcTypeInfo, String columnName) {
    return String.format(
      COLUMN_TYPE,
      columnName,
      jdbcTypeInfo.toString()
    );
  }

  /**
   * Generate {@link JdbcTypeInfo} from Metadata record field.
   * @param type {@link JdbcType}
   * @param JdbcTypeField hive type field
   * @param <T> {@link JdbcTypeInfo}
   * @return {@link JdbcTypeInfo}
   */
  protected abstract <T extends JdbcTypeInfo> T generateJdbcTypeInfoFromMetadataField(
      JdbcType type,
      Field JdbcTypeField,
      JdbcSchemaWriter schemaWriter
  ) throws StageException;

  /**
   * Generate Metdata Record Extra Info Field from {@link JdbcTypeInfo}.
   * @param JdbcTypeInfo {@link JdbcTypeInfo}
   * @param <T> {@link JdbcTypeInfo}
   * @return {@link Field} of Extra Info for the hive type inside the metadata record
   */
  protected abstract <T extends JdbcTypeInfo> Field generateExtraInfoFieldForMetadataRecord(T JdbcTypeInfo);

  /**
   * Returns the {@link JdbcTypeInfo} from the information in the result set of desc query.
   * @param JdbcTypeString Column definition from the SQL Result Set.
   * @param <T> {@link JdbcTypeInfo}
   * @return {@link JdbcTypeInfo}
   * @throws StageException
   */
  public abstract <T extends JdbcTypeInfo> T generateJdbcTypeInfoFromResultSet(String JdbcTypeString, JdbcSchemaWriter schemaWriter)
      throws JdbcStageCheckedException;

  /**
   * Get the {@link JdbcTypeInfo} from the record field
   * @param field Field inside a record for which JdbcTypeInfo need to be extracted.
   * @param <T> {@link JdbcTypeInfo}
   * @return {@link JdbcTypeInfo}
   * @throws StageException
   */
  public abstract <T extends JdbcTypeInfo> T generateJdbcTypeInfoFromRecordField(Field field, JdbcSchemaWriter schemaWriter, Object... auxillaryArgs)
      throws JdbcStageCheckedException;

  /**
   * Create a new {@link JdbcTypeInfo}
   * @param <T> {@link JdbcTypeInfo}
   * @return {@link JdbcTypeInfo}
   */
  public abstract <T extends JdbcTypeInfo> T createTypeInfo(JdbcType JdbcType, JdbcSchemaWriter schemaWriter, Object... auxillaryArgs);
}
